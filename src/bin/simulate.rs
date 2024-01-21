use anyhow::bail;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

use flexss::{
    block_picker::BlockPicker, drain_aware_shuffle::DrainAwareShuffle, naive_shuffle::NaiveShuffle,
    rendevouz::Rendevouz, BackendId, Health, Picker, RoundRobin, TenantId, rendevouz_shuffle::RendevouzShuffle,
};

fn main() {
    health_aware::<RoundRobin>().unwrap();
    health_aware::<NaiveShuffle>().unwrap();
    health_aware::<BlockPicker>().unwrap();
    health_aware::<Rendevouz>().unwrap();
    health_aware::<RendevouzShuffle>().unwrap();

    // RoundRobin is succeptible to poison pill tenants
    assert!(poison_pill::<RoundRobin>().is_err());
    // These pickers all prevent poison pills at steady state
    poison_pill::<NaiveShuffle>().unwrap();
    poison_pill::<DrainAwareShuffle>().unwrap();
    poison_pill::<BlockPicker>().unwrap();
    // Rendevouz hashing lets one backend murder everything
    assert!(poison_pill::<Rendevouz>().is_err());
    poison_pill::<RendevouzShuffle>().unwrap();

    unaligned_rolling_restart::<RoundRobin>().unwrap();
    // NaiveShuffle cannot distinguish between intentional
    // deploys and poison-pill scenarios, so it hits dead shards.
    assert!(unaligned_rolling_restart::<NaiveShuffle>().is_err());
    // Making the picker aware of drains allows it to work with
    // intentional deploys.
    unaligned_rolling_restart::<DrainAwareShuffle>().unwrap();
    // Without some way to guarantee that the blocks
    // are aligned with the deploys, the BlockPicker
    // will hit dead shards.
    assert!(unaligned_rolling_restart::<BlockPicker>().is_err());
    unaligned_rolling_restart::<Rendevouz>().unwrap();
    unaligned_rolling_restart::<RendevouzShuffle>().unwrap();

    // RoundRobin always hits a ton of backends
    assert!(rolling_restart_blast_radius::<RoundRobin>().is_err());
    // NaiveShuffle is good at dealing with ephemeral downtime
    rolling_restart_blast_radius::<NaiveShuffle>().unwrap();
    rolling_restart_blast_radius::<BlockPicker>().unwrap();
    // The drain-aware shuffle picker can deal with lots of unhealthy backends,
    // but the cost is that it sprawls.
    assert!(rolling_restart_blast_radius::<DrainAwareShuffle>().is_err());
    rolling_restart_blast_radius::<Rendevouz>().unwrap();
    rolling_restart_blast_radius::<RendevouzShuffle>().unwrap();

    // Every one of these struggles with a quick recycling
    assert!(recycle_blast_radius::<RoundRobin>().is_err());
    assert!(recycle_blast_radius::<NaiveShuffle>().is_err());
    assert!(recycle_blast_radius::<DrainAwareShuffle>().is_err());
    assert!(recycle_blast_radius::<BlockPicker>().is_err());
    // But rendevouz hashing (and other consistent hashing approaches)
    // have a very limited blast radius even when the underlying fleet
    // changes.
    recycle_blast_radius::<Rendevouz>().unwrap();
    recycle_blast_radius::<RendevouzShuffle>().unwrap();

    load_distribution::<RoundRobin>().unwrap();
    load_distribution::<NaiveShuffle>().unwrap();
    load_distribution::<BlockPicker>().unwrap();
    assert!(load_distribution::<Rendevouz>().is_err());
    load_distribution::<RendevouzShuffle>().unwrap();
}

#[derive(Default)]
struct Simulation {
    backends: BTreeMap<BackendId, Health>,
}

fn health_aware<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(5);
    // One backend is known to be unhealthy
    for i in 0..30 {
        let h = if i == 0 { Health::Down } else { Health::Up };
        s.backends.insert(BackendId(i), h);
        p.register(BackendId(i), h);
    }

    for tenant_id in 0..100 {
        let tenant_id = TenantId(tenant_id);
        for _ in 0..100 {
            let b = p.pick(tenant_id).unwrap();
            if s.backends.get(&b).unwrap() != &Health::Up {
                bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
            }
        }
    }
    Ok(())
}

fn poison_pill<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(5);
    for i in 0..30 {
        s.backends.insert(BackendId(i), Health::Up);
        p.register(BackendId(i), Health::Up);
    }

    // Tenant 0 poisons backends
    for _ in 0..1_000 {
        let Some(b) = p.pick(TenantId(0)) else { break };
        *s.backends.get_mut(&b).unwrap() = Health::Down;
        p.register(b, Health::Down);
    }

    if s.backends.values().filter(|&&h| h == Health::Down).count() == s.backends.len() {
        bail!("a single tenant poisoned all backends");
    }
    Ok(())
}

fn load_distribution<P: Picker>() -> anyhow::Result<()> {
    // We are going to have 50 backends and 100 tenants, and we want to
    // ensure that every backend receives some reasonable fraction of load.
    let mut s = Simulation::default();
    let mut p = P::new(5);
    let backends: Vec<BackendId> = (0..50).map(BackendId).collect();
    for &b in &backends {
        s.backends.insert(b, Health::Up);
        p.register(b, Health::Up);
    }

    let mut tally: BTreeMap<BackendId, usize> = BTreeMap::new();
    let tenants: Vec<TenantId> = (0..100).map(TenantId).collect();
    let num_requests = 100;
    for &tenant_id in &tenants {
        for _ in 0..num_requests {
            let b = p.pick(tenant_id).unwrap();
            *tally.entry(b).or_default() += 1;
        }
    }

    // Ensure that all backends receive at least 10% of their fair share
    let fair = tenants.len() * num_requests / backends.len();
    for &b in &backends {
        let recv = tally.get(&b).copied().unwrap_or_default();
        if recv < fair / 5 {
            bail!("{b:?} received {recv} which is less than 20% of {fair}");
        }
    }
    Ok(())
}

fn unaligned_rolling_restart<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(5);
    let mut backends: Vec<BackendId> = (0..30).map(BackendId).collect();
    for &b in &backends {
        s.backends.insert(b, Health::Up);
        p.register(b, Health::Up);
    }

    // Restart 33% of the fleet at a time.
    assert!(backends.len() % 3 == 0); // just for simplicity, ensure we can process evenly in thirds
    let stage_size = backends.len() / 3;
    let stages = {
        let mut prng = SmallRng::seed_from_u64(42);
        backends.shuffle(&mut prng);
        backends.windows(stage_size)
    };
    for stage in stages {
        // mark all of this stage's backends as draining
        for &b in stage {
            *s.backends.get_mut(&b).unwrap() = Health::Draining;
            p.register(b, Health::Draining);
        }

        for tenant_id in 0..2_000 {
            let tenant_id = TenantId(tenant_id);
            for _ in 0..100 {
                let Some(b) = p.pick(tenant_id) else {
                    bail!("could not route request for {tenant_id:?}")
                };
                if s.backends.get(&b).unwrap() != &Health::Up {
                    bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
                }
            }
        }

        // mark all of this stage's backends as healthy again
        for &b in &backends {
            *s.backends.get_mut(&b).unwrap() = Health::Up;
            p.register(b, Health::Up);
        }
    }
    Ok(())
}

fn rolling_restart_blast_radius<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(6);
    let backends: Vec<BackendId> = (0..30).map(BackendId).collect();
    for &b in &backends {
        s.backends.insert(b, Health::Up);
        p.register(b, Health::Up);
    }

    // Suppose we deploy to the entire fleet?
    // How many distinct backends will a single tenant hit over the course
    // of that deploy?
    let tenant_id = TenantId(0);
    let mut touched = BTreeSet::new();
    for stage in backends.windows(5) {
        // drain backends
        for &b in stage {
            *s.backends.get_mut(&b).unwrap() = Health::Draining;
            p.register(b, Health::Draining);

            for _ in 0..10 {
                let Some(choice) = p.pick(tenant_id) else {
                    bail!("could not route request for {tenant_id:?}")
                };
                if s.backends.get(&choice).unwrap() != &Health::Up {
                    bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
                }
                touched.insert(choice);
            }
        }
        // undrain backends
        for &b in stage {
            *s.backends.get_mut(&b).unwrap() = Health::Up;
            p.register(b, Health::Up);

            for _ in 0..10 {
                let Some(choice) = p.pick(tenant_id) else {
                    bail!("could not route request for {tenant_id:?}")
                };
                if s.backends.get(&choice).unwrap() != &Health::Up {
                    bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
                }
                touched.insert(choice);
            }
        }
    }

    if touched.len() > backends.len() / 2 {
        bail!("over the course of the deploy, tenant 0 sprawled out to more than half of all backends");
    }
    Ok(())
}

fn recycle_blast_radius<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(6);
    let fleet_size: usize = 30;
    let before_backends: Vec<BackendId> = (0..fleet_size).map(|i| BackendId(i as u64)).collect();
    let after_backends: Vec<BackendId> = (0..fleet_size)
        .map(|i| BackendId(fleet_size as u64 + i as u64))
        .collect();
    for &b in &before_backends {
        s.backends.insert(b, Health::Up);
        p.register(b, Health::Up);
    }

    // Suppose we _recycle_ the entire fleet?
    // How many distinct backends will a single tenant hit over the course
    // of that cycling?
    let tenant_id = TenantId(0);
    let mut touched = BTreeSet::new();
    for i in 0..before_backends.len() {
        // spin up new backend
        s.backends.insert(after_backends[i], Health::Up);
        p.register(after_backends[i], Health::Up);

        for _ in 0..10 {
            let Some(choice) = p.pick(tenant_id) else {
                bail!("could not route request for {tenant_id:?}")
            };
            if s.backends.get(&choice).unwrap() != &Health::Up {
                bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
            }
            touched.insert(choice);
        }

        // spin down old backend
        s.backends.remove(&before_backends[i]).unwrap();
        p.unregister(before_backends[i]);

        for _ in 0..10 {
            let Some(choice) = p.pick(tenant_id) else {
                bail!("could not route request for {tenant_id:?}")
            };
            if s.backends.get(&choice).unwrap() != &Health::Up {
                bail!("tenant {tenant_id:?} got routed to an unhealthy backend");
            }
            touched.insert(choice);
        }
    }

    if touched.len() > fleet_size {
        bail!("over the course of the deploy, tenant 0 sprawled out to more than {fleet_size} backends ({})", touched.len());
    }
    Ok(())
}
