use anyhow::bail;
use std::collections::BTreeMap;

use flexss::{
    block_picker::BlockPicker, naive_shuffle::NaiveShuffle, BackendId, Health, Picker, RoundRobin,
    TenantId,
};

fn main() {
    health_aware::<RoundRobin>().unwrap();
    health_aware::<NaiveShuffle>().unwrap();
    health_aware::<BlockPicker>().unwrap();

    // RoundRobin is succeptible to poison pill tenants
    assert!(poison_pill::<RoundRobin>().is_err());
    // These pickers all prevent poison pills at steady state
    poison_pill::<NaiveShuffle>().unwrap();
    poison_pill::<BlockPicker>().unwrap();

}

struct Simulation {
    backends: BTreeMap<BackendId, Health>,
}
impl Default for Simulation {
    fn default() -> Self {
        Self {
            backends: BTreeMap::new(),
        }
    }
}

fn health_aware<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(5);
    // One backend is known to be unhealthy
    for i in 0..30 {
        let h = if i == 0 { Health::Down } else { Health::Up } ;
        s.backends.insert(BackendId(i), h);
        p.register(BackendId(i), h);
    }

    for tenant_id in 0 .. 100 {
        let tenant_id = TenantId(tenant_id);
        for _ in 0 .. 100 {
            let b = p.pick(tenant_id).unwrap();
            if s.backends.get(&b).unwrap() == &Health::Down {
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
        let b = p.pick(TenantId(0)).unwrap();
        *s.backends.get_mut(&b).unwrap() = Health::Down;
    }

    if s.backends.values().filter(|&&h| h == Health::Down).count() == s.backends.len() {
        bail!("a single tenant poisoned all backends");
    }
    Ok(())
}


fn rolling_restart<P: Picker>() -> anyhow::Result<()> {
    let mut s = Simulation::default();
    let mut p = P::new(5);
    let num_backends = 30;
    for i in 0..num_backends {
        s.backends.insert(BackendId(i), Health::Up);
        p.register(BackendId(i), Health::Up);
    }

    // Restart 10% of the fleet at a time.
    assert!(num_backends % 10 == 0); // just for simplicity, ensure we can process 10% at a time
    let stage_size = num_backends / 10;
    for stage in 0 .. 10 {
        for i in 0 .. stage_size {
            *s.backends.get_mut(&BackendId(stage * stage_size + i)).unwrap() = Health::Draining;
        }
    }

    if s.backends.values().filter(|&&h| h == Health::Down).count() == s.backends.len() {
        bail!("a single tenant poisoned all backends");
    }
    Ok(())
}
