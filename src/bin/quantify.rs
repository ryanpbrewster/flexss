use std::collections::BTreeMap;

use flexss::{
    self, block_picker::BlockPicker, naive_shuffle::NaiveShuffle, BackendId, Picker, TenantId,
};
use rand::{rngs::SmallRng, Rng, SeedableRng};
fn main() {
    println!(
        "[NaiveShuffle] Load Balancing: {}",
        quantify_load_balancing::<NaiveShuffle>()
    );
    println!(
        "[NaiveShuffle] Tenant Isolation: {:?}",
        quantify_tenant_isolation::<NaiveShuffle>()
    );

    println!(
        "[BlockPicker] Load Balancing: {}",
        quantify_load_balancing::<BlockPicker>()
    );
    println!(
        "[BlockPicker] Tenant Isolation: {:?}",
        quantify_tenant_isolation::<BlockPicker>()
    );
}

fn quantify_load_balancing<P: Picker>() -> f64 {
    let num_backends = 10;
    let mut p = P::new(3);
    for i in 0..num_backends {
        p.register(BackendId(i as u64));
    }

    let mut tally: Vec<usize> = vec![0; num_backends];
    let mut prng = SmallRng::seed_from_u64(42);
    for _ in 0..100_000 {
        let choice = p.pick(TenantId(prng.gen_range(0..1_000))).unwrap();
        tally[choice.0 as usize] += 1;
    }

    let mean = tally.iter().copied().sum::<usize>() as f64 / num_backends as f64;
    let variance = tally
        .iter()
        .copied()
        .map(|x| {
            let dx = x as f64 - mean;
            dx * dx
        })
        .sum::<f64>()
        / num_backends as f64;
    variance.sqrt()
}

fn quantify_tenant_isolation<P: Picker>() -> Vec<f64> {
    let shard_size = 3;
    let mut oracle = P::new(shard_size);
    for i in 0..100 {
        oracle.register(BackendId(i));
    }

    let num_tenants = 100;
    let mut tally: BTreeMap<TenantId, BTreeMap<BackendId, usize>> = BTreeMap::new();
    for tenant_id in 0..num_tenants {
        let tenant_id = TenantId(tenant_id);
        for _ in 0..1_000 {
            let backend_id = oracle.pick(tenant_id).unwrap();
            *tally
                .entry(tenant_id)
                .or_default()
                .entry(backend_id)
                .or_default() += 1;
        }
    }
    let mut overlaps = vec![0; shard_size + 1];
    for (tenant_1, backends_1) in &tally {
        for (tenant_2, backends_2) in &tally {
            if tenant_1 == tenant_2 {
                continue;
            }
            let count = backends_1
                .keys()
                .filter(|b| backends_2.contains_key(b))
                .count();
            overlaps[count] += 1;
        }
    }
    let normalization = (num_tenants * num_tenants) as f64;
    overlaps
        .into_iter()
        .map(|c| c as f64 / normalization)
        .collect()
}
