use std::collections::BTreeMap;

use flexss::{self, naive_shuffle::NaiveShuffle, BackendId, Picker, TenantId, block_picker::BlockPicker};
fn main() {
    println!(
        "[NaiveShuffle] Load Balancing: {}",
        quantify_load_balancing::<NaiveShuffle>()
    );
    println!(
        "[NaiveShuffle] Tenant Isolation: {}",
        quantify_tenant_isolation::<NaiveShuffle>()
    );

    println!(
        "[BlockPicker] Load Balancing: {}",
        quantify_load_balancing::<BlockPicker>()
    );
    println!(
        "[BlockPicker] Tenant Isolation: {}",
        quantify_tenant_isolation::<BlockPicker>()
    );
}

fn quantify_load_balancing<P: Picker>() -> f64 {
    let mut oracle = P::new(3);
    for i in 0..10 {
        oracle.add_backend(BackendId(i));
    }

    let mut tally: BTreeMap<BackendId, usize> = BTreeMap::new();
    for _ in 0..1_000 {
        let choice = oracle.pick(TenantId(42)).unwrap();
        *tally.entry(choice).or_default() += 1;
    }
    // The shard size is 3. The picker should exercise the whole thing, but never go outside.
    assert_eq!(tally.len(), 3);
    1.0 - (tally.values().copied().max().unwrap() - tally.values().copied().min().unwrap()) as f64
        / 1_000.0
}

fn quantify_tenant_isolation<P: Picker>() -> usize {
    let shard_size = 3;
    let mut oracle = P::new(shard_size);
    for i in 0..100 {
        oracle.add_backend(BackendId(i));
    }

    let mut tally: BTreeMap<TenantId, BTreeMap<BackendId, usize>> = BTreeMap::new();
    for tenant_id in 1..100 {
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
    let mut penalty = 0;
    for (tenant_1, backends_1) in &tally {
        for (tenant_2, backends_2) in &tally {
            if tenant_1 == tenant_2 {
                continue;
            }
            let overlap = backends_1
                .keys()
                .filter(|b| backends_2.contains_key(b))
                .count();
            penalty += match overlap {
                0 => 0,
                1 => 1,
                2 => 10,
                _ => 1_000,
            };
        }
    }
    penalty
}
