use std::collections::BTreeMap;

use flexss::{self, naive_shuffle::NaiveShuffle, BackendId, Picker, TenantId};
fn main() {
    println!(
        "[NaiveShuffle] Load Balancing: {}",
        quantify_load_balancing::<NaiveShuffle>()
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
    assert_eq!(
        tally.values().copied().collect::<Vec<_>>(),
        vec![325, 343, 332]
    );
    assert_eq!(tally.len(), 3);
    1.0 - (tally.values().copied().max().unwrap() - tally.values().copied().min().unwrap()) as f64
        / 1_000.0
}
