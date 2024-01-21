#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TenantId(pub u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct BackendId(pub u64);

pub trait Picker {
    fn new(shard_size: usize) -> Self;
    fn add_backend(&mut self, id: BackendId);
    fn remove_backend(&mut self, id: BackendId);
    fn pick(&mut self, id: TenantId) -> Option<BackendId>;
}

pub mod naive_shuffle;

#[cfg(test)]
mod test {
    use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
    use std::collections::{BTreeMap, BTreeSet};

    use crate::{BackendId, Picker, TenantId};

    #[test]
    fn hello() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn oracle_smoke() {
        let mut oracle = Oracle::new(3);
        for i in 0..10 {
            oracle.add_backend(BackendId(i));
        }
        assert_eq!(oracle.pick(TenantId(42)).unwrap(), BackendId(4));
    }

    #[test]
    fn oracle_load_balancing() {
        let mut oracle = Oracle::new(3);
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
    }

    #[test]
    fn oracle_tenant_isolation() {
        let mut oracle = Oracle::new(3);
        for i in 0..10 {
            oracle.add_backend(BackendId(i));
        }

        let mut mk_tally = |tenant: TenantId| -> BTreeMap<BackendId, usize> {
            let mut tally = BTreeMap::new();
            for _ in 0..1_000 {
                let choice = oracle.pick(tenant).unwrap();
                *tally.entry(choice).or_default() += 1;
            }
            tally
        };

        // tenants 1 and 2 only overlap on a single backend: 7
        assert_eq!(
            mk_tally(TenantId(1)).keys().copied().collect::<Vec<_>>(),
            vec![BackendId(0), BackendId(1), BackendId(7)]
        );
        assert_eq!(
            mk_tally(TenantId(2)).keys().copied().collect::<Vec<_>>(),
            vec![BackendId(2), BackendId(7), BackendId(9)]
        );
    }
}
