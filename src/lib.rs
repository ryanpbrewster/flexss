#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
struct TenantId(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
struct BackendId(u64);

trait Picker {
    fn add_backend(&mut self, id: BackendId);
    fn remove_backend(&mut self, id: BackendId);
    fn pick(&mut self, id: TenantId) -> Option<BackendId>;
}

#[cfg(test)]
mod test {
    use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
    use std::collections::{BTreeMap, BTreeSet};

    use crate::{BackendId, Picker, TenantId};

    #[test]
    fn hello() {
        assert_eq!(2 + 2, 4);
    }

    struct Oracle {
        backends: BTreeSet<BackendId>,
        shard_size: usize,
        prng: SmallRng,
    }
    impl Oracle {
        fn new(shard_size: usize) -> Self {
            Self {
                shard_size,
                backends: BTreeSet::default(),
                prng: SmallRng::seed_from_u64(42),
            }
        }
    }
    impl Picker for Oracle {
        fn add_backend(&mut self, id: BackendId) {
            self.backends.insert(id);
        }

        fn remove_backend(&mut self, id: BackendId) {
            self.backends.remove(&id);
        }

        fn pick(&mut self, id: TenantId) -> Option<BackendId> {
            let mut all_backends: Vec<BackendId> = self.backends.iter().copied().collect();
            let (shuffled, _remainder) = {
                let mut prng = SmallRng::seed_from_u64(id.0);
                all_backends.partial_shuffle(&mut prng, self.shard_size)
            };

            // Note: different RNG! This one is not determinstic based on the tenant id.
            shuffled.choose(&mut self.prng).copied()
        }
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
    fn oracle_statistics() {
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
}
