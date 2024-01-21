use std::collections::BTreeSet;

use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};

use crate::{BackendId, Picker, TenantId};

pub struct NaiveShuffle {
    backends: BTreeSet<BackendId>,
    shard_size: usize,
    prng: SmallRng,
}
impl Picker for NaiveShuffle {
    fn new(shard_size: usize) -> Self {
        Self {
            shard_size,
            backends: BTreeSet::default(),
            prng: SmallRng::seed_from_u64(42),
        }
    }
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
