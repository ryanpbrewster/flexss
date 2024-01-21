use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{BackendId, Picker, TenantId};

pub struct BlockPicker {
    backends: Vec<BackendId>,
    shard_size: usize,
    prng: SmallRng,
}
impl Picker for BlockPicker {
    fn new(shard_size: usize) -> Self {
        Self {
            shard_size,
            backends: Vec::default(),
            prng: SmallRng::seed_from_u64(42),
        }
    }
    fn add_backend(&mut self, id: BackendId) {
        self.backends.push(id);
        self.backends.sort();
    }

    fn remove_backend(&mut self, id: BackendId) {
        self.backends.retain(|&v| v != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        let bucket_size = self.backends.len() / self.shard_size;
        if bucket_size == 0 {
            return None;
        }
        let bucket = self.prng.gen_range(0..self.shard_size);

        // Note: different RNG! This one is determinstic based on the tenant id and bucket.
        let mut prng = SmallRng::seed_from_u64(id.0 ^ bucket as u64);
        let slot = prng.gen_range(0..bucket_size);
        Some(self.backends[bucket * bucket_size + slot])
    }
}
