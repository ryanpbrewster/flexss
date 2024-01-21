use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{Backend, BackendId, Health, Picker, TenantId};

pub struct BlockPicker {
    backends: Vec<Backend>,
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
    fn register(&mut self, id: BackendId, health: Health) {
        if let Some(existing) = self.backends.iter_mut().find(|b| b.id == id) {
            existing.health = health;
        } else {
            self.backends.push(Backend { id, health });
            self.backends.sort();
        }
    }

    fn unregister(&mut self, id: BackendId) {
        self.backends.retain(|b| b.id != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        let bucket_size = self.backends.len() / self.shard_size;
        if bucket_size == 0 {
            return None;
        }
        let bucket = self.prng.gen_range(0..self.shard_size);
        for i in 0..self.shard_size {
            let bucket = (bucket + i) % self.shard_size;
            // Note: different RNG! This one is determinstic based on the tenant id and bucket.
            let mut prng = SmallRng::seed_from_u64(id.0 ^ bucket as u64);
            let slot = prng.gen_range(0..bucket_size);
            let b = self.backends[bucket * bucket_size + slot];
            if b.health == Health::Up {
                return Some(b.id);
            }
        }
        None
    }
}
