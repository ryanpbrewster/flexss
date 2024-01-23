use rand::{rngs::SmallRng, seq::SliceRandom, Rng, SeedableRng};

use crate::{hash, Backend, BackendId, Health, Picker, TenantId};
pub struct DrainAwareShuffle {
    backends: Vec<Backend>,
    shard_size: usize,
    prng: SmallRng,
}
impl Picker for DrainAwareShuffle {
    fn new(shard_size: usize) -> Self {
        Self {
            shard_size,
            backends: Vec::new(),
            prng: SmallRng::seed_from_u64(42),
        }
    }
    fn register(&mut self, id: BackendId, health: Health) {
        if let Some(existing) = self.backends.iter_mut().find(|b| b.id == id) {
            existing.health = health;
        } else {
            self.backends.push(Backend {
                id,
                health,
                hash: hash(id),
            });
            self.backends.sort();
        }
    }

    fn unregister(&mut self, id: BackendId) {
        self.backends.retain(|b| b.id != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        if self.backends.is_empty() {
            return None;
        }
        let mut all_backends: Vec<Backend> = self
            .backends
            .iter()
            .filter(|b| b.health != Health::Draining)
            .cloned()
            .collect();
        let (shuffled, _remainder) = {
            let mut prng = SmallRng::seed_from_u64(id.0);
            all_backends.partial_shuffle(&mut prng, self.shard_size)
        };

        // Note: different RNG! This one is not determinstic based on the tenant id.
        let idx = self.prng.gen_range(0..self.shard_size);
        for i in 0..self.shard_size {
            let b = shuffled[(idx + i) % shuffled.len()];
            if b.health == Health::Up {
                return Some(b.id);
            }
        }
        None
    }
}
