use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{combine, hash, Backend, BackendId, Health, Picker, TenantId};

pub struct RendevouzShuffle {
    backends: Vec<Backend>,
    shard_size: usize,
    prng: SmallRng,
}
impl Picker for RendevouzShuffle {
    fn new(shard_size: usize) -> Self {
        Self {
            backends: Vec::new(),
            shard_size,
            prng: SmallRng::seed_from_u64(42),
        }
    }
    fn register(&mut self, id: BackendId, health: Health) {
        if health == Health::Draining {
            self.unregister(id);
            return;
        }
        if let Some(existing) = self.backends.iter_mut().find(|b| b.id == id) {
            existing.health = health;
        } else {
            self.backends.push(Backend {
                id,
                health,
                hash: hash(id),
            });
        }
    }

    fn unregister(&mut self, id: BackendId) {
        self.backends.retain(|b| b.id != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        assert!(self.backends.len() >= self.shard_size);

        let th = hash(id);
        self.backends.select_nth_unstable_by_key(self.shard_size, |b| combine(th, b.hash));
        let healthy = self
            .backends[..self.shard_size]
            .iter()
            .filter(|b| b.health == Health::Up)
            .count();
        if healthy == 0 {
            None
        } else {
            Some(
                self.backends
                    .iter()
                    .filter(|b| b.health == Health::Up)
                    .nth(self.prng.gen_range(0..healthy))
                    .unwrap()
                    .id,
            )
        }
    }
}
