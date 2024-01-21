use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

use rand::{rngs::SmallRng, SeedableRng, seq::SliceRandom};

use crate::{Backend, BackendId, Health, Picker, TenantId};

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
        if let Some(existing) = self.backends.iter_mut().find(|b| b.id == id) {
            existing.health = health;
        } else {
            self.backends.push(Backend { id, health });
        }
    }

    fn unregister(&mut self, id: BackendId) {
        self.backends.retain(|b| b.id != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        assert!(self.backends.len() >= self.shard_size);
        let mut shard = BTreeMap::new();
        for i in 0 .. self.shard_size {
            let best = self.backends.iter().filter(|b| b.health != Health::Draining && !shard.contains_key(&b.id))
                .max_by_key(|b| {
                    let mut h = DefaultHasher::new();
                    i.hash(&mut h);
                    id.hash(&mut h);
                    b.id.hash(&mut h);
                    h.finish()
                })?;
            shard.insert(best.id, best.health);
        }

        let mut healthy: Vec<BackendId> = shard.into_iter().filter(|&(_b, h)| h == Health::Up).map(|(b, _h)| b).collect();
        healthy.sort();
        healthy.choose(&mut self.prng).copied()
    }
}
