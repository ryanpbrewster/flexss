use std::{
    collections::BinaryHeap,
    ops::{Deref, DerefMut},
};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{combine, hash, Backend, BackendId, Health, Picker, TenantId};

pub struct RendevouzShuffle {
    backends: Vec<Backend>,
    shard_size: usize,
    prng: SmallRng,
    scratch: BinaryHeap<Entry>,
}
impl Picker for RendevouzShuffle {
    fn new(shard_size: usize) -> Self {
        Self {
            backends: Vec::new(),
            shard_size,
            prng: SmallRng::seed_from_u64(42),
            scratch: BinaryHeap::with_capacity(shard_size),
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
        }
    }

    fn unregister(&mut self, id: BackendId) {
        self.backends.retain(|b| b.id != id);
    }

    fn pick(&mut self, id: TenantId) -> Option<BackendId> {
        assert!(self.backends.len() >= self.shard_size);

        let th = hash(id);

        self.scratch.clear();
        for &b in &self.backends {
            if b.health == Health::Draining {
                continue;
            }
            let score = combine(th, b.hash);
            if self.scratch.len() < self.shard_size {
                self.scratch.push(Entry { score, b });
            } else {
                let mut cur = self.scratch.peek_mut().unwrap();
                if score < cur.deref().score {
                    *cur.deref_mut() = Entry { score, b };
                }
            }
        }
        let healthy = self
            .scratch
            .iter()
            .filter(|e| e.b.health == Health::Up)
            .count();
        if healthy == 0 {
            None
        } else {
            Some(
                self.scratch
                    .iter()
                    .filter(|e| e.b.health == Health::Up)
                    .nth(self.prng.gen_range(0..healthy))
                    .unwrap()
                    .b
                    .id,
            )
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    score: u64,
    b: Backend,
}
