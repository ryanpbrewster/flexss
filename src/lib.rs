use std::{collections::hash_map::DefaultHasher, hash::Hasher, ops::BitXor};

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord, Hash)]
pub struct TenantId(pub u64);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct BackendId(pub u64);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum Health {
    Up,
    Draining,
    Down,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Backend {
    id: BackendId,
    health: Health,
    hash: u64,
}

pub trait Picker {
    fn new(shard_size: usize) -> Self;
    fn register(&mut self, id: BackendId, health: Health);
    fn unregister(&mut self, id: BackendId);
    fn pick(&mut self, id: TenantId) -> Option<BackendId>;
}

pub struct RoundRobin {
    idx: usize,
    backends: Vec<Backend>,
}
impl Picker for RoundRobin {
    fn new(_shard_size: usize) -> Self {
        Self {
            backends: Vec::new(),
            idx: 0,
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

    fn pick(&mut self, _id: TenantId) -> Option<BackendId> {
        if self.backends.is_empty() {
            return None;
        }
        for _ in 0..self.backends.len() {
            self.idx = (self.idx + 1) % self.backends.len();
            let b = self.backends[self.idx];
            if b.health != Health::Up {
                continue;
            }
            return Some(b.id);
        }
        None
    }
}

pub mod block_picker;
pub mod drain_aware_shuffle;
pub mod naive_shuffle;
pub mod rendevouz;
pub mod rendevouz_shuffle;

/// Taken from FxHash, this is a mediocre quality (but extremely fast!) way to
/// combine two hash values.
pub(crate) fn combine(a: u64, b: u64) -> u64 {
    const K: u64 = 0x517cc1b727220a95;
    a.rotate_left(5).bitxor(b).wrapping_mul(K)
}

/// This uses the hash_map default (should be SipHash 1-3 as of 2024-01).
pub(crate) fn hash<T: std::hash::Hash>(t: T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}
