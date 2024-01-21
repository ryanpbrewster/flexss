#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
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
            self.backends.push(Backend { id, health });
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
pub mod naive_shuffle;
