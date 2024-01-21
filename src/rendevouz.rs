use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

use crate::{Backend, Picker, BackendId, Health, TenantId};


pub struct Rendevouz {
    backends: Vec<Backend>,
}
impl Picker for Rendevouz {
    fn new(_shard_size: usize) -> Self {
        Self {
            backends: Vec::new(),
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
        self.backends.iter().filter(|b| b.health == Health::Up).max_by_key(|b| {
            let mut h = DefaultHasher::new();
            id.hash(&mut h);
            b.id.hash(&mut h);
            h.finish()
        }).map(|b| b.id)
    }
}