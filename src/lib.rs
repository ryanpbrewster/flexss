#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct TenantId(pub u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct BackendId(pub u64);

pub trait Picker {
    fn new(shard_size: usize) -> Self;
    fn add_backend(&mut self, id: BackendId);
    fn remove_backend(&mut self, id: BackendId);
    fn pick(&mut self, id: TenantId) -> Option<BackendId>;
}

pub mod naive_shuffle;
