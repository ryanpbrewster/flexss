use std::fmt::Debug;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use crate::{combine, hash, Backend, BackendId, Health, Picker, TenantId};

pub struct RendevouzShuffle {
    backends: Vec<Backend>,
    shard_size: usize,
    prng: SmallRng,
    scratch: Vec<Entry>,
}
impl Picker for RendevouzShuffle {
    fn new(shard_size: usize) -> Self {
        Self {
            backends: Vec::new(),
            shard_size,
            prng: SmallRng::seed_from_u64(42),
            scratch: Vec::with_capacity(shard_size),
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
        self.scratch.extend(self.backends[..self.shard_size].iter().copied().map(|b| Entry { score: combine(th, b.hash), b }));
        rebuild_heap(&mut self.scratch);
        for &b in &self.backends[self.shard_size..] {
            if b.health == Health::Draining {
                continue;
            }
            let score = combine(th, b.hash);

            if score < self.scratch[0].score {
                self.scratch[0] = Entry { score, b };
                sift_down(&mut self.scratch, 0);
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Entry {
    score: u64,
    b: Backend,
}

fn sift_down<T: Ord>(xs: &mut [T], mut cur: usize) {
    let mut child = 2*cur + 1;
    let end = xs.len();
    while child < end.saturating_sub(2){
        child += (xs[child] <= xs[child + 1]) as usize;
        if xs[cur] >= xs[child] {
            return;
        }

        xs.swap(cur, child);
        cur = child;
        child = 2 * cur + 1;
    }
    if child < xs.len() && xs[cur] < xs[child] {
        xs.swap(cur, child);
    }
}
fn rebuild_heap<T: Ord + Debug>(xs: &mut [T]) {
    let mut n = xs.len() / 2;
    while n > 0 {
        n -= 1;
        sift_down(xs, n);
    }
}

#[cfg(test)]
mod test {
    use super::rebuild_heap;

    #[test]
    fn rebuild_test() {
        let mut xs = [3, 1, 4, 1, 5, 9, 2, 6];
        rebuild_heap(&mut xs);
        assert_eq!(xs, [9, 6, 4, 1, 5, 3, 2, 1]);
    }
}