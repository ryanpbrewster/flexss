use criterion::{black_box, criterion_group, criterion_main, Criterion};
use flexss::{rendevouz_shuffle::RendevouzShuffle, BackendId, Health, Picker, TenantId};

fn rendevouz_shuffle(c: &mut Criterion) {
    let mut p = RendevouzShuffle::new(100);
    let backends: Vec<BackendId> = (0..1_000).map(BackendId).collect();
    for &b in &backends {
        p.register(b, Health::Up);
    }
    let tenant_id = TenantId(0);
    c.bench_function("rendevous_shuffle_pick", |b| {
        b.iter(|| black_box(p.pick(tenant_id)))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = rendevouz_shuffle,
}
criterion_main!(benches);
