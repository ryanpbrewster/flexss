use criterion::{black_box, criterion_group, criterion_main, Criterion};
use flexss::{rendevouz_shuffle::RendevouzShuffle, BackendId, Health, Picker, TenantId};

fn rendevouz_shuffle_large(c: &mut Criterion) {
    let mut p = RendevouzShuffle::new(100);
    let backends: Vec<BackendId> = (0..1_000).map(BackendId).collect();
    for &b in &backends {
        p.register(b, Health::Up);
    }
    let tenant_id = TenantId(0);
    c.bench_function("rendevous_shuffle_large", |b| {
        b.iter(|| black_box(p.pick(tenant_id)))
    });
}

fn rendevouz_shuffle_small(c: &mut Criterion) {
    let mut p = RendevouzShuffle::new(6);
    let backends: Vec<BackendId> = (0..30).map(BackendId).collect();
    for &b in &backends {
        p.register(b, Health::Up);
    }
    let tenant_id = TenantId(0);
    c.bench_function("rendevous_shuffle_small", |b| {
        b.iter(|| black_box(p.pick(tenant_id)))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = rendevouz_shuffle_large, rendevouz_shuffle_small,
}
criterion_main!(benches);
