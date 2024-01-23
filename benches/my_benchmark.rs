use criterion::{black_box, criterion_group, criterion_main, Criterion};
use flexss::{rendevouz_shuffle::RendevouzShuffle, BackendId, Health, Picker, TenantId};

fn rendevouz_shuffle(c: &mut Criterion) {
    for (name, n, k) in [
        ("large", 1_000, 100),
        ("midsize", 200, 20),
        ("small", 30, 6),
    ] {
        let mut p = RendevouzShuffle::new(k);
        let backends: Vec<BackendId> = (0..n).map(BackendId).collect();
        for &b in &backends {
            p.register(b, Health::Up);
        }
        let tenant_id = TenantId(0);
        c.bench_function(&format!("rendevous_shuffle_{name}"), |b| {
            b.iter(|| black_box(p.pick(tenant_id)))
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = rendevouz_shuffle,
}
criterion_main!(benches);
