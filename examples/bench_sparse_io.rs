//! Sparse I/O workload: 20 domain actors each handling URLs one-at-a-time
//! with a 30ms simulated HTTP fetch delay. Between fetches each actor is idle.
//!
//! This tests idle-actor overhead. Crossbeam workers must busy-poll sleeping
//! actors; per-actor tasks just park in recv().await.
//!
//! Expected metric: total time ≈ URLS_PER_DOMAIN * FETCH_DELAY_MS (all domains
//! run in parallel, each domain is sequential).

use aktor::*;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const N_DOMAINS: usize = 20;
const URLS_PER_DOMAIN: usize = 10;
const TOTAL_URLS: usize = N_DOMAINS * URLS_PER_DOMAIN;
const FETCH_DELAY_MS: u64 = 30;

#[derive(Debug, Clone)]
enum Msg {
    Fetch { sent_at: Instant },
    FetchDone { latency_us: u64 },
}

impl Message for Msg {
    fn type_id(&self) -> &'static str {
        "SparsMsg"
    }
}

#[derive(Debug)]
struct DomainActor {
    queue: VecDeque<Instant>,
    in_flight: bool,
    done: Arc<AtomicU64>,
    total_latency_us: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl DomainActor {
    fn new(done: Arc<AtomicU64>, total_latency_us: Arc<AtomicU64>, notify: Arc<Notify>) -> Self {
        Self {
            queue: VecDeque::new(),
            in_flight: false,
            done,
            total_latency_us,
            notify,
        }
    }

    fn dispatch_next(&mut self, ctx: &ActorContext<Msg>) {
        if self.in_flight {
            return;
        }
        let Some(sent_at) = self.queue.pop_front() else {
            return;
        };
        self.in_flight = true;
        ctx.pipe_to_self(async move {
            tokio::time::sleep(Duration::from_millis(FETCH_DELAY_MS)).await;
            Ok::<Msg, ()>(Msg::FetchDone {
                latency_us: sent_at.elapsed().as_micros() as u64,
            })
        });
    }
}

impl Actor for DomainActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
        match msg {
            Msg::Fetch { sent_at } => {
                self.queue.push_back(sent_at);
                self.dispatch_next(ctx);
            }
            Msg::FetchDone { latency_us } => {
                self.in_flight = false;
                self.total_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
                let prev = self.done.fetch_add(1, Ordering::Relaxed);
                if prev + 1 == TOTAL_URLS as u64 {
                    self.notify.notify_one();
                }
                self.dispatch_next(ctx);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let done = Arc::new(AtomicU64::new(0));
    let total_latency_us = Arc::new(AtomicU64::new(0));
    let notify = Arc::new(Notify::new());

    let system = ActorSystem::new(ActorSystemConfig::default())
        .await
        .unwrap();

    let mut domains = Vec::new();
    for i in 0..N_DOMAINS {
        let done = done.clone();
        let total_latency_us = total_latency_us.clone();
        let notify = notify.clone();
        let r = system
            .spawn_actor(
                &format!("domain-{i}"),
                move || DomainActor::new(done.clone(), total_latency_us.clone(), notify.clone()),
                ActorProps::default(),
            )
            .unwrap();
        domains.push(r);
    }

    let start = Instant::now();

    for (idx, domain) in domains.iter().enumerate() {
        for _ in 0..URLS_PER_DOMAIN {
            domain
                .tell(
                    Msg::Fetch {
                        sent_at: Instant::now(),
                    },
                    None,
                )
                .unwrap();
        }
        let _ = idx;
    }

    notify.notified().await;
    let elapsed = start.elapsed();

    let n = done.load(Ordering::Relaxed);
    let avg_lat_ms = total_latency_us.load(Ordering::Relaxed) as f64 / n as f64 / 1_000.0;
    let throughput = n as f64 / elapsed.as_secs_f64();

    println!(
        "BENCH_JSON:{{\"bench\":\"sparse_io\",\"urls\":{n},\"ms\":{},\"urls_sec\":{throughput:.1},\"avg_lat_ms\":{avg_lat_ms:.1}}}",
        elapsed.as_millis()
    );

    system.shutdown().await.unwrap();
}
