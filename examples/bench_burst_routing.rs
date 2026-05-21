//! Burst routing workload: a router actor receives 10,000 messages and
//! round-robins them to 20 worker actors with no artificial delay.
//!
//! This tests raw message dispatch throughput — the classic case where
//! crossbeam's batch-stealing is expected to win.
//!
//! Expected metric: messages/sec through the router.

use aktor::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

const N_WORKERS: usize = 20;
const N_MESSAGES: u64 = 10_000;

#[derive(Debug, Clone)]
enum Msg {
    Work { sent_at: Instant },
}

impl Message for Msg {
    fn type_id(&self) -> &'static str {
        "BurstMsg"
    }
}

#[derive(Debug)]
struct RouterActor {
    workers: Vec<ActorRef<Msg>>,
    cursor: usize,
}

impl Actor for RouterActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, _ctx: &ActorContext<Msg>) {
        let target = &self.workers[self.cursor % N_WORKERS];
        let _ = target.tell(msg, None);
        self.cursor += 1;
    }
}

#[derive(Debug)]
struct WorkerActor {
    done: Arc<AtomicU64>,
    total_latency_us: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl Actor for WorkerActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, _ctx: &ActorContext<Msg>) {
        let Msg::Work { sent_at } = msg;
        let latency_us = sent_at.elapsed().as_micros() as u64;
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        let prev = self.done.fetch_add(1, Ordering::Relaxed);
        if prev + 1 == N_MESSAGES {
            self.notify.notify_one();
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

    let mut workers = Vec::new();
    for i in 0..N_WORKERS {
        let r = system
            .spawn_actor(
                &format!("worker-{i}"),
                WorkerActor {
                    done: done.clone(),
                    total_latency_us: total_latency_us.clone(),
                    notify: notify.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();
        workers.push(r);
    }

    let router = system
        .spawn_actor(
            "router",
            RouterActor {
                workers: workers.clone(),
                cursor: 0,
            },
            ActorProps::default(),
        )
        .unwrap();

    let start = Instant::now();

    for _ in 0..N_MESSAGES {
        router
            .tell(
                Msg::Work {
                    sent_at: Instant::now(),
                },
                None,
            )
            .unwrap();
    }

    notify.notified().await;
    let elapsed = start.elapsed();

    let n = done.load(Ordering::Relaxed);
    let avg_lat_us = total_latency_us.load(Ordering::Relaxed) as f64 / n as f64;
    let throughput = n as f64 / elapsed.as_secs_f64();

    println!(
        "BENCH_JSON:{{\"bench\":\"burst_routing\",\"msgs\":{n},\"ms\":{},\"msgs_sec\":{throughput:.0},\"avg_lat_us\":{avg_lat_us:.1}}}",
        elapsed.as_millis()
    );

    system.shutdown().await.unwrap();
}
