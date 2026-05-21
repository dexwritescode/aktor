//! Idle-actor scheduling pressure: 200 actors spawned, 195 permanently idle,
//! 5 active "ping" actors that schedule themselves every PING_INTERVAL_MS.
//!
//! When a ping arrives the actor records how long after the expected deadline
//! it actually fired (scheduling jitter). Higher jitter = scheduler is wasting
//! time on the 195 idle actors.
//!
//! Expected result: per-actor (tokio) approach shows lower jitter because idle
//! actors are parked tasks that tokio ignores. Crossbeam workers wake every
//! 100µs regardless of whether any actor has work.

use aktor::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const N_IDLE: usize = 195;
const N_ACTIVE: usize = 5;
const PING_INTERVAL_MS: u64 = 50;
const RUN_DURATION_MS: u64 = 2_000;
const EXPECTED_PINGS: u64 = (RUN_DURATION_MS / PING_INTERVAL_MS) * N_ACTIVE as u64;

#[derive(Debug, Clone)]
enum Msg {
    /// Periodic self-ping for active actors
    Ping { scheduled_at: Instant },
}

impl Message for Msg {
    fn type_id(&self) -> &'static str {
        "IdleMsg"
    }
}

// ── Idle actor: sits quietly forever ─────────────────────────────────────────

#[derive(Debug, Default)]
struct IdleActor;

impl Actor for IdleActor {
    type Msg = Msg;
    fn handle(&mut self, _msg: Msg, _ctx: &ActorContext<Msg>) {}
}

// ── Active ping actor ─────────────────────────────────────────────────────────

#[derive(Debug)]
struct PingActor {
    deadline: Instant,
    ping_count: u64,
    max_pings: u64,
    total_jitter_us: Arc<AtomicU64>,
    max_jitter_us: Arc<AtomicU64>,
    done_count: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Actor for PingActor {
    type Msg = Msg;

    fn pre_start(&mut self, ctx: &ActorContext<Msg>) -> Result<(), ActorError> {
        self.deadline = Instant::now() + Duration::from_millis(PING_INTERVAL_MS);
        ctx.schedule_to_self(
            Duration::from_millis(PING_INTERVAL_MS),
            Msg::Ping {
                scheduled_at: Instant::now(),
            },
        );
        Ok(())
    }

    fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
        let Msg::Ping { scheduled_at } = msg;
        let jitter_us = scheduled_at.elapsed().as_micros() as u64;
        self.total_jitter_us.fetch_add(jitter_us, Ordering::Relaxed);
        let mut cur = self.max_jitter_us.load(Ordering::Relaxed);
        while jitter_us > cur {
            match self.max_jitter_us.compare_exchange_weak(
                cur,
                jitter_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
        self.ping_count += 1;
        if self.ping_count < self.max_pings {
            ctx.schedule_to_self(
                Duration::from_millis(PING_INTERVAL_MS),
                Msg::Ping {
                    scheduled_at: Instant::now(),
                },
            );
        } else {
            let prev = self.done_count.fetch_add(1, Ordering::Relaxed);
            if prev + 1 == N_ACTIVE {
                self.notify.notify_one();
            }
        }
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let total_jitter_us = Arc::new(AtomicU64::new(0));
    let max_jitter_us = Arc::new(AtomicU64::new(0));
    let done_count = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    let system = ActorSystem::new(ActorSystemConfig::default())
        .await
        .unwrap();

    // Spawn idle actors — they will never receive a message.
    for i in 0..N_IDLE {
        system
            .spawn_actor(&format!("idle-{i}"), IdleActor, ActorProps::default())
            .unwrap();
    }

    let max_pings = RUN_DURATION_MS / PING_INTERVAL_MS;

    // Spawn active ping actors.
    for i in 0..N_ACTIVE {
        system
            .spawn_actor(
                &format!("ping-{i}"),
                PingActor {
                    deadline: Instant::now(),
                    ping_count: 0,
                    max_pings,
                    total_jitter_us: total_jitter_us.clone(),
                    max_jitter_us: max_jitter_us.clone(),
                    done_count: done_count.clone(),
                    notify: notify.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();
    }

    notify.notified().await;

    let total_pings = EXPECTED_PINGS;
    let avg_jitter_us = total_jitter_us.load(Ordering::Relaxed) as f64 / total_pings as f64;
    let max_jitter = max_jitter_us.load(Ordering::Relaxed);

    println!(
        "BENCH_JSON:{{\"bench\":\"idle_latency\",\"pings\":{total_pings},\"avg_jitter_us\":{avg_jitter_us:.1},\"max_jitter_us\":{max_jitter},\"idle_actors\":{N_IDLE}}}",
    );

    system.shutdown().await.unwrap();
}
