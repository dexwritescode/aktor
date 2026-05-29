//! Full crawler topology: mirrors the muse actor hierarchy.
//! Frontier → DomainActors → ephemeral CrawlerActors → back to Frontier.
//!
//! - 1 frontier actor manages the URL queue and deduplication.
//! - Up to 5 domain actors, spawned lazily per domain.
//! - Each domain actor spawns an ephemeral child to "fetch" the URL.
//! - The ephemeral child sleeps (simulating HTTP) then reports back.
//! - Frontier tracks completion and stops when all URLs are crawled.
//!
//! This is the closest benchmark to actual muse runtime behaviour.
//! Tests the full message round-trip: frontier→domain→crawler→domain→frontier.

use aktor::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

const TOTAL_SEED_URLS: usize = 60;
const FETCH_DELAY_MS: u64 = 20;

#[derive(Debug, Clone)]
enum Msg {
    /// Frontier → DomainActor
    FetchUrl { url: String, sent_at: Instant },
    /// Ephemeral crawler → DomainActor → Frontier
    PageResult { url: String, latency_us: u64 },
    /// DomainActor notifies Frontier it has stopped
    DomainStopped(String),
}

impl Message for Msg {
    fn type_id(&self) -> &'static str {
        "TopoMsg"
    }
}

// ── Ephemeral crawler ────────────────────────────────────────────────────────

#[derive(Debug)]
struct CrawlerActor {
    reply_to: ActorRef<Msg>,
}

impl Actor for CrawlerActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
        if let Msg::FetchUrl { url, sent_at } = msg {
            let reply_to = self.reply_to.clone();
            let delay = Duration::from_millis(FETCH_DELAY_MS);
            // Simulate HTTP fetch asynchronously; report back via reply_to.
            ctx.pipe_to_self(async move {
                tokio::time::sleep(delay).await;
                let latency_us = sent_at.elapsed().as_micros() as u64;
                let _ = reply_to.tell(Msg::PageResult { url, latency_us }, None);
                Ok::<Msg, ()>(Msg::PageResult {
                    url: String::new(),
                    latency_us: 0,
                })
            });
        }
    }
}

// ── Domain actor ─────────────────────────────────────────────────────────────

#[derive(Debug)]
struct DomainActor {
    domain: String,
    queue: VecDeque<(String, Instant)>,
    in_flight: bool,
    frontier: ActorRef<Msg>,
}

impl DomainActor {
    fn dispatch_next(&mut self, ctx: &ActorContext<Msg>) {
        if self.in_flight {
            return;
        }
        let Some((url, sent_at)) = self.queue.pop_front() else {
            // idle and empty — stop self, frontier will get DomainStopped
            ctx.stop_self();
            return;
        };
        self.in_flight = true;
        let reply_to = ctx.actor_ref().clone();
        let name = format!("crawler-{}", uuid::Uuid::new_v4().simple());
        if let Ok(crawler_ref) = ctx.spawn_child(
            &name,
            move || CrawlerActor {
                reply_to: reply_to.clone(),
            },
            None,
        ) {
            let _ = crawler_ref.tell(Msg::FetchUrl { url, sent_at }, None);
        }
    }
}

impl Actor for DomainActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
        match msg {
            Msg::FetchUrl { url, sent_at } => {
                self.queue.push_back((url, sent_at));
                self.dispatch_next(ctx);
            }
            Msg::PageResult { url, latency_us } if !url.is_empty() => {
                self.in_flight = false;
                let _ = self
                    .frontier
                    .tell(Msg::PageResult { url, latency_us }, None);
                self.dispatch_next(ctx);
            }
            _ => {}
        }
    }

    fn post_stop(&mut self, _ctx: &ActorContext<Msg>) -> Result<(), ActorError> {
        let _ = self
            .frontier
            .tell(Msg::DomainStopped(self.domain.clone()), None);
        Ok(())
    }
}

// ── Frontier actor ───────────────────────────────────────────────────────────

#[derive(Debug)]
struct FrontierActor {
    seen: HashSet<String>,
    domains: HashMap<String, ActorRef<Msg>>,
    crawled: u64,
    total: u64,
    done: Arc<AtomicU64>,
    total_latency_us: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl FrontierActor {
    fn domain_of(url: &str) -> String {
        // extract "domainN" from "https://domainN.example.com/..."
        url.split('/')
            .nth(2)
            .unwrap_or("unknown")
            .split('.')
            .next()
            .unwrap_or("unknown")
            .to_string()
    }

    fn route(&mut self, url: String, sent_at: Instant, ctx: &ActorContext<Msg>) {
        let domain = Self::domain_of(&url);
        if !self.domains.contains_key(&domain) {
            let domain_name = domain.clone();
            let frontier_ref = ctx.actor_ref().clone();
            if let Ok(r) = ctx.spawn_child(
                &domain,
                move || DomainActor {
                    domain: domain_name.clone(),
                    queue: VecDeque::new(),
                    in_flight: false,
                    frontier: frontier_ref.clone(),
                },
                None,
            ) {
                self.domains.insert(domain.clone(), r);
            }
        }
        if let Some(r) = self.domains.get(&domain) {
            let _ = r.tell(Msg::FetchUrl { url, sent_at }, None);
        }
    }
}

impl Actor for FrontierActor {
    type Msg = Msg;

    fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
        match msg {
            Msg::FetchUrl { url, sent_at } => {
                if self.seen.insert(url.clone()) {
                    self.route(url, sent_at, ctx);
                }
            }
            Msg::PageResult { url, latency_us } if !url.is_empty() => {
                self.crawled += 1;
                self.total_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
                if self.crawled >= self.total {
                    self.done.fetch_add(self.crawled, Ordering::Relaxed);
                    self.notify.notify_one();
                }
            }
            Msg::DomainStopped(domain) => {
                self.domains.remove(&domain);
            }
            _ => {}
        }
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

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

    let done_f = done.clone();
    let total_latency_us_f = total_latency_us.clone();
    let notify_f = notify.clone();
    let frontier = system
        .spawn_actor(
            "frontier",
            move || FrontierActor {
                seen: HashSet::new(),
                domains: HashMap::new(),
                crawled: 0,
                total: TOTAL_SEED_URLS as u64,
                done: done_f.clone(),
                total_latency_us: total_latency_us_f.clone(),
                notify: notify_f.clone(),
            },
            ActorProps::default(),
        )
        .unwrap();

    let start = Instant::now();

    for i in 0..TOTAL_SEED_URLS {
        let domain_id = i % 5;
        let url = format!("https://domain{domain_id}.example.com/page-{i}");
        frontier
            .tell(
                Msg::FetchUrl {
                    url,
                    sent_at: Instant::now(),
                },
                None,
            )
            .unwrap();
    }

    notify.notified().await;
    let elapsed = start.elapsed();

    let n = done.load(Ordering::Relaxed);
    let avg_lat_ms = total_latency_us.load(Ordering::Relaxed) as f64 / n as f64 / 1_000.0;
    let throughput = n as f64 / elapsed.as_secs_f64();

    println!(
        "BENCH_JSON:{{\"bench\":\"crawler_topology\",\"urls\":{n},\"ms\":{},\"urls_sec\":{throughput:.1},\"avg_lat_ms\":{avg_lat_ms:.1}}}",
        elapsed.as_millis()
    );

    system.shutdown().await.unwrap();
}
