//! Demonstrates registering and using the HTTP extensions with an actor system.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example http_extension --features http
//! ```
//!
//! The example:
//! 1. Registers `AsyncHttpClientExtension` and `HttpClientExtension` with the system.
//! 2. Spawns a `FetchActor` that issues an async GET via `ctx.pipe_to_self`.
//! 3. Exercises the blocking `HttpClientExtension` directly from main.

use aktor::extensions::{AsyncHttpClientExtension, HttpClientExtension};
use aktor::{Actor, ActorContext, ActorProps, ActorSystem, ActorSystemConfig, Message};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::info;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum FetchMsg {
    Fetch(String),
    Done(String),
}

impl Message for FetchMsg {
    fn type_id(&self) -> &'static str {
        "FetchMsg"
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct FetchActor {
    notify: Arc<Notify>,
    success: Arc<AtomicBool>,
}

impl Actor for FetchActor {
    type Msg = FetchMsg;

    fn handle(&mut self, msg: FetchMsg, ctx: &ActorContext<FetchMsg>) {
        match msg {
            FetchMsg::Fetch(url) => {
                info!("Fetching {url} asynchronously …");
                // Grab the shared client from the extension registry.
                let client = ctx
                    .system()
                    .extension::<AsyncHttpClientExtension>()
                    .client();
                // pipe_to_self runs the future on a Tokio task and delivers
                // the result as the next FetchMsg::Done message.
                ctx.pipe_to_self::<_, String>(async move {
                    let status = client
                        .get(&url)
                        .send()
                        .await
                        .map(|r| r.status().to_string())
                        .unwrap_or_else(|e| format!("error: {e}"));
                    Ok(FetchMsg::Done(status))
                });
            }
            FetchMsg::Done(status) => {
                info!("Async response status: {status}");
                self.success.store(true, Ordering::Release);
                self.notify.notify_one();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let notify = Arc::new(Notify::new());
    let success = Arc::new(AtomicBool::new(false));

    let system = ActorSystem::new(ActorSystemConfig::default()).await?;

    // Register the async HTTP extension (shared connection pool).
    system.register_extension(AsyncHttpClientExtension::new());

    // Register the blocking HTTP extension.
    system.register_extension(HttpClientExtension::with_timeout(Duration::from_secs(10)));

    // Spawn the fetch actor.
    let actor_ref = system.spawn_actor(
        "fetcher",
        FetchActor {
            notify: notify.clone(),
            success: success.clone(),
        },
        ActorProps::default(),
    )?;

    // Kick off an async fetch.
    actor_ref.tell(FetchMsg::Fetch("https://httpbin.org/get".into()), None)?;

    // Wait for the actor to signal completion.
    notify.notified().await;

    assert!(
        success.load(Ordering::Acquire),
        "fetch actor did not set success flag"
    );

    // Demonstrate the blocking client independently.
    let http_ext = system.extension::<HttpClientExtension>();
    let resp = http_ext
        .client()
        .get("https://httpbin.org/status/200")
        .send()?;
    info!("Blocking GET status: {}", resp.status());
    assert!(resp.status().is_success(), "expected 2xx from blocking GET");

    system.shutdown().await?;

    println!("http_extension example completed successfully.");
    Ok(())
}
