use aktor::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Simple message for benchmarking
#[derive(Debug, Clone)]
struct BenchMessage {
    // content: String,
    timestamp: Instant,
    processed_counter: Arc<AtomicU64>,
}

impl Message for BenchMessage {
    fn type_id(&self) -> &'static str {
        "BenchMessage"
    }
}

// Benchmark actor that processes messages and sends to other actors
#[derive(Debug)]
struct BenchActor {
    message_count: u64,
}

impl BenchActor {
    fn new() -> Self {
        Self { message_count: 0 }
    }

    /// Simulate light computational work
    /// This does some simple arithmetic to prevent the compiler from optimizing it away
    fn simulate_work(&mut self) {
        // Simulate ~10 microseconds of CPU work
        let mut sum = 0u64;
        for i in 0..100 {
            sum = sum.wrapping_add(i * self.message_count);
            sum = sum.wrapping_mul(1103515245).wrapping_add(12345); // LCG step
        }
        // Prevent compiler from optimizing away the loop
        std::hint::black_box(sum);
    }
}

impl Actor<BenchMessage> for BenchActor {
    fn handle(&mut self, msg: BenchMessage, _ctx: &ActorContext<BenchMessage>) {
        self.message_count += 1;

        // Increment processed counter
        msg.processed_counter.fetch_add(1, Ordering::Relaxed);

        // Calculate latency (optional - not used currently)
        let _latency = msg.timestamp.elapsed().as_nanos() as u64;

        // Simulate computation work
        self.simulate_work();
    }
}

// Configuration for the benchmark
#[derive(Clone)]
struct BenchmarkConfig {
    actor_count: u32,
    messages_per_actor: u32,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            actor_count: 10000,
            messages_per_actor: 50000,
        }
    }
}

// Metrics collector
struct BenchmarkMetrics {
    // start_time: Instant,
}

impl BenchmarkMetrics {
    fn new() -> Self {
        Self {
            // start_time: Instant::now(),
        }
    }

    // fn print_stats(&self) {
    //     // let elapsed = self.start_time.elapsed();
    //
    //     println!("=== Performance Metrics ===");
    //     // println!("Duration: {:.2}s", elapsed.as_secs_f64());
    // }
}

async fn run_benchmark(config: BenchmarkConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aktor Performance Benchmark ===");
    println!("Actors: {}", config.actor_count);
    println!("Messages per actor: {}", config.messages_per_actor);
    println!(
        "Total messages: {}",
        config.actor_count * config.messages_per_actor
    );

    let _metrics = BenchmarkMetrics::new();

    // Create actor system
    let system_config = ActorSystemConfig {
        max_actors: config.actor_count as usize * 2,
        default_mailbox_size: 20000, // Large mailbox for high throughput
        ..Default::default()
    };
    let system = ActorSystem::new(system_config).await?;

    println!("\nCreating {} actors...", config.actor_count);
    let mut actor_refs = Vec::new();

    // Spawn all actors first
    for i in 0..config.actor_count {
        let actor = BenchActor::new(
            // i
        );

        let actor_ref = system
            .spawn_actor(
                &format!("bench-actor-{}", i),
                actor,
                ActorProps::new().with_mailbox_size(10000),
            )
            .await?;

        actor_refs.push(actor_ref);
    }

    println!("Actors created. Setting up interconnections...");
    sleep(Duration::from_millis(10000)).await;

    // Note: In our current design, we can't easily inject references to other actors
    // after creation, so actors will discover each other through the system registry
    // This is a design limitation we could address in future versions

    println!("Starting benchmark...");

    let benchmark_start = Instant::now();

    // Channel for passing metrics between tasks
    let (metrics_tx, mut metrics_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>();
    let processed_counter = Arc::new(AtomicU64::new(0));

    // Start message generation
    let message_sender = tokio::spawn({
        //let actor_refs = actor_refs.clone();
        //let config = config.clone();
        let metrics_tx = metrics_tx.clone();
        let processed_counter = processed_counter.clone();

        async move {
            let total_messages = (config.actor_count * config.messages_per_actor) as u64;
            let mut total_sent = 0u64;
            let mut total_failed = 0u64;

            println!(
                "Sending {} total messages as fast as possible...",
                total_messages
            );

            // Send all messages as fast as possible
            for i in 0..total_messages {
                let _sender_idx = (i % actor_refs.len() as u64) as usize;
                let target_idx = ((i + 1) % actor_refs.len() as u64) as usize;

                let message = BenchMessage {
                    // content: format!("benchmark_msg_{}", i),
                    timestamp: Instant::now(),
                    processed_counter: processed_counter.clone(),
                };

                //if let Err(_) = actor_refs[target_idx].tell(message, Some(actor_refs[sender_idx].clone())).await {
                if let Err(_) = actor_refs[target_idx].tell(message, None) {
                    total_failed += 1;
                } else {
                    total_sent += 1;
                }

                // Send periodic metrics updates
                if i % 10000 == 0 {
                    let _ = metrics_tx.send((total_sent, total_failed));
                }
            }

            // Send final metrics
            let _ = metrics_tx.send((total_sent, total_failed));
            println!(
                "Message sending complete! Sent: {}, Failed: {}",
                total_sent, total_failed
            );
        }
    });

    // Wait for message sending to complete and print final stats
    let _ = message_sender.await;

    let elapsed = benchmark_start.elapsed();

    // Get final metrics
    let mut final_sent = 0u64;
    let mut final_failed = 0u64;
    while let Ok((sent, failed)) = metrics_rx.try_recv() {
        final_sent = sent;
        final_failed = failed;
    }

    let send_rate = final_sent as f64 / elapsed.as_secs_f64();
    let error_rate = if final_sent > 0 {
        final_failed as f64 / (final_sent + final_failed) as f64 * 100.0
    } else {
        0.0
    };

    println!("=== FINAL RESULTS ===");
    println!("Duration: {:.2}s", elapsed.as_secs_f64());
    println!("Messages sent: {}", final_sent);
    println!("Messages failed: {}", final_failed);
    println!("Send rate: {:.0} msg/sec", send_rate);
    println!("Error rate: {:.2}%", error_rate);
    println!(
        "Messages per actor per second: {:.1}",
        send_rate / config.actor_count as f64
    );

    // Check how many messages were processed immediately
    let processed_at_send_complete = processed_counter.load(Ordering::Relaxed);
    println!("\n=== PROCESSING STATS ===");
    println!(
        "Messages processed during sending: {}/{}",
        processed_at_send_complete, final_sent
    );
    println!(
        "Processing rate: {:.1}%",
        (processed_at_send_complete as f64 / final_sent as f64) * 100.0
    );

    // Give time for in-flight messages to be processed
    println!("\nWaiting for remaining message processing...");
    sleep(Duration::from_secs(2)).await;

    let final_processed = processed_counter.load(Ordering::Relaxed);
    println!(
        "Total messages processed: {}/{}",
        final_processed, final_sent
    );
    println!(
        "Final processing rate: {:.1}%",
        (final_processed as f64 / final_sent as f64) * 100.0
    );

    // Cleanup
    println!("\nShutting down actor system...");
    system.shutdown().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure tracing for performance analysis
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN) // Reduce logging overhead
        .init();

    run_benchmark(BenchmarkConfig::default()).await
}
