use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, warn};
use tracing_subscriber;

mod protocol;
mod worker;
mod executor;
mod connection;

use worker::Worker;

#[derive(Parser, Debug)]
#[command(name = "job-worker")]
#[command(about = "High-performance job execution worker", long_about = None)]
struct Args {
    /// Worker ID (unique identifier)
    #[arg(short, long)]
    id: String,

    /// Coordinator address
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    coordinator: String,

    /// Worker listening port
    #[arg(short, long, default_value = "9001")]
    port: u16,

    /// Maximum concurrent jobs
    #[arg(short, long, default_value = "4")]
    max_concurrent: usize,

    /// Heartbeat interval in seconds
    #[arg(long, default_value = "5")]
    heartbeat_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    let args = Args::parse();
    
    info!(
        worker_id = %args.id,
        coordinator = %args.coordinator,
        port = args.port,
        max_concurrent = args.max_concurrent,
        "Starting worker"
    );

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    
    let worker = Worker::new(
        args.id,
        args.coordinator,
        args.max_concurrent,
        args.heartbeat_interval,
    );

    // Graceful shutdown handler
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");
        warn!("Shutdown signal received");
    };

    tokio::select! {
        result = worker.run() => {
            if let Err(e) = result {
                warn!("Worker stopped with error: {}", e);
            }
        }
        _ = shutdown_signal => {
            info!("Shutting down gracefully");
        }
    }

    Ok(())
}
