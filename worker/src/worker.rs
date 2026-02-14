use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::connection::Connection;
use crate::executor::JobExecutor;
use crate::protocol::{ExecuteJob, Heartbeat, Message, RegisterWorker};

pub struct Worker {
    id: String,
    coordinator_addr: String,
    max_concurrent: usize,
    heartbeat_interval: Duration,
}

impl Worker {
    pub fn new(
        id: String,
        coordinator_addr: String,
        max_concurrent: usize,
        heartbeat_interval_secs: u64,
    ) -> Self {
        Self {
            id,
            coordinator_addr,
            max_concurrent,
            heartbeat_interval: Duration::from_secs(heartbeat_interval_secs),
        }
    }

    pub async fn run(self) -> Result<()> {
        info!(
            worker_id = %self.id,
            coordinator = %self.coordinator_addr,
            "Connecting to coordinator"
        );

        // Connect to coordinator
        let stream = TcpStream::connect(&self.coordinator_addr)
            .await
            .context("Failed to connect to coordinator")?;

        let mut connection = Connection::new(stream);

        // Register with coordinator
        self.register(&mut connection).await?;

        // Start worker loop
        self.worker_loop(connection).await
    }

    async fn register(&self, connection: &mut Connection) -> Result<()> {
        let register_msg = Message::RegisterWorker(RegisterWorker {
            worker_id: self.id.clone(),
            max_concurrent: self.max_concurrent,
            capabilities: vec!["shell".to_string()],
        });

        connection.send(&register_msg).await?;

        // Wait for registration confirmation
        match connection.receive().await? {
            Some(Message::WorkerRegistered(reg)) => {
                info!(
                    worker_id = %reg.worker_id,
                    coordinator_id = %reg.coordinator_id,
                    "Worker registered successfully"
                );
                Ok(())
            }
            Some(msg) => {
                anyhow::bail!("Unexpected message during registration: {:?}", msg);
            }
            None => {
                anyhow::bail!("Connection closed during registration");
            }
        }
    }

    async fn worker_loop(self, mut connection: Connection) -> Result<()> {
        // Channels for job execution
        let (job_tx, mut job_rx) = mpsc::channel::<ExecuteJob>(100);
        let (result_tx, mut result_rx) = mpsc::channel(100);

        // Semaphore to limit concurrent jobs
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let executor = Arc::new(JobExecutor::new(self.id.clone()));
        let worker_id = self.id.clone();

        // Stats tracking
        let jobs_running = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let jobs_completed = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Clone for heartbeat task
        let semaphore_for_heartbeat = semaphore.clone();
        let heartbeat_worker_id = self.id.clone();
        let heartbeat_jobs_running = Arc::clone(&jobs_running);
        let heartbeat_jobs_completed = Arc::clone(&jobs_completed);
        let heartbeat_interval = self.heartbeat_interval;
        let (heartbeat_tx, mut heartbeat_rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let mut ticker = interval(heartbeat_interval);
            loop {
                ticker.tick().await;
                let available_permits = semaphore_for_heartbeat.available_permits();
                let load = if available_permits > 0 {
                    heartbeat_jobs_running.load(std::sync::atomic::Ordering::Relaxed) as f64 / available_permits as f64
                } else {
                    0.0
                };
                
                let heartbeat = Message::Heartbeat(Heartbeat {
                    worker_id: heartbeat_worker_id.clone(),
                    load,
                    jobs_running: heartbeat_jobs_running.load(std::sync::atomic::Ordering::Relaxed),
                    jobs_completed: heartbeat_jobs_completed.load(std::sync::atomic::Ordering::Relaxed),
                });
                if heartbeat_tx.send(heartbeat).await.is_err() {
                    break;
                }
            }
        });

        // Clone for job execution task
        let semaphore_for_jobs = semaphore.clone();
        
        // Job execution task
        tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                let permit = semaphore_for_jobs.clone().acquire_owned().await.unwrap();
                let executor = Arc::clone(&executor);
                let result_tx = result_tx.clone();
                let jobs_running = Arc::clone(&jobs_running);
                let jobs_completed = Arc::clone(&jobs_completed);

                tokio::spawn(async move {
                    jobs_running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let result = executor.execute(job.job_id, job.payload, job.timeout_ms).await;
                    
                    jobs_running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    jobs_completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let _ = result_tx.send(Message::JobResult(result)).await;
                    drop(permit);
                });
            }
        });

        info!(worker_id = %worker_id, "Worker loop started");

        // Main event loop
        loop {
            tokio::select! {
                // Receive messages from coordinator
                msg = connection.receive() => {
                    match msg? {
                        Some(Message::ExecuteJob(job)) => {
                            info!(job_id = %job.job_id, "Received job");
                            if job_tx.send(job).await.is_err() {
                                error!("Failed to queue job for execution");
                            }
                        }
                        Some(msg) => {
                            warn!("Unexpected message: {:?}", msg);
                        }
                        None => {
                            warn!("Connection to coordinator closed");
                            break;
                        }
                    }
                }

                // Send job results to coordinator
                Some(result) = result_rx.recv() => {
                    if let Err(e) = connection.send(&result).await {
                        error!(error = %e, "Failed to send job result");
                    }
                }

                // Send heartbeats to coordinator
                Some(heartbeat) = heartbeat_rx.recv() => {
                    if let Err(e) = connection.send(&heartbeat).await {
                        error!(error = %e, "Failed to send heartbeat");
                    }
                }
            }
        }

        Ok(())
    }
}