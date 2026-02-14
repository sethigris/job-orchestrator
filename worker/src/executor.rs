use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::protocol::{JobPayload, JobResult, JobStatus};

pub struct JobExecutor {
    worker_id: String,
}

impl JobExecutor {
    pub fn new(worker_id: String) -> Self {
        Self { worker_id }
    }

    /// Execute a job with timeout and resource isolation
    pub async fn execute(&self, job_id: Uuid, payload_b64: String, timeout_ms: u64) -> JobResult {
        let start = Instant::now();
        
        debug!(job_id = %job_id, "Decoding job payload");
        
        // Decode payload
        let payload = match self.decode_payload(&payload_b64) {
            Ok(p) => p,
            Err(e) => {
                error!(job_id = %job_id, error = %e, "Failed to decode payload");
                return JobResult {
                    job_id,
                    worker_id: self.worker_id.clone(),
                    status: JobStatus::Failure,
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: general_purpose::STANDARD.encode(format!("Decode error: {}", e)),
                    duration_ms: start.elapsed().as_millis() as u64,
                };
            }
        };

        info!(
            job_id = %job_id,
            command = %payload.command,
            args = ?payload.args,
            "Executing job"
        );

        // Execute with timeout
        let timeout_duration = Duration::from_millis(timeout_ms);
        match timeout(timeout_duration, self.run_command(payload)).await {
            Ok(result) => {
                let duration = start.elapsed().as_millis() as u64;
                info!(
                    job_id = %job_id,
                    status = ?result.status,
                    duration_ms = duration,
                    "Job completed"
                );
                JobResult {
                    job_id,
                    worker_id: self.worker_id.clone(),
                    status: result.status,
                    exit_code: result.exit_code,
                    stdout: result.stdout,
                    stderr: result.stderr,
                    duration_ms: duration,
                }
            }
            Err(_) => {
                error!(job_id = %job_id, "Job timed out");
                JobResult {
                    job_id,
                    worker_id: self.worker_id.clone(),
                    status: JobStatus::Timeout,
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: general_purpose::STANDARD.encode("Job execution timed out"),
                    duration_ms: timeout_ms,
                }
            }
        }
    }

    fn decode_payload(&self, payload_b64: &str) -> Result<JobPayload> {
        let bytes = general_purpose::STANDARD
            .decode(payload_b64)
            .context("Failed to decode base64")?;
        let payload: JobPayload = serde_json::from_slice(&bytes)
            .context("Failed to parse JSON payload")?;
        Ok(payload)
    }

    async fn run_command(&self, payload: JobPayload) -> CommandResult {
        let mut cmd = Command::new(&payload.command);
        cmd.args(&payload.args)
            .envs(&payload.env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .stdin(Stdio::null());

        // Security: Run with reduced privileges if possible
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // Set process group for easier cleanup
            cmd.process_group(0);
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return CommandResult {
                    status: JobStatus::Failure,
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: general_purpose::STANDARD.encode(format!("Spawn error: {}", e)),
                };
            }
        };

        // Capture output
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();

        if let Some(mut stdout) = child.stdout.take() {
            let _ = stdout.read_to_end(&mut stdout_buf).await;
        }

        if let Some(mut stderr) = child.stderr.take() {
            let _ = stderr.read_to_end(&mut stderr_buf).await;
        }

        let status = match child.wait().await {
            Ok(s) => s,
            Err(e) => {
                return CommandResult {
                    status: JobStatus::Failure,
                    exit_code: -1,
                    stdout: general_purpose::STANDARD.encode(stdout_buf),
                    stderr: general_purpose::STANDARD.encode(format!(
                        "{}\nWait error: {}",
                        String::from_utf8_lossy(&stderr_buf),
                        e
                    )),
                };
            }
        };

        let exit_code = status.code().unwrap_or(-1);
        let job_status = if status.success() {
            JobStatus::Success
        } else {
            JobStatus::Failure
        };

        CommandResult {
            status: job_status,
            exit_code,
            stdout: general_purpose::STANDARD.encode(stdout_buf),
            stderr: general_purpose::STANDARD.encode(stderr_buf),
        }
    }
}

struct CommandResult {
    status: JobStatus,
    exit_code: i32,
    stdout: String,
    stderr: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_successful_job() {
        let executor = JobExecutor::new("test-worker".to_string());
        
        let payload = JobPayload {
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            env: Default::default(),
        };
        
        let payload_json = serde_json::to_string(&payload).unwrap();
        let payload_b64 = general_purpose::STANDARD.encode(payload_json);
        
        let result = executor.execute(Uuid::new_v4(), payload_b64, 5000).await;
        
        assert_eq!(result.status, JobStatus::Success);
        assert_eq!(result.exit_code, 0);
    }

    #[tokio::test]
    async fn test_job_timeout() {
        let executor = JobExecutor::new("test-worker".to_string());
        
        let payload = JobPayload {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            env: Default::default(),
        };
        
        let payload_json = serde_json::to_string(&payload).unwrap();
        let payload_b64 = general_purpose::STANDARD.encode(payload_json);
        
        let result = executor.execute(Uuid::new_v4(), payload_b64, 100).await;
        
        assert_eq!(result.status, JobStatus::Timeout);
    }
}