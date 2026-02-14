use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Message envelope for all protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    #[serde(rename = "execute_job")]
    ExecuteJob(ExecuteJob),
    
    #[serde(rename = "job_result")]
    JobResult(JobResult),
    
    #[serde(rename = "heartbeat")]
    Heartbeat(Heartbeat),
    
    #[serde(rename = "register_worker")]
    RegisterWorker(RegisterWorker),
    
    #[serde(rename = "worker_registered")]
    WorkerRegistered(WorkerRegistered),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteJob {
    pub job_id: Uuid,
    pub payload: String, // base64-encoded
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: Uuid,
    pub worker_id: String,
    pub status: JobStatus,
    pub exit_code: i32,
    pub stdout: String, // base64-encoded
    pub stderr: String, // base64-encoded
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Success,
    Failure,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub worker_id: String,
    pub load: f64,
    pub jobs_running: usize,
    pub jobs_completed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterWorker {
    pub worker_id: String,
    pub max_concurrent: usize,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistered {
    pub worker_id: String,
    pub coordinator_id: String,
}

/// Decoded job payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPayload {
    pub command: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

impl Message {
    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        let len = (json.len() as u32).to_be_bytes();
        Ok([&len[..], &json[..]].concat())
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let heartbeat = Message::Heartbeat(Heartbeat {
            worker_id: "worker-1".to_string(),
            load: 0.5,
            jobs_running: 2,
            jobs_completed: 100,
        });

        let bytes = heartbeat.to_bytes().unwrap();
        assert!(bytes.len() > 4);

        let decoded = Message::from_bytes(&bytes[4..]).unwrap();
        if let Message::Heartbeat(hb) = decoded {
            assert_eq!(hb.worker_id, "worker-1");
            assert_eq!(hb.jobs_running, 2);
        } else {
            panic!("Wrong message type");
        }
    }
}
