use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, warn};

use crate::protocol::Message;

const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB

pub struct Connection {
    stream: TcpStream,
    read_buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::with_capacity(8192),
        }
    }

    /// Send a message to the coordinator
    pub async fn send(&mut self, message: &Message) -> Result<()> {
        let bytes = message.to_bytes()
            .context("Failed to serialize message")?;
        
        debug!(msg_type = ?message, size = bytes.len(), "Sending message");
        
        self.stream.write_all(&bytes).await
            .context("Failed to write to socket")?;
        
        self.stream.flush().await
            .context("Failed to flush socket")?;
        
        Ok(())
    }

    /// Receive a message from the coordinator
    pub async fn receive(&mut self) -> Result<Option<Message>> {
        loop {
            // Try to parse a message from the buffer
            if let Some(message) = self.parse_message()? {
                return Ok(Some(message));
            }

            // Read more data
            let n = self.stream.read_buf(&mut self.read_buffer).await
                .context("Failed to read from socket")?;

            if n == 0 {
                // Connection closed
                if self.read_buffer.is_empty() {
                    return Ok(None);
                } else {
                    anyhow::bail!("Connection closed with incomplete message");
                }
            }
        }
    }

    fn parse_message(&mut self) -> Result<Option<Message>> {
        // Need at least 4 bytes for length prefix
        if self.read_buffer.len() < 4 {
            return Ok(None);
        }

        // Parse length
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&self.read_buffer[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        // Validate length
        if length > MAX_MESSAGE_SIZE {
            anyhow::bail!("Message too large: {} bytes", length);
        }

        // Check if we have the full message
        if self.read_buffer.len() < 4 + length {
            return Ok(None);
        }

        // Extract message
        self.read_buffer.advance(4);
        let data = self.read_buffer.split_to(length);
        
        let message = Message::from_bytes(&data)
            .context("Failed to deserialize message")?;
        
        debug!(msg_type = ?message, "Received message");
        
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Heartbeat;

    #[tokio::test]
    async fn test_connection_roundtrip() {
        // Create a pair of connected sockets
        let (client, server) = tokio::io::duplex(1024);
        
        let mut client_conn = Connection {
            stream: client.into(),
            read_buffer: BytesMut::new(),
        };
        
        let mut server_conn = Connection {
            stream: server.into(),
            read_buffer: BytesMut::new(),
        };
        
        // Send message from client
        let msg = Message::Heartbeat(Heartbeat {
            worker_id: "test".to_string(),
            load: 0.5,
            jobs_running: 0,
            jobs_completed: 0,
        });
        
        client_conn.send(&msg).await.unwrap();
        
        // Receive on server
        let received = server_conn.receive().await.unwrap();
        assert!(received.is_some());
    }
}