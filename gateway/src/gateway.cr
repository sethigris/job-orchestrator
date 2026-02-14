require "http/server"
require "json"
require "uuid"
require "base64"

# Standalone Crystal Gateway for Distributed Job Orchestrator
# This file can be used independently of the main project

module JobGateway
  VERSION = "0.1.0"

  # Client for communicating with the Erlang coordinator
  class CoordinatorClient
    def initialize(@host : String, @port : Int32)
    end

    def submit_job(payload : JobPayload, priority : Int32, max_retries : Int32, timeout_ms : Int64) : String
      job_id = UUID.random.to_s

      request = {
        "type" => "register_job",
        "job_id" => job_id,
        "payload" => encode_payload(payload),
        "priority" => priority,
        "max_retries" => max_retries,
        "timeout_ms" => timeout_ms,
        "submitted_at" => Time.utc.to_rfc3339
      }

      send_message(request)
      job_id
    end

    private def encode_payload(payload : JobPayload) : String
      json_payload = {
        "command" => payload.command,
        "args" => payload.args,
        "env" => payload.env
      }.to_json

      Base64.strict_encode(json_payload)
    end

    private def send_message(message : Hash(String, String | Int32 | Int64))
      socket = TCPSocket.new(@host, @port)
      
      json = message.to_json
      length = json.bytesize
      
      # Write length prefix (4 bytes big-endian)
      io = IO::Memory.new
      io.write_bytes(length.to_u32, IO::ByteFormat::BigEndian)
      socket.write(io.to_slice)
      
      # Write message
      socket.write(json.to_slice)
      socket.close
    rescue ex
      puts "Error sending message: #{ex.message}"
      raise ex
    end
  end

  # Job payload structure
  struct JobPayload
    property command : String
    property args : Array(String)
    property env : Hash(String, String)

    def initialize(@command, @args = [] of String, @env = {} of String => String)
    end

    def to_json(builder : JSON::Builder)
      builder.object do
        builder.field "command", @command
        builder.field "args", @args
        builder.field "env", @env
      end
    end
  end

  # Main HTTP API Server
  class APIServer
    def initialize(@port : Int32, @coordinator_host : String, @coordinator_port : Int32)
      @client = CoordinatorClient.new(@coordinator_host, @coordinator_port)
    end

    def start
      server = HTTP::Server.new do |context|
        handle_request(context)
      end

      address = server.bind_tcp "0.0.0.0", @port
      puts "═══════════════════════════════════════"
      puts "  Job Gateway Server Started"
      puts "═══════════════════════════════════════"
      puts ""
      puts "Gateway listening on http://#{address}"
      puts "Coordinator: #{@coordinator_host}:#{@coordinator_port}"
      puts ""
      puts "Endpoints:"
      puts "  POST /jobs        - Submit a job"
      puts "  GET  /jobs/:id    - Query job status"
      puts "  GET  /health      - Health check"
      puts ""
      puts "Example:"
      puts "  curl -X POST http://localhost:#{@port}/jobs \\"
      puts "    -H 'Content-Type: application/json' \\"
      puts "    -d '{\"command\":\"echo\",\"args\":[\"hello\"]}'"
      puts ""
      
      server.listen
    end

    private def handle_request(context)
      path = context.request.path
      method = context.request.method

      # Log request
      puts "[#{Time.utc}] #{method} #{path}"

      case {method, path}
      when {"POST", "/jobs"}
        handle_submit_job(context)
      when {"GET", "/jobs/*"}
        # Handle all GET requests that start with /jobs/
        handle_query_job(context)
      when {"GET", "/health"}
        handle_health(context)
      else
        context.response.status = HTTP::Status::NOT_FOUND
        context.response.content_type = "application/json"
        context.response.print({
          "error" => "Not found",
          "path" => path
        }.to_json)
      end
    rescue ex
      puts "Error handling request: #{ex.message}"
      puts ex.backtrace.join("\n")
      
      context.response.status = HTTP::Status::INTERNAL_SERVER_ERROR
      context.response.content_type = "application/json"
      context.response.print({
        "error" => ex.message
      }.to_json)
    end

    private def handle_submit_job(context)
      body = context.request.body.try(&.gets_to_end)
      
      unless body
        context.response.status = HTTP::Status::BAD_REQUEST
        context.response.content_type = "application/json"
        context.response.print({"error" => "Empty body"}.to_json)
        return
      end

      request = JSON.parse(body)
      
      # Extract command
      command = request["command"]?.try(&.as_s)
      
      unless command
        context.response.status = HTTP::Status::BAD_REQUEST
        context.response.content_type = "application/json"
        context.response.print({"error" => "Missing 'command' field"}.to_json)
        return
      end
      
      # Extract optional args
      args = request["args"]?.try(&.as_a.map(&.as_s)) || [] of String
      
      # Extract optional environment variables
      env = request["env"]?.try do |e|
        hash = {} of String => String
        e.as_h.each { |k, v| hash[k] = v.as_s }
        hash
      end || {} of String => String
      
      # Extract job configuration
      priority = request["priority"]?.try(&.as_i) || 5
      max_retries = request["max_retries"]?.try(&.as_i) || 3
      timeout_ms = request["timeout_ms"]?.try(&.as_i64) || 30000_i64

      # Validate priority
      if priority < 1 || priority > 10
        context.response.status = HTTP::Status::BAD_REQUEST
        context.response.content_type = "application/json"
        context.response.print({
          "error" => "Priority must be between 1 and 10"
        }.to_json)
        return
      end

      # Create and submit job
      payload = JobPayload.new(command, args, env)
      job_id = @client.submit_job(payload, priority, max_retries, timeout_ms)

      puts "Job submitted: #{job_id} (priority: #{priority})"

      context.response.status = HTTP::Status::CREATED
      context.response.content_type = "application/json"
      context.response.print({
        "job_id" => job_id,
        "status" => "submitted",
        "priority" => priority,
        "max_retries" => max_retries,
        "timeout_ms" => timeout_ms
      }.to_json)
    end

    private def handle_query_job(context)
      job_id = context.request.path.split("/").last
      
      # In a real implementation, we would query the coordinator for job status
      # For now, return a placeholder response
      context.response.content_type = "application/json"
      context.response.status = HTTP::Status::OK
      context.response.print({
        "job_id" => job_id,
        "status" => "pending",
        "message" => "Job status query not yet implemented. Check coordinator event log."
      }.to_json)
    end

    private def handle_health(context)
      context.response.content_type = "application/json"
      context.response.status = HTTP::Status::OK
      context.response.print({
        "status" => "healthy",
        "service" => "job-gateway",
        "version" => VERSION,
        "coordinator" => {
          "host" => @coordinator_host,
          "port" => @coordinator_port
        },
        "timestamp" => Time.utc.to_rfc3339
      }.to_json)
    end
  end
end

# Main entry point
if ARGV.size < 1
  puts "Usage: crystal run gateway.cr -- [port] [coordinator_host] [coordinator_port]"
  puts "Example: crystal run gateway.cr -- 8080 127.0.0.1 9000"
  puts ""
  puts "Or use environment variables:"
  puts "  COORDINATOR_HOST=192.168.1.10 COORDINATOR_PORT=9000 crystal run gateway.cr -- 8080"
  exit 1
end

port = ARGV[0].to_i

# Allow coordinator host/port from command line or environment
coordinator_host = if ARGV.size > 1
  ARGV[1]
else
  ENV.fetch("COORDINATOR_HOST", "127.0.0.1")
end

coordinator_port = if ARGV.size > 2
  ARGV[2].to_i
else
  ENV.fetch("COORDINATOR_PORT", "9000").to_i
end

# Create and start server
server = JobGateway::APIServer.new(port, coordinator_host, coordinator_port)
server.start