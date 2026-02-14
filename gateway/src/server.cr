require "http/server"
require "json"
require "uuid"
require "base64"

module JobGateway
  VERSION = "0.1.0"

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

  class APIServer
    def initialize(@port : Int32, @coordinator_host : String, @coordinator_port : Int32)
      @client = CoordinatorClient.new(@coordinator_host, @coordinator_port)
    end

    def start
      server = HTTP::Server.new do |context|
        handle_request(context)
      end

      address = server.bind_tcp "0.0.0.0", @port
      puts "Gateway listening on http://#{address}"
      puts "Coordinator: #{@coordinator_host}:#{@coordinator_port}"
      puts "\nEndpoints:"
      puts "  POST /jobs - Submit a job"
      puts "  GET  /jobs/:id - Query job status"
      puts "  GET  /health - Health check"
      puts ""
      
      server.listen
    end

    private def handle_request(context)
      path = context.request.path
      method = context.request.method

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
          "error" => "Not found"
        }.to_json)
      end
    rescue ex
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
        context.response.print({"error" => "Empty body"}.to_json)
        return
      end

      request = JSON.parse(body)
      
      # Check if command exists
      command = request["command"]?.try(&.as_s)
      
      unless command
        context.response.status = HTTP::Status::BAD_REQUEST
        context.response.content_type = "application/json"
        context.response.print({"error" => "Missing 'command' field"}.to_json)
        return
      end
      
      args = request["args"]?.try(&.as_a.map(&.as_s)) || [] of String
      env = request["env"]?.try do |e|
        hash = {} of String => String
        e.as_h.each { |k, v| hash[k] = v.as_s }
        hash
      end || {} of String => String
      
      priority = request["priority"]?.try(&.as_i) || 5
      max_retries = request["max_retries"]?.try(&.as_i) || 3
      timeout_ms = request["timeout_ms"]?.try(&.as_i64) || 30000_i64

      payload = JobPayload.new(command, args, env)
      job_id = @client.submit_job(payload, priority, max_retries, timeout_ms)

      context.response.status = HTTP::Status::CREATED
      context.response.content_type = "application/json"
      context.response.print({
        "job_id" => job_id,
        "status" => "submitted"
      }.to_json)
    end

    private def handle_query_job(context)
      job_id = context.request.path.split("/").last
      
      # In a real implementation, query coordinator for job status
      context.response.status = HTTP::Status::OK
      context.response.content_type = "application/json"
      context.response.print({
        "job_id" => job_id,
        "status" => "pending",
        "message" => "Job status query not yet implemented"
      }.to_json)
    end

    private def handle_health(context)
      context.response.status = HTTP::Status::OK
      context.response.content_type = "application/json"
      context.response.print({
        "status" => "healthy",
        "service" => "job-gateway",
        "version" => VERSION
      }.to_json)
    end
  end
end

# Main entry point
if ARGV.size < 1
  puts "Usage: crystal run src/server.cr -- [port]"
  puts "Example: crystal run src/server.cr -- 8080"
  exit 1
end

port = ARGV[0].to_i
coordinator_host = ENV.fetch("COORDINATOR_HOST", "127.0.0.1")
coordinator_port = ENV.fetch("COORDINATOR_PORT", "9000").to_i

server = JobGateway::APIServer.new(port, coordinator_host, coordinator_port)
server.start