require "http/client"
require "json"
require "option_parser"

module JobGateway
  class CLI
    def initialize(@gateway_url : String)
    end

    def submit(command : String, args : Array(String), priority : Int32, max_retries : Int32, timeout_ms : Int64)
      payload = {
        "command" => command,
        "args" => args,
        "priority" => priority,
        "max_retries" => max_retries,
        "timeout_ms" => timeout_ms
      }

      response = HTTP::Client.post(
        "#{@gateway_url}/jobs",
        headers: HTTP::Headers{"Content-Type" => "application/json"},
        body: payload.to_json
      )

      if response.success?
        result = JSON.parse(response.body)
        puts "Job submitted successfully"
        puts "  Job ID: #{result["job_id"]}"
        puts "  Status: #{result["status"]}"
      else
        puts "Failed to submit job"
        puts "  Status: #{response.status}"
        puts "  Body: #{response.body}"
      end
    rescue ex
      puts "Error: #{ex.message}"
    end

    def query(job_id : String)
      response = HTTP::Client.get("#{@gateway_url}/jobs/#{job_id}")

      if response.success?
        result = JSON.parse(response.body)
        puts "Job Status:"
        puts "  ID: #{result["job_id"]}"
        puts "  Status: #{result["status"]}"
      else
        puts "Failed to query job"
        puts "  Status: #{response.status}"
      end
    rescue ex
      puts "Error: #{ex.message}"
    end

    def health
      response = HTTP::Client.get("#{@gateway_url}/health")

      if response.success?
        result = JSON.parse(response.body)
        puts "Gateway Health:"
        puts "  Status: #{result["status"]}"
        puts "  Service: #{result["service"]}"
        puts "  Version: #{result["version"]}"
      else
        puts "Gateway is unhealthy"
      end
    rescue ex
      puts "Error: #{ex.message}"
    end
  end
end

# Parse command line arguments
gateway_url = ENV.fetch("GATEWAY_URL", "http://localhost:8080")
command = ""
args = [] of String
priority = 5
max_retries = 3
timeout_ms = 30000_i64
action = "submit"
job_id = ""

parser = OptionParser.new do |parser|
  parser.banner = "Usage: job-cli [options] [command]"
  
  parser.on("-u URL", "--url=URL", "Gateway URL (default: http://localhost:8080)") do |url|
    gateway_url = url
  end
  
  parser.on("-p PRIORITY", "--priority=PRIORITY", "Job priority 1-10 (default: 5)") do |p|
    priority = p.to_i
  end
  
  parser.on("-r RETRIES", "--retries=RETRIES", "Max retries (default: 3)") do |r|
    max_retries = r.to_i
  end
  
  parser.on("-t TIMEOUT", "--timeout=TIMEOUT", "Timeout in ms (default: 30000)") do |t|
    timeout_ms = t.to_i64
  end
  
  parser.on("submit", "Submit a job") do
    action = "submit"
    parser.banner = "Usage: job-cli submit [options] COMMAND [ARGS...]"
  end
  
  parser.on("query", "Query job status") do
    action = "query"
    parser.banner = "Usage: job-cli query JOB_ID"
  end
  
  parser.on("health", "Check gateway health") do
    action = "health"
  end
  
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit
  end
end

parser.parse

cli = JobGateway::CLI.new(gateway_url)

case action
when "submit"
  if ARGV.size < 1
    puts "Error: No command specified"
    puts "Usage: job-cli submit COMMAND [ARGS...]"
    exit 1
  end
  
  command = ARGV[0]
  args = ARGV[1..-1]
  
  cli.submit(command, args, priority, max_retries, timeout_ms)
when "query"
  if ARGV.size < 1
    puts "Error: No job ID specified"
    puts "Usage: job-cli query JOB_ID"
    exit 1
  end
  
  job_id = ARGV[0]
  cli.query(job_id)
when "health"
  cli.health
else
  puts "Unknown action: #{action}"
  puts "Available actions: submit, query, health"
  exit 1
end