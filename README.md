Distributed Job Orchestrator
============================
A multi-language distributed system for running jobs across a cluster.

QUICK START (4 terminals)
-------------------------
1. Clone and build:
   git clone <repo>
   cd job-orchestrator
   ./scripts/build-all.sh

2. Terminal 1 - Coordinator:
   cd coordinator && rebar3 shell

3. Terminal 2 - Worker:
   cd worker && cargo run -- --id worker-1

4. Terminal 3 - Gateway:
   cd gateway && ./bin/gateway 8080

5. Terminal 4 - Scheduler:
   cd scheduler && source venv/bin/activate && python scheduler.py

CLI USAGE
---------
Submit a job:
  cd gateway && ./bin/job-cli submit echo "Hello World"

Submit with options:
  ./bin/job-cli --priority 8 --retries 5 submit sleep 10

Check health:
  ./bin/job-cli health

Query job status:
  ./bin/job-cli query JOB_ID

ONE-LINER TEST
--------------
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command":"echo","args":["hello"]}'

COMPONENTS
----------
Component      Language    Port    Command
Gateway        Crystal     8080    ./bin/gateway 8080
Coordinator    Erlang      9000    rebar3 shell
Worker         Rust        5001+   cargo run -- --id worker-1
Scheduler      Python      5000    python scheduler.py

REQUIREMENTS
------------
- Rust
- Erlang/OTP
- Crystal
- Python 3
- ~5 minutes setup time

LICENSE
-------
MIT - Educational Purpose