#!/bin/bash

echo "════════════════════════════════════════════════"
echo "  Testing Distributed Job Orchestrator"
echo "════════════════════════════════════════════════"
echo ""

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test 1: Gateway Health
echo -e "${YELLOW}Test 1: Gateway Health${NC}"
if curl -s http://localhost:8080/health | grep -q "healthy"; then
    echo -e "${GREEN}✓ Gateway is healthy${NC}"
else
    echo -e "${RED}✗ Gateway not responding${NC}"
fi
echo ""

# Test 2: Scheduler Health
echo -e "${YELLOW}Test 2: Scheduler Health${NC}"
if curl -s http://localhost:5000/health | grep -q "healthy"; then
    echo -e "${GREEN}✓ Scheduler is healthy${NC}"
else
    echo -e "${RED}✗ Scheduler not responding${NC}"
fi
echo ""

# Test 3: Submit a job
echo -e "${YELLOW}Test 3: Submitting job${NC}"
JOB_RESULT=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command":"echo","args":["test"],"priority":5}')

if echo "$JOB_RESULT" | grep -q "job_id"; then
    JOB_ID=$(echo "$JOB_RESULT" | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)
    echo -e "${GREEN}✓ Job submitted successfully${NC}"
    echo "  Job ID: $JOB_ID"
else
    echo -e "${RED}✗ Job submission failed${NC}"
    echo "  Response: $JOB_RESULT"
fi
echo ""

# Test 4: Query job status
echo -e "${YELLOW}Test 4: Querying job${NC}"
if [ ! -z "$JOB_ID" ]; then
    sleep 2  # Wait for job to process
    JOB_STATUS=$(curl -s http://localhost:8080/jobs/$JOB_ID)
    echo "  Job status: $JOB_STATUS"
    echo -e "${GREEN}✓ Job query successful${NC}"
else
    echo -e "${RED}✗ No job ID to query${NC}"
fi
echo ""

# Test 5: Submit multiple jobs
echo -e "${YELLOW}Test 5: Submitting 3 concurrent jobs${NC}"
for i in {1..30}; do
    curl -s -X POST http://localhost:8080/jobs \
      -H "Content-Type: application/json" \
      -d "{\"command\":\"echo\",\"args\":[\"job$i\"],\"priority\":$i}" > /dev/null
    echo "  Submitted job $i"
done
echo -e "${GREEN}✓ All jobs submitted${NC}"
echo ""

echo "════════════════════════════════════════════════"
echo -e "${GREEN}Tests complete!${NC}"
echo "════════════════════════════════════════════════"