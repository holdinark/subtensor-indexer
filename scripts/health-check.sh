#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Services to check
SERVICES=(
    "clickhouse"
    "shovel_block_timestamp"
    "shovel_extrinsics"
    "shovel_events"
    "shovel_stake_map"
    "shovel_hotkey_owner_map"
    "shovel_subnets"
    "shovel_daily_stake"
    "shovel_daily_balance"
    "shovel_tao_price"
    "shovel_alpha_to_tao"
    "shovel_validators"
)

echo -e "${BLUE}🏥 Health Check - Subtensor Indexer Services${NC}"
echo "=================================================="

# Check if docker compose is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ docker not found${NC}"
    exit 1
fi

# Get service status
all_healthy=true

for service in "${SERVICES[@]}"; do
    status=$(docker compose ps -q "$service" 2>/dev/null)
    
    if [ -z "$status" ]; then
        echo -e "${RED}❌ $service: Not found${NC}"
        all_healthy=false
        continue
    fi
    
    # Check if container is running
    running=$(docker inspect -f '{{.State.Running}}' "$status" 2>/dev/null)
    health=$(docker inspect -f '{{.State.Health.Status}}' "$status" 2>/dev/null)
    
    if [ "$running" = "true" ]; then
        if [ "$health" = "healthy" ]; then
            echo -e "${GREEN}✅ $service: Running & Healthy${NC}"
        elif [ "$health" = "unhealthy" ]; then
            echo -e "${YELLOW}⚠️  $service: Running but Unhealthy${NC}"
            all_healthy=false
        else
            echo -e "${GREEN}✅ $service: Running${NC}"
        fi
    else
        echo -e "${RED}❌ $service: Not running${NC}"
        all_healthy=false
    fi
done

echo "=================================================="

if [ "$all_healthy" = true ]; then
    echo -e "${GREEN}🎉 All services are healthy!${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some services need attention${NC}"
    echo ""
    echo -e "${YELLOW}To restart failed services:${NC}"
    echo "  docker compose up -d"
    echo ""
    echo -e "${YELLOW}To view logs for a specific service:${NC}"
    echo "  docker compose logs -f <service_name>"
    exit 1
fi 