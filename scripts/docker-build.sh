#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Available services
SERVICES=(
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

print_usage() {
    echo -e "${BLUE}Usage: $0 [OPTIONS] [SERVICE_NAME...]${NC}"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo "  --all, -a          Build all services"
    echo "  --parallel, -p     Build services in parallel (faster but uses more resources)"
    echo "  --no-cache         Build without using Docker cache"
    echo "  --push             Push images to registry after building"
    echo "  --help, -h         Show this help message"
    echo ""
    echo -e "${YELLOW}Available services:${NC}"
    for service in "${SERVICES[@]}"; do
        echo "  - $service"
    done
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  $0 shovel_events                     # Build single service"
    echo "  $0 shovel_events shovel_extrinsics   # Build multiple services"
    echo "  $0 --all                             # Build all services"
    echo "  $0 --all --parallel                  # Build all services in parallel"
    echo "  $0 --all --no-cache                  # Rebuild all services from scratch"
}

build_service() {
    local service=$1
    local extra_args=$2
    
    echo -e "${BLUE}Building $service...${NC}"
    
    if [[ "$service" == "shovel_validators" ]]; then
        # Special handling for validators service
        docker compose build $extra_args $service
    else
        # Use unified Dockerfile for other services
        docker compose build $extra_args $service
    fi
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully built $service${NC}"
    else
        echo -e "${RED}✗ Failed to build $service${NC}"
        return 1
    fi
}

# Parse command line arguments
PARALLEL=false
NO_CACHE=false
PUSH=false
BUILD_ALL=false
SERVICES_TO_BUILD=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --all|-a)
            BUILD_ALL=true
            shift
            ;;
        --parallel|-p)
            PARALLEL=true
            shift
            ;;
        --no-cache)
            NO_CACHE=true
            shift
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        -*)
            echo -e "${RED}Unknown option $1${NC}"
            print_usage
            exit 1
            ;;
        *)
            # Check if it's a valid service name
            if [[ " ${SERVICES[@]} " =~ " $1 " ]]; then
                SERVICES_TO_BUILD+=("$1")
            else
                echo -e "${RED}Unknown service: $1${NC}"
                echo -e "${YELLOW}Available services:${NC}"
                printf '%s\n' "${SERVICES[@]}"
                exit 1
            fi
            shift
            ;;
    esac
done

# Determine which services to build
if [ "$BUILD_ALL" = true ]; then
    SERVICES_TO_BUILD=("${SERVICES[@]}")
elif [ ${#SERVICES_TO_BUILD[@]} -eq 0 ]; then
    echo -e "${RED}No services specified. Use --all to build all services or specify service names.${NC}"
    print_usage
    exit 1
fi

# Prepare extra Docker arguments
EXTRA_ARGS=""
if [ "$NO_CACHE" = true ]; then
    EXTRA_ARGS="--no-cache"
fi

echo -e "${BLUE}Building services: ${SERVICES_TO_BUILD[*]}${NC}"

# Build base image first for better caching
echo -e "${BLUE}Building base image for better layer caching...${NC}"
docker build -t subtensor-indexer-base:latest \
    --target base \
    -f scraper_service/Dockerfile \
    scraper_service/ $EXTRA_ARGS

# Build services
if [ "$PARALLEL" = true ]; then
    echo -e "${YELLOW}Building services in parallel...${NC}"
    for service in "${SERVICES_TO_BUILD[@]}"; do
        build_service "$service" "$EXTRA_ARGS" &
    done
    wait
else
    echo -e "${YELLOW}Building services sequentially...${NC}"
    for service in "${SERVICES_TO_BUILD[@]}"; do
        build_service "$service" "$EXTRA_ARGS"
    done
fi

# Push images if requested
if [ "$PUSH" = true ]; then
    echo -e "${BLUE}Pushing images to registry...${NC}"
    for service in "${SERVICES_TO_BUILD[@]}"; do
        echo -e "${BLUE}Pushing $service...${NC}"
        docker compose push "$service"
    done
fi

echo -e "${GREEN}✓ Build completed!${NC}" 