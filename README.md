# Subtensor Indexer

A high-performance indexer for the Subtensor blockchain with fast querying capabilities.

## 🚀 Quick Start

Get up and running in development mode with live code reloading:

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your settings

# 2. Start in development mode (with live reloading)
make run
```

That's it! Your services will start with automatic code reloading - edit any Python file and see changes instantly.

## 📋 Available Commands

Run `make help` to see all available commands:

### 🏗️ Building

- `make build` - Build all services
- `make build-fast` - Build all services in parallel
- `make build-clean` - Clean build (no cache)

### 🚀 Running

- `make run` - **Quick start in development mode** (with live reloading)
- `make up` - Start all services
- `make down` - Stop all services
- `make restart` - Restart all services

### 🔧 Development

- `make dev-mode` - Start with live code reloading (changes reflected instantly)
- `make dev-build` - Build services in parallel
- `make dev-up` - Start development environment

### 📊 Monitoring

- `make logs` - View logs from all services
- `make health` - Check health of all services

### 🧹 Maintenance

- `make clean` - Stop services and remove containers
- `make prune` - Clean up Docker system

### Examples

```bash
# Build specific service
make build SERVICE=shovel_events

# View logs for specific service
make logs SERVICE=shovel_events

# Restart specific service
docker compose restart shovel_events
```

## 🏗️ Architecture

This project uses [ClickHouse](https://clickhouse.com/docs/en/intro) as the database for fast time-series data storage and querying.

### Core Components

1. **ClickHouse Database** - Fast analytical database for blockchain data
2. **Shovels** - Python services that scrape and index specific types of blockchain data
3. **Shared Libraries** - Common functionality for database connections, substrate interaction, etc.

### How Shovels Work

Each shovel is responsible for:

- Scraping a specific class of blockchain data
- Processing it block by block
- Storing it efficiently in ClickHouse
- Maintaining checkpoints for reliability

The `ShovelBaseClass` handles:

- ✅ Walking through historical blocks
- ✅ Keeping up with new finalized blocks
- ✅ Checkpointing progress
- ✅ Batch inserting data for performance
- ✅ Error handling and retries

## 🔧 Development

### Development Mode Features

- **🔄 Live Code Reloading** - Edit Python files and see changes instantly
- **📁 Volume Mounting** - Your local code is mounted into containers
- **⚡ Fast Iteration** - No rebuilding needed for code changes
- **🐛 Easy Debugging** - Direct file editing with immediate feedback

### Creating a New Shovel

1. **Create the service directory:**

```bash
mkdir scraper_service/shovel_my_data
cd scraper_service/shovel_my_data
```

2. **Create your shovel class:**

```python
# main.py
from shared.base import ShovelBaseClass

class MyDataShovel(ShovelBaseClass):
    def process_block(self, block_number):
        """Process a single block - implement your logic here"""
        # 1. Scrape data from blockchain using substrate client
        substrate = self.get_substrate_client()
        block_data = substrate.get_block(block_number)

        # 2. Transform the data
        processed_data = self.transform_data(block_data)

        # 3. Store in ClickHouse using batch insert
        self.buffer_insert('my_table', processed_data)

    def transform_data(self, block_data):
        """Transform raw blockchain data into your desired format"""
        return [{"block": block_data.number, "data": "..."}]

def main():
    MyDataShovel(name="my_data").start()

if __name__ == "__main__":
    main()
```

3. **Add to docker-compose.yml:**

```yaml
shovel_my_data:
  <<: *shovel-service
  build:
    context: ./scraper_service
    dockerfile: ./Dockerfile
    args:
      SERVICE_NAME: shovel_my_data
  container_name: shovel_my_data
```

4. **Add to docker-compose.override.yml** (for development):

```yaml
shovel_my_data:
  volumes:
    - ./scraper_service/shared:/app/shared
    - ./scraper_service/shovel_my_data:/app/shovel_my_data
```

5. **Start your new shovel:**

```bash
make run
# or start just your service:
docker compose up -d shovel_my_data
```

### Interacting with Substrate

```python
from shared.substrate import get_substrate_client

# Get singleton substrate client
substrate = get_substrate_client()

# Use it for blockchain queries
block = substrate.get_block(block_number)
events = substrate.get_events(block_number)
```

### Interacting with ClickHouse

**Always use batch inserts for performance:**

```python
from shared.clickhouse.batch_insert import buffer_insert

# Buffer data for batch insertion (much faster)
rows = [
    {"block_number": 1000, "data": "value1"},
    {"block_number": 1001, "data": "value2"},
]
buffer_insert('my_table', rows)

# The ShovelBaseClass automatically flushes buffers periodically
```

## 🐳 Docker Architecture

### Centralized Dockerfile

All services now use a single, parameterized Dockerfile at `scraper_service/Dockerfile` with:

- Optimized layer caching
- Shared dependency installation
- Service-specific code copying via build args
- Environment variable support for runtime

### Development vs Production

- **Development**: Uses `docker-compose.override.yml` for volume mounting
- **Production**: Uses copied code in containers for better performance

## 📁 Project Structure

```
subtensor-indexer/
├── docker-compose.yml          # Main service definitions
├── docker-compose.override.yml # Development overrides (volume mounting)
├── Makefile                    # Easy development commands
├── .env                        # Environment configuration
├── scraper_service/
│   ├── Dockerfile              # Centralized container definition
│   ├── Dockerfile.validators   # Special Dockerfile for Rust services
│   ├── .dockerignore          # Docker build optimization
│   ├── requirements.txt        # Python dependencies
│   ├── shared/                 # Common libraries
│   │   ├── base.py            # ShovelBaseClass
│   │   ├── substrate.py       # Blockchain interaction
│   │   └── clickhouse/        # Database utilities
│   ├── shovel_events/         # Event indexing service
│   ├── shovel_extrinsics/     # Extrinsic indexing service
│   ├── shovel_validators/     # Validator data service
│   └── ...                    # Other shovels
└── scripts/                   # Build and utility scripts
```

## 🔧 Configuration

Create your `.env` file:

```bash
cp .env.example .env
```

Key configuration options:

- Database connection settings
- Substrate node endpoints
- Service-specific parameters

## 🚨 Troubleshooting

### Services can't find main.py

**Fixed!** The centralized Dockerfile now properly handles service names as environment variables.

### Rust Bindings Issues

If you encounter Rust compilation issues with the validators service:

1. The project uses a special `Dockerfile.validators` for Rust requirements
2. Check the validators service logs for specific error messages

### Code Changes Not Reflected

Make sure you're running in development mode:

```bash
make run  # This uses development mode with volume mounting
```

If still not working:

```bash
# Restart specific service
docker compose restart shovel_my_service
```

### Performance Issues

- Use `buffer_insert()` instead of direct database inserts
- Check ClickHouse query performance with `EXPLAIN` statements
- Monitor container resources with `docker stats`

## 🎯 Production Deployment

For production, disable the development override:

```bash
# Build production images
make build

# Start without development volumes
docker compose -f docker-compose.yml up -d
```

## 📊 Monitoring

- View all logs: `make logs`
- Check service health: `make health`
- Monitor specific service: `make logs SERVICE=shovel_events`
- ClickHouse interface: http://localhost:8123

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes using development mode: `make run`
4. Test your changes work correctly
5. Submit a pull request

## 📝 TODO

- [ ] Implement proper logging framework
- [ ] Add observability and alerting
- [ ] Performance monitoring dashboard
- [ ] Automated testing pipeline
- [ ] Production deployment guides

## 🛠️ Dependencies

- Docker & Docker Compose
- Python 3.12+ (in containers)
- ClickHouse (containerized)

No local Python installation required - everything runs in containers!
