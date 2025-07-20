# End-to-End Business Intelligence Tutorial

A comprehensive tutorial project demonstrating how to build a modern data lakehouse architecture using Apache Iceberg, MinIO, and Python.

## 🏗️ Architecture Overview

This project sets up a complete data lakehouse environment with the following components:

- **MinIO**: S3-compatible object storage for data lake storage
- **Apache Iceberg**: Modern table format for large analytic datasets
- **Iceberg REST Catalog**: RESTful catalog service for managing Iceberg tables
- **PostgreSQL**: Backend database for Iceberg catalog metadata
- **Python Environment**: For data processing and analytics

## 📋 Prerequisites

Before getting started, ensure you have the following installed:

- [Docker](https://docs.docker.com/get-docker/) (20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (2.0+)
- [Python](https://www.python.org/downloads/) (3.12+)
- [Poetry](https://python-poetry.org/docs/#installation) (for Python dependency management)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/jianwei14/e2e_bi_tutorial.git
cd e2e_bi_tutorial
```

### 2. Set Up Environment Variables

Create a `.env` file in the project root with the following configuration:

```bash
# AWS/MinIO Configuration
AWS_ACCESS_KEY_ID=minio_admin
AWS_SECRET_ACCESS_KEY=minio_password
AWS_REGION=us-east-1

# Data Lake Configuration
LAKEHOUSE_NAME=lakehouse
S3_ENDPOINT=http://minio:9000

# PostgreSQL Configuration for Iceberg Catalog
ICEBERG_PG_CATALOG_DB=iceberg_catalog
ICEBERG_PG_CATALOG_USER=iceberg_user
ICEBERG_PG_CATALOG_PASSWORD=iceberg_password
```

### 3. Start the Services

```bash
# Start all services in the background
docker compose up -d

# View logs (optional)
docker compose logs -f
```

### 4. Verify Services are Running

Check that all services are healthy:

```bash
docker compose ps
```

You should see all services in "Up" status.

## 🔧 Service Access Points

Once the services are running, you can access:

| Service | URL | Purpose |
|---------|-----|---------|
| MinIO Console | http://localhost:9001 | Object storage web interface |
| MinIO API | http://localhost:9000 | S3-compatible API endpoint |
| Iceberg REST Catalog | http://localhost:8181 | Iceberg catalog API |
| PostgreSQL | localhost:5433 | Iceberg metadata database |

### MinIO Console Access
- **URL**: http://localhost:9001
- **Username**: `minio_admin` (or your `AWS_ACCESS_KEY_ID`)
- **Password**: `minio_password` (or your `AWS_SECRET_ACCESS_KEY`)

## 📊 Usage Examples

### Setting Up Python Environment

```bash
# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### Working with Iceberg Tables

Here's a basic example of how to work with Iceberg tables in this setup:

```python
from pyiceberg.catalog import load_catalog

# Load the Iceberg catalog
catalog = load_catalog(
    "default",
    **{
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minio_admin",
        "s3.secret-access-key": "minio_password",
    }
)

# List available tables
print(catalog.list_tables())

# Create a namespace
catalog.create_namespace("tutorial")

# Create a table
# (Add your table creation code here)
```

## 🏗️ Architecture Details

### Data Flow

1. **Data Ingestion**: Raw data is stored in MinIO buckets
2. **Table Management**: Iceberg manages table metadata and schema evolution
3. **Catalog Service**: REST catalog provides a centralized metadata store
4. **Metadata Storage**: PostgreSQL stores Iceberg catalog metadata
5. **Data Processing**: Python applications process data using Iceberg APIs

### Network Architecture

All services communicate through a shared Docker network (`shared_network`), enabling:
- Service discovery by container name
- Isolated networking environment
- Simplified configuration

## 🛠️ Development

### Adding Python Dependencies

```bash
# Add a new dependency
poetry add package_name

# Add development dependencies
poetry add --group dev package_name
```

### Modifying Services

To modify service configurations:

1. Edit `docker-compose.yaml`
2. Update environment variables in `.env`
3. Restart services: `docker compose down && docker compose up -d`

## 📝 Common Tasks

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f iceberg-rest
```

### Stopping Services

```bash
# Stop all services
docker compose down

# Stop and remove volumes (⚠️ deletes all data)
docker compose down -v
```

### Restarting a Service

```bash
# Restart specific service
docker compose restart iceberg-rest
```

### Accessing Service Containers

```bash
# Access MinIO container
docker compose exec minio sh

# Access PostgreSQL container
docker compose exec iceberg-pg-catalog psql -U iceberg_user -d iceberg_catalog
```

## 🐛 Troubleshooting

### Common Issues

**Service fails to start:**
- Check if ports are already in use: `lsof -i :9000,9001,8181,5433`
- Verify environment variables in `.env` file
- Check Docker logs: `docker compose logs service_name`

**MinIO connection issues:**
- Ensure MinIO is running: `docker compose ps minio`
- Verify credentials match between services
- Check network connectivity between containers

**Iceberg catalog errors:**
- Ensure PostgreSQL is healthy: `docker compose ps iceberg-pg-catalog`
- Verify database connection parameters
- Check Iceberg REST service logs

### Reset Everything

If you need to start fresh:

```bash
# Stop all services and remove volumes
docker compose down -v

# Remove any orphaned containers
docker system prune

# Start fresh
docker compose up -d
```

## 📚 Next Steps

1. **Data Ingestion**: Set up data pipelines to ingest data into your lakehouse
2. **Schema Evolution**: Explore Iceberg's schema evolution capabilities
3. **Query Engine**: Connect analytical tools (DuckDB, Spark, etc.) to query your data
4. **Monitoring**: Add observability and monitoring to your data pipeline

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🔗 Useful Links

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [MinIO Documentation](https://docs.min.io/)
- [Poetry Documentation](https://python-poetry.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
