# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
MG (Master Global) is a Python package for sports data processing and model management, specifically focused on daily fantasy sports (DFS) operations. The package integrates with PostgreSQL databases, Google Cloud services

## Build & Development Commands

### Installation
```bash
pip install -e .
```

### Package Installation (from source)
```bash
pip install -r requirements.txt
```

No specific build, test, or lint configuration files are present - the project uses standard setuptools build with pyproject.toml.

## Architecture Overview

### Core Components

**Database Management (`db/`)**
- `PostgresManager` class handles multi-database, multi-schema PostgreSQL connections
- `SQLServerManager` class for SQL Server connections
- `SqlETL` class for ETL operations between databases
- Legacy SQL Server connections in `sql_connections.py`
- Supports Digital Ocean PostgreSQL instances
- Configuration defined in `mg.db.config.POSTGRES_HOSTS`

**Model Management (`models/model_manager.py`)**
- `ModelManager` class for ML model serialization, versioning, and storage
- Supports both local filesystem and Google Cloud Storage
- Uses pickle/dill for serialization with automatic fallback
- Production/archive model structure in GCS with automatic versioning

**Google Cloud Integration (`google_cloud/`)**
- `cloud_storage.py`: GCS operations for model and data storage
- `secret_manager.py`: Google Secret Manager integration
- `jobs.py`: Cloud Run job management
- `publish.py`: Pub/Sub messaging


**Data Processing (`etl/`)**
- `Proteus` class for shape-shifting data transformations (dict flattening, snake_case, type conversions)
- `lexis.py` - String normalization and name similarity functions
- `chronos.py` - Date/time parsing and timezone conversion

**Entity Mapping (`etl/hermes/`)**
- `Cartographer` base class for mapping external source IDs to internal entities
- `PlayerCartographer` - Match players by name, team, position with fuzzy matching
- `TeamCartographer` - Match teams by name, abbreviation, location, mascot
- `GameCartographer` - Match games by team IDs, team names, date, and time
- Features: cached lookups, confidence ratings (0-100), automatic persistence to database

**Source Data Models (`db/hermes/`)**
- `SourceEntity` base dataclass with source, source_id, timestamps
- `SourcePlayer` - Standardized player data from external sources
- `SourceTeam` - Standardized team data from external sources
- `SourceGame` - Standardized game data from external sources
- Auto-normalization in `__post_init__`, `to_dict()` for database insertion

### Key Configuration

**Database Connections**
- Database configuration in `mg.db.config.POSTGRES_HOSTS`
- Credentials loaded from environment variables:
  - `DO_HOST` - Digital Ocean PostgreSQL host
  - `DO_PASSWORD` - Digital Ocean PostgreSQL password
  - `DO_USER` (optional, defaults to "doadmin")
  - `DO_PORT` (optional, defaults to "25060")
- Each host can have multiple databases and schemas

**File System Paths**
- Platform-specific paths defined in `settings.py`
- Windows: `C:/Users/gabri/Downloads`, `C:/GG/gglib/data`
- Mac/Linux: `/Users/gabe/Downloads`, `/Users/gabe/GG/gglib/data`

## Important Notes

**Security Considerations**
- Database credentials loaded from environment variables (see Database Connections above)
- GCP service account credentials should be configured via `MG_GCP_CREDENTIALS` environment variable

**Version Management**
- Current version: 1.9.4 (defined in `pyproject.toml` and `__init__.py`)
- Models stored with automatic versioning and cleanup (max 5 versions by default)

**Database Schema Export**
- `mg.utils.schema_exporter.PostgreSQLSchemaExporter` - Export database schemas and sample data
- Exports DDL for tables (with triggers), views (with queries), stored procedures/functions (with logic), and triggers (with function definitions)
- Organizes output by schema in `sql_infrastructure_*` folders
- Supports both CSV and JSON sample data export

**Dependencies**
- Requires Python >=3.11
- Heavy focus on data processing: pandas, SQLAlchemy, psycopg2
- Google Cloud: google-cloud-storage, google-cloud-secret-manager

## Common Workflows

**Model Operations**
```python
from mg.models.model_manager import ModelManager
mm = ModelManager()
mm.save_model_to_gcs(model, "model_name", results=metrics, sport="cfb")
loaded = mm.load_model_from_gcs("model_name", sport="cfb")
```

**Database Operations**
```python
from mg.db.postgres_manager import PostgresManager
pgm = PostgresManager("digital_ocean", "defaultdb", "control")
results = pgm.execute("SELECT * FROM table")
```

**Schema Export Operations**
```python
from mg.utils.schema_exporter import PostgreSQLSchemaExporter

# Export all schemas from a database
exporter = PostgreSQLSchemaExporter(
    host_key='digital_ocean',
    database_key='cfb', 
    schema_key='core',
    output_base_path='sql_infrastructure_cfb',
    sample_rows=100
)

# Export all schemas in the database
exporter.export_all()

# Or export specific schemas only
exporter.export_all(specific_schemas=['core', 'draftkings', 'fanduel'])
```