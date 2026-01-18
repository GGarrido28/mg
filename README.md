## Master Global (MG) Library

A Python package for sports data processing and model management, specifically focused on daily fantasy sports (DFS) operations. The package integrates with PostgreSQL databases and Google Cloud services for data storage, model versioning, and automated workflows.

## Installation

```bash
pip install -e .
```

## Configuration

This library uses environment variables for all sensitive configuration. Set the following variables before use:

### Database Credentials
| Variable | Description | Required |
|----------|-------------|----------|
| `MG_DO_HOST` | Digital Ocean PostgreSQL host | Yes |
| `MG_DO_PASSWORD` | Digital Ocean PostgreSQL password | Yes |
| `MG_DO_USER` | PostgreSQL username (default: `doadmin`) | No |
| `MG_DO_PORT` | PostgreSQL port (default: `25060`) | No |
| `MG_SS_HOST` | SQL Server host | For SQL Server only |
| `MG_SS_USER` | SQL Server username | For SQL Server only |
| `MG_SS_PASSWORD` | SQL Server password | For SQL Server only |

### Email Alerts
| Variable | Description |
|----------|-------------|
| `MG_EMAIL_SENDER` | Gmail sender address |
| `MG_EMAIL_RECEIVER` | Email recipient address |
| `MG_EMAIL_APP_PASSWORD` | Gmail app password |

### Google Cloud
| Variable | Description |
|----------|-------------|
| `MG_GCP_PROJECT_NUMBER` | GCP project number for Secret Manager |
| `MG_GCP_CREDENTIALS` | Path to GCP service account JSON |

---

## Package Structure

### mg/alerts
Email alerting and notification system for monitoring automated processes.

**Key Components:**
- `notification.py` - Send email alerts with optional attachments via Gmail SMTP
- `alert_manager.py` - Manage alert rules and thresholds for automated monitoring
- `stale_checks.py` - Monitor data freshness and trigger alerts for stale data
- `config.py` - Email credentials configuration (env vars)
- `constants.py` - Static constants (MAC SSH host)

**Usage:**
```python
from mg.alerts.notification import send_email_alert
send_email_alert("Subject", "Message body")
```

---

### mg/etl
Data transformation, validation, and entity matching utilities for normalizing data from multiple sources.

**Key Components:**
- `proteus.py` - `Proteus` class for shape-shifting data transformations (dict flattening, snake_case, type conversions, validation)
- `lexis.py` - String normalization and name similarity functions for entity matching
- `chronos.py` - Date/time parsing and timezone conversion utilities
- `export_data_scrape.py` - Export scraped data to various formats

**Usage:**
```python
from mg.etl.proteus import Proteus
p = Proteus()
data = p.sql_friendly_columns(data)  # Normalize column names for SQL
data = p.unnest_dict(nested_data)    # Flatten nested dictionaries
```

---

### mg/etl/hermes
Entity mapping system (Cartographers) for linking external source IDs to internal master entities. Named after the Greek messenger god who served as a guide between worlds.

**Key Components:**
- `base.py` - `Cartographer` base class with caching, database persistence, and logging
- `player.py` - `PlayerCartographer` for mapping players by name, team, position with fuzzy matching
- `team.py` - `TeamCartographer` for mapping teams by name, abbreviation, location, mascot
- `game.py` - `GameCartographer` for mapping games by team IDs, team names, date, and time

**Features:**
- Cached source_id lookups for fast repeated queries
- Confidence ratings (0-100) for match quality tracking
- Fuzzy name matching with configurable thresholds
- Automatic mapping persistence to database
- LoggerManager integration for structured logging

### mg/db
Multi-database connection management supporting PostgreSQL and SQL Server with connection pooling and query execution.

**Key Components:**
- `postgres_manager.py` - `PostgresManager` class for PostgreSQL operations with multi-schema support
- `sql_server_manager.py` - `SQLServerManager` class for SQL Server connections
- `sql_etl.py` - `SqlETL` class for ETL operations between databases
- `postgres_user.py` - Admin utilities for PostgreSQL user/privilege management
- `queries.py` - Reusable SQL query templates
- `config.py` - Database credentials and host configuration (env vars)

**Usage:**
```python
from mg.db.postgres_manager import PostgresManager
pgm = PostgresManager("digital_ocean", "defaultdb", "control")
results = pgm.execute("SELECT * FROM table")
pgm.insert_rows("table_name", columns, data, update=True)  # Upsert
```

---

### mg/db/hermes
Standardized source data models for ETL pipelines. These dataclasses normalize incoming data from external sources before mapping to internal entities via the Cartographer classes.

**Key Components:**
- `base.py` - `SourceEntity` base class with source, source_id, timestamps, and raw_data storage
- `player.py` - `SourcePlayer` for player data (name fields, team, position, physical attributes, status)
- `team.py` - `SourceTeam` for team data (name, abbreviation, location, mascot, league, colors)
- `game.py` - `SourceGame` for game data (teams, timing, scores, venue, weather, broadcast)

**Features:**
- Automatic data normalization in `__post_init__` (whitespace trimming, case normalization)
- Separation of source fields (source_team) from universal fields (team)
- UUID generation for internal tracking
- `to_dict()` method for database insertion

**Usage:**
```python
from mg.db.hermes import SourcePlayer, SourceTeam, SourceGame

# Create standardized player from scraped data
player = SourcePlayer(
    source="draftkings",
    source_id="dk_12345",
    full_name="Patrick Mahomes",
    source_team="KC",
    position="QB",
    jersey_number=15,
)

# Create standardized game
game = SourceGame(
    source="espn",
    source_id="espn_401234",
    source_away_team="DAL",
    source_home_team="PHI",
    start_time=datetime(2024, 1, 15, 20, 0),
    season=2024,
    week=15,
)

# Convert to dict for database insertion
player_dict = player.to_dict()
```

---

### mg/google_cloud
Google Cloud Platform integrations for storage, messaging, and serverless job execution.

**Key Components:**
- `cloud_storage.py` - GCS operations: upload, download, list, and manage objects in buckets
- `secret_manager.py` - Retrieve secrets from Google Secret Manager
- `publish.py` - `PubSub` class for publishing messages to Google Pub/Sub topics
- `jobs.py` - `CloudRunJobRunner` for managing and executing Cloud Run jobs
- `config.py` - GCP project configuration (env vars)
- `constants.py` - Bucket names and credential mappings

**Usage:**
```python
from mg.google_cloud.cloud_storage import upload_file, download_file
upload_file(bucket_name, local_path, remote_path)

from mg.google_cloud.publish import publish_message
publish_message({"key": "value"}, "topic-name", project_id="my-project")
```

---

### mg/logging
Centralized logging and process tracking with database persistence and email alerting.

**Key Components:**
- `logger_manager.py` - `LoggerManager` class providing:
  - Multi-level logging (info, warning, error, debug)
  - Automatic process tracking in database
  - Email alerts on errors
  - Performance timing and system resource monitoring
  - Decorator utilities for function timing and argument logging

**Usage:**
```python
from mg.logging.logger_manager import LoggerManager
logger = LoggerManager("script_name", __file__, sport="nfl", database="nfl")
logger.start_timer()
logger.log("info", "Processing started")
logger.log("error", "Something failed", send_alert=True)  # Sends email
logger.end_timer()  # Logs to database
```

---

### mg/mac
macOS-specific utilities for cron job deployment and remote script execution.

**Key Components:**
- `cron_manager.py` - Deploy and manage cron jobs on remote Mac machines via SSH, with environment variable injection

**Usage:**
```python
from mg.mac.cron_manager import CronManager
cm = CronManager()
cm.deploy_cron_job("0 * * * *", "/path/to/script.py")
```

---

### mg/models
Machine learning model serialization, versioning, and cloud storage management.

**Key Components:**
- `model_manager.py` - `ModelManager` class for:
  - Saving/loading models to local filesystem or GCS
  - Automatic versioning with configurable retention
  - Support for pickle and dill serialization
  - Model metadata and results tracking

**Usage:**
```python
from mg.models.model_manager import ModelManager
mm = ModelManager()
mm.save_model_to_gcs(model, "model_name", results=metrics, sport="cfb")
loaded_model = mm.load_model_from_gcs("model_name", sport="cfb")
```

---

### mg/process_manager
Process orchestration and execution management for running automated data pipelines.

**Key Components:**
- `process_manager.py` - Coordinate multiple processes and manage dependencies
- `process_runner.py` - Execute individual processes with logging and error handling

---

### mg/scraper_tools
Web scraping utilities and HTTP session management.

**Key Components:**
- `http_handler.py` - HTTP client with session management, cookie handling, and token refresh capabilities

---

### mg/utils
General-purpose utility functions used across the package.

**Key Components:**
- `settings.py` - Platform-specific file paths and date constants
- `schema_exporter.py` - `PostgreSQLSchemaExporter` for exporting database schemas (DDL, views, functions) with sample data
- `project_folder.py` - CLI tool for printing project directory structure in tree format

**Usage:**
```python
from mg.utils.schema_exporter import PostgreSQLSchemaExporter
exporter = PostgreSQLSchemaExporter("digital_ocean", "cfb", "core")
exporter.export_all()  # Exports DDL, views, functions to sql_infrastructure_* folder
```

---

## Dependencies

- Python >= 3.11
- pandas, SQLAlchemy, psycopg2 (data processing)
- google-cloud-storage, google-cloud-secret-manager (GCP)
- pyodbc (SQL Server)
- dill (model serialization)