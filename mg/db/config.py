import os

# Digital Ocean PostgreSQL credentials from environment variables
_DO_HOST = os.getenv("DO_HOST")
_DO_PASSWORD = os.getenv("DO_PASSWORD")
_DO_USER = os.getenv("DO_USER", "doadmin")
_DO_PORT = os.getenv("DO_PORT", "25060")

# SQL Server credentials from environment variables
_SS_HOST = os.getenv("SS_HOST")
_SS_PASSWORD = os.getenv("SS_PASSWORD")
_SS_USER = os.getenv("SS_USER")

# Database to schema mapping
DB_SCHEMAS = {
    "cfb": ["core", "draftkings", "underdog"],
    "defaultdb": ["control", "data", "draftkings", "fanduel", "underdog"],
    "golf": ["core", "draftkings"],
    "mlb": ["core", "draftkings", "fanduel"],
    "mma": ["core", "draftkings"],
    "nfl": ["core", "draftkings", "underdog"],
    "nhl": ["core", "draftkings"],
}

POSTGRES_HOSTS = {
    "digital_ocean": {
        "defaultdb": {
            "control": {
                "database": "defaultdb",
                "schema": "control",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "data": {
                "database": "defaultdb",
                "schema": "control",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "defaultdb",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "fanduel": {
                "database": "defaultdb",
                "schema": "fanduel",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "underdog": {
                "database": "defaultdb",
                "schema": "underdog",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "cfb": {
            "core": {
                "database": "cfb",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "cfb",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "fanduel": {
                "database": "cfb",
                "schema": "fanduel",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "underdog": {
                "database": "cfb",
                "schema": "underdog",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "nfl": {
            "core": {
                "database": "nfl",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "nfl",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "fanduel": {
                "database": "nfl",
                "schema": "fanduel",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "underdog": {
                "database": "nfl",
                "schema": "underdog",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "mlb": {
            "core": {
                "database": "mlb",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "mlb",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "fanduel": {
                "database": "mlb",
                "schema": "fanduel",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "nhl": {
            "core": {
                "database": "nhl",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "nhl",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "fanduel": {
                "database": "nhl",
                "schema": "fanduel",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "golf": {
            "core": {
                "database": "golf",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "golf",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
        "mma": {
            "core": {
                "database": "mma",
                "schema": "core",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
            "draftkings": {
                "database": "mma",
                "schema": "draftkings",
                "host": _DO_HOST,
                "user": _DO_USER,
                "password": _DO_PASSWORD,
                "port": _DO_PORT,
            },
        },
    },
}
