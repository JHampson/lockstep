# ODCS Sync

A production-grade Python CLI tool for synchronizing [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/) YAML specifications to Databricks Unity Catalog.

## Features

- **Table Management**: Create tables, add columns, update descriptions
- **Constraint Handling**: Set primary key and NOT NULL constraints (informational)
- **Tag Synchronization**: Create, update, and remove tags on tables and columns
- **Certification**: Manage Unity Catalog certification status (`certified`, `deprecated`)
- **Idempotent Operations**: Safe to run multiple times - only applies necessary changes
- **Dry Run Mode**: Preview changes before applying them
- **Rich CLI Output**: Beautiful, informative terminal output

## Installation

### Using uv (recommended)

```bash
# Clone the repository
git clone https://github.com/your-org/odcs-sync.git
cd odcs-sync

# Install with uv
uv pip install -e ".[dev]"
```

### Using pip

```bash
pip install -e ".[dev]"
```

## Authentication

The tool supports multiple authentication methods for both **AWS** and **Azure** Databricks:

### Option 1: OAuth (Interactive - Default)

Uses the Databricks SDK credential chain (Azure CLI, Databricks CLI, environment variables):

```bash
# First, authenticate with Databricks CLI
databricks auth login --host https://your-workspace.databricks.com

# Then run the tool
odcs-sync from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx" \
  --oauth
```

### Option 2: Service Principal / OAuth M2M (Recommended for CI/CD)

Works on both AWS and Azure Databricks:

```bash
odcs-sync from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"
```

Or via environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxx"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

odcs-sync from-file contracts/
```

### Option 3: Personal Access Token

```bash
odcs-sync from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx" \
  --no-oauth \
  --token "your-personal-access-token"
```

Or via environment variable:

```bash
export DATABRICKS_TOKEN="your-personal-access-token"
odcs-sync from-file contracts/ --no-oauth
```

### Option 4: Config File

Create `~/.odcs_sync.yaml`:

```yaml
host: "https://your-workspace.databricks.com"
http_path: "/sql/1.0/warehouses/xxx"

# Choose one authentication method:

# Service Principal (recommended for automation)
client_id: "your-client-id"
client_secret: "your-client-secret"

# Or Personal Access Token
# token: "your-token"
# use_oauth: false
```

## Quick Start

### 1. Create an ODCS Contract

Create a file `contracts/customers.yaml`:

```yaml
apiVersion: v3.0.0
kind: DataContract
name: customer_contract
version: "1.0.0"
status: active
description: "Customer master data for the sales domain"

dataset:
  catalog: main
  schema: sales
  table: customers

schema:
  properties:
    - name: customer_id
      logicalType: string
      description: "Unique customer identifier (UUID)"
      required: true
      primaryKey: true
      tags:
        pii: "false"
    
    - name: email
      logicalType: string
      description: "Customer email address"
      required: true
      tags:
        pii: "true"
        classification: "sensitive"
    
    - name: full_name
      logicalType: string
      description: "Customer full name"
      required: false
      tags:
        pii: "true"
    
    - name: created_at
      logicalType: timestamp
      description: "Account creation timestamp"
      required: false
    
    - name: total_orders
      logicalType: integer
      description: "Total lifetime orders"
      required: false

tags:
  domain: "sales"
  team: "customer-success"
  data_product: "customer-360"

certification: certified
```

### 2. Validate the Contract

```bash
odcs-sync validate contracts/customers.yaml
```

### 3. Preview Changes (Dry Run)

```bash
odcs-sync from-file contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx" \
  --dry-run
```

### 4. Apply Changes

```bash
odcs-sync from-file contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx"
```

## CLI Reference

### `odcs-sync from-file`

Synchronize ODCS contracts to Unity Catalog.

```bash
odcs-sync from-file PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory containing contract files

**Connection Options:**
- `--host TEXT`: Databricks workspace host URL
- `--http-path TEXT`: HTTP path for SQL warehouse

**Authentication Options:**
- `--oauth/--no-oauth`: Use OAuth authentication (default: enabled)
- `--client-id TEXT`: OAuth client ID for service principal / M2M auth
- `--client-secret TEXT`: OAuth client secret for service principal / M2M auth
- `--token TEXT`: Personal Access Token (use with `--no-oauth`)

**Sync Options:**
- `--dry-run, -n`: Show planned changes without executing
- `--allow-destructive`: Allow destructive operations (drop columns, remove tags)
- `--preserve-extra-tags`: Don't remove tags that exist in catalog but not in contract
- `--catalog-override TEXT`: Override catalog name from contracts
- `--schema-override TEXT`: Override schema name from contracts
- `--table-prefix TEXT`: Prefix to add to table names

**Output Options:**
- `--verbose, -v`: Enable verbose output
- `--quiet, -q`: Suppress non-error output

**Examples:**

```bash
# Sync with OAuth (interactive)
odcs-sync from-file contracts/ --host "https://..." --http-path "/sql/..." --oauth --dry-run

# Sync with Service Principal (CI/CD)
odcs-sync from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --http-path "/sql/1.0/warehouses/xxx" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"

# Sync with Personal Access Token
odcs-sync from-file contracts/ --no-oauth --token "dapi..."

# Preview changes (dry run)
odcs-sync from-file contracts/ --dry-run

# Allow dropping columns
odcs-sync from-file contracts/ --allow-destructive

# Override catalog for dev environment
odcs-sync from-file contracts/ --catalog-override dev_catalog

# Preserve extra tags in Unity Catalog
odcs-sync from-file contracts/ --preserve-extra-tags
```

### `odcs-sync validate`

Validate ODCS contract files without syncing.

```bash
odcs-sync validate PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory to validate

**Options:**
- `--verbose, -v`: Show detailed validation output

**Examples:**

```bash
# Validate a single file
odcs-sync validate contracts/customers.yaml

# Validate all files in a directory
odcs-sync validate contracts/
```

## ODCS Contract Schema

The tool supports a subset of the ODCS specification focused on Unity Catalog synchronization:

### Contract Metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `apiVersion` | string | No | ODCS spec version (default: v3.0.0) |
| `kind` | string | No | Resource kind (default: DataContract) |
| `name` | string | Yes | Contract name |
| `version` | string | No | Contract version (default: 1.0.0) |
| `status` | enum | No | draft, active, deprecated, retired |
| `description` | string | No | Contract description |

### Dataset (Target Table)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `catalog` | string | Yes | Unity Catalog name |
| `schema` | string | Yes | Schema/database name |
| `table` | string | Yes | Table name |

### Schema (Columns)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Column name |
| `logicalType` | string | Yes | Data type (string, integer, timestamp, etc.) |
| `physicalType` | string | No | Override Databricks type |
| `description` | string | No | Column description |
| `required` | boolean | No | NOT NULL constraint |
| `primaryKey` | boolean | No | Primary key column |
| `tags` | object | No | Column-level tags (key-value) |

### Supported Logical Types

| Logical Type | Databricks Type |
|--------------|-----------------|
| string | STRING |
| integer | INT |
| long | BIGINT |
| float | FLOAT |
| double | DOUBLE |
| decimal | DECIMAL |
| boolean | BOOLEAN |
| date | DATE |
| timestamp | TIMESTAMP |
| timestamp_ntz | TIMESTAMP_NTZ |
| binary | BINARY |

### Table-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `tags` | object | Key-value tags for the table |
| `certification` | enum | certified, deprecated, not_certified |

## Sync Behavior

### Idempotent Operations

The tool is designed to be idempotent - running the same contract multiple times will only apply changes on the first run. Subsequent runs will detect no changes needed.

### Default Behavior (Safe Mode)

By default, the tool will:
- ✅ Create tables if they don't exist
- ✅ Add missing columns
- ✅ Update descriptions
- ✅ Add missing tags
- ✅ Update tag values
- ✅ Set constraints (PK, NOT NULL)
- ✅ Set certification status
- ❌ NOT drop columns (requires `--allow-destructive`)
- ❌ NOT remove tags (requires `--allow-destructive` or omit `--preserve-extra-tags`)

### Destructive Mode

With `--allow-destructive`:
- ✅ Drop columns not in contract
- ✅ Remove tags not in contract
- ✅ Drop constraints not in contract

### Tag Preservation

With `--preserve-extra-tags`:
- Tags in Unity Catalog but not in contract will be kept

## Development

### Setup

```bash
# Clone and install
git clone https://github.com/your-org/odcs-sync.git
cd odcs-sync
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=odcs_sync --cov-report=html

# Run specific test file
pytest tests/test_models.py -v
```

### Linting and Formatting

```bash
# Format code
ruff format src tests

# Lint code
ruff check src tests

# Type checking
mypy src
```

### Using Make

```bash
make test      # Run tests
make lint      # Run linting
make format    # Format code
make typecheck # Run mypy
make all       # Run all checks
```

## Architecture

```
src/odcs_sync/
├── __init__.py
├── models/           # Pydantic models
│   ├── contract.py   # ODCS contract models
│   └── catalog_state.py  # Unity Catalog state models
├── databricks/       # Databricks connectivity
│   ├── config.py     # Configuration handling
│   └── connector.py  # SQL connector
├── services/         # Business logic
│   ├── contract_loader.py  # YAML loading/validation
│   ├── introspection.py    # Catalog state fetching
│   ├── diff.py            # Contract vs catalog diffing
│   ├── sql_generator.py   # SQL generation
│   └── sync.py            # Orchestration
└── cli/              # CLI interface
    ├── main.py       # Typer commands
    └── formatters.py # Output formatting
```

## Extending

The tool is designed to be extensible:

- **Additional ODCS Fields**: Pydantic models use `extra="allow"` for forward compatibility
- **New Action Types**: Add to `ActionType` enum and implement in `DiffService`
- **Custom SQL**: Extend `SQLGenerator` for additional operations
- **New Commands**: Add Typer commands in `cli/main.py`

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please read CONTRIBUTING.md for guidelines.

