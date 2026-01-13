# Lockstep

A Python CLI tool for synchronizing [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/) YAML specifications to Databricks Unity Catalog.

## Features

- **Table Management**: Create tables, add columns, update descriptions
- **Constraint Handling**: Set primary key and NOT NULL constraints (informational)
- **Tag Synchronization**: Create, update, and remove tags on tables and columns
- **Certification**: Manage Unity Catalog certification via `system.certification_status` tag
- **Idempotent Operations**: Safe to run multiple times - only applies necessary changes
- **Dry Run Mode**: Preview changes before applying them
- **Rich CLI Output**: Beautiful, informative terminal output

## Installation

### Using UV Tool (Recommended)

Install as a global CLI tool with an isolated environment:

```bash
# From local directory
uv tool install --python python3.11 /path/to/contract_sync

# From git repository
uv tool install --python python3.11 git+https://github.com/your-org/lockstep.git

# Upgrade to latest version
uv tool upgrade lockstep

# Uninstall
uv tool uninstall lockstep
```

The tool installs to:
- **Isolated environment**: `~/.local/share/uv/tools/lockstep/`
- **Executable symlink**: `~/.local/bin/lockstep`

> **Note:** Ensure `~/.local/bin` is in your `$PATH`

### Using pip

```bash
# Clone and install
git clone https://github.com/your-org/lockstep.git
cd lockstep
pip install .

# Or install in development mode
pip install -e ".[dev]"
```

### Running with UV (without installation)

Run directly from source without installing:

```bash
cd /path/to/contract_sync
uv run lockstep --help
uv run lockstep from-file contracts/ --dry-run
```

### Building Standalone Executable

Build a standalone executable (~34MB) that doesn't require Python:

```bash
# Install PyInstaller
uv pip install pyinstaller

# Build the executable
uv run pyinstaller --onefile --name lockstep --clean src/lockstep/cli/main.py

# Run it
./dist/lockstep --version
```

> **Note:** PyInstaller builds are platform-specific. Build on macOS for macOS, Linux for Linux, etc.

## Authentication

The tool supports multiple authentication methods for both **AWS** and **Azure** Databricks:

### Option 1: OAuth (Interactive - Default)

Uses the Databricks SDK credential chain (Azure CLI, Databricks CLI, environment variables):

```bash
# First, authenticate with Databricks CLI
databricks auth login --host https://your-workspace.databricks.com

# Then run the tool
lockstep from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --oauth
```

### Option 2: Service Principal / OAuth M2M (Recommended for CI/CD)

Works on both AWS and Azure Databricks:

```bash
lockstep from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"
```

Or via environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxx"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

lockstep from-file contracts/
```

### Option 3: Personal Access Token

```bash
lockstep from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --no-oauth \
  --token "your-personal-access-token"
```

Or via environment variable:

```bash
export DATABRICKS_TOKEN="your-personal-access-token"
lockstep from-file contracts/ --no-oauth
```

### Option 4: Config File

Create `~/.lockstep.yaml`:

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
  # Unity Catalog certification is set via the system.certification_status tag
  system.certification_status: "certified"  # or "deprecated"
```

### 2. Validate the Contract

```bash
lockstep validate contracts/customers.yaml
```

### 3. Preview Changes (Dry Run)

```bash
lockstep from-file contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --dry-run
```

### 4. Apply Changes

```bash
lockstep from-file contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx"
```

## CLI Reference

### `lockstep from-file`

Synchronize ODCS contracts to Unity Catalog.

```bash
lockstep from-file PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory containing contract files

**Connection Options:**
- `--host TEXT`: Databricks workspace host URL
- `--sql-endpoint TEXT`: SQL warehouse endpoint path (e.g., `/sql/1.0/warehouses/xxx`)

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
lockstep from-file contracts/ --host "https://..." --sql-endpoint "/sql/..." --oauth --dry-run

# Sync with Service Principal (CI/CD)
lockstep from-file contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"

# Sync with Personal Access Token
lockstep from-file contracts/ --no-oauth --token "dapi..."

# Preview changes (dry run)
lockstep from-file contracts/ --dry-run

# Allow dropping columns
lockstep from-file contracts/ --allow-destructive

# Override catalog for dev environment
lockstep from-file contracts/ --catalog-override dev_catalog

# Preserve extra tags in Unity Catalog
lockstep from-file contracts/ --preserve-extra-tags
```

### `lockstep validate`

Validate ODCS contract files without syncing.

```bash
lockstep validate PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory to validate

**Options:**
- `--verbose, -v`: Show detailed validation output

**Examples:**

```bash
# Validate a single file
lockstep validate contracts/customers.yaml

# Validate all files in a directory
lockstep validate contracts/
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

## Exit Codes

The CLI uses specific exit codes for CI/CD integration:

| Exit Code | Meaning |
|-----------|---------|
| **0** | Success - no changes needed (in sync) |
| **1** | Error - sync failed, connection error, or validation error |
| **2** | Differences detected (dry-run mode only) |

### CI/CD Drift Detection

Use `--dry-run` to detect drift between contracts and Unity Catalog:

```bash
#!/bin/bash
lockstep from-file contracts/ \
  --host "$DATABRICKS_HOST" \
  --sql-endpoint "$DATABRICKS_HTTP_PATH" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --dry-run

exit_code=$?

case $exit_code in
  0)
    echo "✅ Unity Catalog is in sync with contracts"
    ;;
  2)
    echo "⚠️ Drift detected! Unity Catalog differs from contracts"
    exit 1  # Fail the CI pipeline
    ;;
  *)
    echo "❌ Error occurred during sync check"
    exit 1
    ;;
esac
```

### GitHub Actions Example

```yaml
# .github/workflows/data-contracts.yml
name: Data Contract Sync

on:
  push:
    branches: [main]
    paths:
      - 'contracts/**'
  pull_request:
    branches: [main]
    paths:
      - 'contracts/**'
  workflow_dispatch:  # Manual trigger

env:
  PYTHON_VERSION: '3.11'

jobs:
  validate:
    name: Validate Contracts
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          
      - name: Install lockstep
        run: |
          pip install uv
          uv tool install lockstep
          
      - name: Validate contract syntax
        run: |
          lockstep validate contracts/

  drift-check:
    name: Check for Drift
    needs: validate
    runs-on: ubuntu-latest
    outputs:
      has_drift: ${{ steps.check.outputs.has_drift }}
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          
      - name: Install lockstep
        run: |
          pip install uv
          uv tool install lockstep
          
      - name: Check for contract drift
        id: check
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_HTTP_PATH: ${{ secrets.DATABRICKS_HTTP_PATH }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
        run: |
          set +e
          lockstep from-file contracts/ --dry-run
          exit_code=$?
          set -e
          
          if [ $exit_code -eq 0 ]; then
            echo "✅ No drift detected"
            echo "has_drift=false" >> $GITHUB_OUTPUT
          elif [ $exit_code -eq 2 ]; then
            echo "::warning::Contract drift detected - changes will be applied on merge"
            echo "has_drift=true" >> $GITHUB_OUTPUT
          else
            echo "::error::Sync check failed"
            exit 1
          fi

  apply:
    name: Apply Changes
    needs: drift-check
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    runs-on: ubuntu-latest
    environment: production  # Requires approval if configured
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          
      - name: Install lockstep
        run: |
          pip install uv
          uv tool install lockstep
          
      - name: Apply contract changes
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_HTTP_PATH: ${{ secrets.DATABRICKS_HTTP_PATH }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
        run: |
          lockstep from-file contracts/
          
      - name: Summary
        run: |
          echo "### ✅ Data Contracts Applied" >> $GITHUB_STEP_SUMMARY
          echo "Changes have been synchronized to Unity Catalog." >> $GITHUB_STEP_SUMMARY
```

**Repository Secrets Setup:**

Add these secrets in your repository settings (Settings → Secrets and variables → Actions):
- `DATABRICKS_HOST` - Your Databricks workspace URL
- `DATABRICKS_HTTP_PATH` - SQL warehouse endpoint path
- `DATABRICKS_CLIENT_ID` - Service principal client ID
- `DATABRICKS_CLIENT_SECRET` - Service principal secret

**Environment Protection (Optional):**

Create a `production` environment (Settings → Environments) with:
- Required reviewers for deployment approval
- Branch protection rules

### Azure DevOps Pipelines Example

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include:
      - main
  paths:
    include:
      - contracts/*

variables:
  - group: databricks-credentials  # Variable group containing secrets

stages:
  - stage: ValidateContracts
    displayName: 'Validate Data Contracts'
    jobs:
      - job: Validate
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
              
          - script: |
              pip install uv
              uv tool install lockstep
            displayName: 'Install lockstep'
            
          - script: |
              lockstep validate contracts/
            displayName: 'Validate contract syntax'

  - stage: CheckDrift
    displayName: 'Check for Drift'
    dependsOn: ValidateContracts
    jobs:
      - job: DriftCheck
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'
              
          - script: |
              pip install uv
              uv tool install lockstep
            displayName: 'Install lockstep'
            
          - script: |
              lockstep from-file contracts/ --dry-run
              exit_code=$?
              if [ $exit_code -eq 2 ]; then
                echo "##vso[task.logissue type=warning]Contract drift detected"
                exit 1
              elif [ $exit_code -ne 0 ]; then
                echo "##vso[task.logissue type=error]Sync check failed"
                exit 1
              fi
              echo "##vso[task.complete result=Succeeded]No drift detected"
            displayName: 'Check for contract drift'
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_HTTP_PATH: $(DATABRICKS_HTTP_PATH)
              DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
              DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)

  - stage: ApplyChanges
    displayName: 'Apply Contract Changes'
    dependsOn: CheckDrift
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: Deploy
        pool:
          vmImage: 'ubuntu-latest'
        environment: 'production'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - task: UsePythonVersion@0
                  inputs:
                    versionSpec: '3.11'
                    
                - script: |
                    pip install uv
                    uv tool install lockstep
                  displayName: 'Install lockstep'
                  
                - script: |
                    lockstep from-file contracts/
                  displayName: 'Apply contract changes'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_HTTP_PATH: $(DATABRICKS_HTTP_PATH)
                    DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
                    DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)
```

**Variable Group Setup:**

Create a variable group named `databricks-credentials` in Azure DevOps with:
- `DATABRICKS_HOST` - Your Databricks workspace URL
- `DATABRICKS_HTTP_PATH` - SQL warehouse endpoint path
- `DATABRICKS_CLIENT_ID` - Service principal client ID
- `DATABRICKS_CLIENT_SECRET` - Service principal secret (mark as secret)

## Development

### Setup

```bash
# Clone and install
git clone https://github.com/your-org/lockstep.git
cd lockstep
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=lockstep --cov-report=html

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
src/lockstep/
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



