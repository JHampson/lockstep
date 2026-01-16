# Lockstep

A Python CLI tool for synchronizing [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/) YAML specifications to Databricks Unity Catalog.

## Features

- **Table Management**: Create tables, add columns, update descriptions
- **Constraint Handling**: Set primary key and NOT NULL constraints (informational)
- **Tag Synchronization**: Create, update, and remove tags on tables and columns
- **Certification**: Manage Unity Catalog certification via `system.certification_status` tag
- **Idempotent Operations**: Safe to run multiple times - only applies necessary changes
- **Plan & Apply Workflow**: Preview changes with `plan`, apply with `apply` (similar to Terraform)
- **Granular Control**: Fine-grained `--add-*` and `--remove-*` options for selective sync
- **Multi-Workspace Support**: Apply a single contract to multiple Databricks workspaces
- **JUnit XML Reports**: Generate CI/CD-compatible test reports for validation and drift detection
- **Rich CLI Output**: Beautiful, informative terminal output

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ODCS Data Contract (YAML)                            │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │  apiVersion: v3.1.0                                                 │   │
│   │  name: customer_contract                                            │   │
│   │  schema:                                                            │   │
│   │    - name: customers                                                │   │
│   │      properties:                                                    │   │
│   │        - name: customer_id                                          │   │
│   │          logicalType: string                                        │   │
│   │          primaryKey: true                                           │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│                            ┌──────────────┐                                 │
│                            │   Lockstep   │                                 │
│                            │     CLI      │                                 │
│                            └──────┬───────┘                                 │
│                                   │                                         │
│          ┌────────────────────────┼────────────────────────┐                │
│          │                        │                        │                │
│          ▼                        ▼                        ▼                │
│  ┌───────────────┐       ┌───────────────┐       ┌───────────────┐          │
│  │  Development  │       │    Staging    │       │  Production   │          │
│  │   Workspace   │       │   Workspace   │       │   Workspace   │          │
│  │               │       │               │       │               │          │
│  │ --catalog-    │       │ --catalog-    │       │ (default      │          │
│  │ override dev  │       │ override stg  │       │  catalog)     │          │
│  └───────────────┘       └───────────────┘       └───────────────┘          │
│        AWS                     Azure                  Azure                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

                    One Contract → Multiple Workspaces & Clouds
```

**Key Benefits:**
- **Single Source of Truth**: Define your schema once in a version-controlled contract
- **Environment Promotion**: Use `--catalog-override` and `--schema-override` for dev/staging/prod
- **Cross-Cloud**: Works with AWS, Azure, and GCP Databricks workspaces
- **CI/CD Integration**: Automate drift detection and deployment across all environments

## Installation

### Using UV Tool (Recommended)

Install as a global CLI tool with an isolated environment:

```bash
# From local directory
uv tool install --python python3.11 /path/to/contract_sync

# From git repository
uv tool install --python python3.11 git+https://github.com/JHampson/lockstep.git

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
git clone https://github.com/JHampson/lockstep.git
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
uv run lockstep plan contracts/
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

The tool supports multiple authentication methods for **AWS**, **Azure**, and **GCP** Databricks via the `--auth-type` argument.

**Precedence order:** CLI parameters → Environment variables → Config file

### Authentication Types

| Type | Description | Required Options |
|------|-------------|------------------|
| `oauth` | Interactive OAuth (Databricks CLI, Azure CLI) | None (default) |
| `pat` | Personal Access Token | `--token` |
| `sp` | Service Principal / OAuth M2M | `--client-id`, `--client-secret` |

### OAuth (Default)

Uses the Databricks SDK credential chain (Azure CLI, Databricks CLI, environment variables):

```bash
# First, authenticate with Databricks CLI
databricks auth login --host https://your-workspace.databricks.com

# Then run the tool (--auth-type oauth is the default)
lockstep apply contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx"
```

### Service Principal (Recommended for CI/CD)

Works on AWS, Azure, and GCP Databricks:

```bash
lockstep apply contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --auth-type sp \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"
```

Or via environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxx"
export DATABRICKS_AUTH_TYPE="sp"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

lockstep apply contracts/
```

### Personal Access Token

```bash
lockstep apply contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --auth-type pat \
  --token "your-personal-access-token"
```

Or via environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxx"
export DATABRICKS_AUTH_TYPE="pat"
export DATABRICKS_TOKEN="your-personal-access-token"

lockstep apply contracts/
```

### Config File

Create `~/.lockstep.yaml` or `~/.lockstep.toml`:

```yaml
# ~/.lockstep.yaml
host: "https://your-workspace.databricks.com"
http_path: "/sql/1.0/warehouses/xxx"

# Choose one authentication method:

# Service Principal (recommended for automation)
auth_type: "sp"
client_id: "your-client-id"
client_secret: "your-client-secret"

# Or Personal Access Token
# auth_type: "pat"
# token: "your-token"
```

```toml
# ~/.lockstep.toml
host = "https://your-workspace.databricks.com"
http_path = "/sql/1.0/warehouses/xxx"

# Service Principal
auth_type = "sp"
client_id = "your-client-id"
client_secret = "your-client-secret"
```

## Quick Start

### 1. Create an ODCS Contract

Create a file `contracts/customers.yaml`:

```yaml
apiVersion: v3.1.0
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

### 3. Preview Changes (Plan)

```bash
lockstep plan contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx"
```

### 4. Apply Changes

```bash
lockstep apply contracts/customers.yaml \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx"
```

## CLI Reference

### `lockstep plan`

Show what changes would be made without applying them.

```bash
lockstep plan PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory containing contract files

**Connection Options:**
- `--host TEXT`: Databricks workspace host URL
- `--sql-endpoint TEXT`: SQL warehouse endpoint path (e.g., `/sql/1.0/warehouses/xxx`)

**Authentication Options:**
- `--auth-type TEXT`: Authentication type: `oauth` (default), `pat`, or `sp`
- `--token TEXT`: Personal Access Token (required when `--auth-type pat`)
- `--client-id TEXT`: OAuth client ID (required when `--auth-type sp`)
- `--client-secret TEXT`: OAuth client secret (required when `--auth-type sp`)

**Override Options:**
- `--catalog-override TEXT`: Override catalog name from contracts
- `--schema-override TEXT`: Override schema name from contracts
- `--table-prefix TEXT`: Prefix to add to table names

**Ignore Options (exclude from plan output):**
- `--ignore-tags`: Exclude tag changes from the plan
- `--ignore-columns`: Exclude column changes from the plan
- `--ignore-descriptions`: Exclude description changes from the plan
- `--ignore-constraints`: Exclude constraint changes from the plan

**Output Options:**
- `--verbose, -v`: Enable verbose output
- `--quiet, -q`: Suppress non-error output
- `--junit-xml PATH`: Output results in JUnit XML format (for CI/CD integration)

**Exit Codes:**
- `0`: No changes needed (in sync)
- `1`: Error occurred
- `2`: Changes detected (drift)

**Examples:**

```bash
# Preview changes for a single contract
lockstep plan contracts/customer.yaml

# Preview changes for all contracts
lockstep plan contracts/

# Ignore tag changes in the plan
lockstep plan contracts/ --ignore-tags

# Only show column changes (ignore everything else)
lockstep plan contracts/ --ignore-tags --ignore-descriptions --ignore-constraints

# Output results as JUnit XML for CI/CD
lockstep plan contracts/ --junit-xml reports/drift.xml
```

### `lockstep apply`

Apply ODCS contracts to Unity Catalog.

```bash
lockstep apply PATH [OPTIONS]
```

**Arguments:**
- `PATH`: Path to YAML file or directory containing contract files

**Connection Options:**
- `--host TEXT`: Databricks workspace host URL
- `--sql-endpoint TEXT`: SQL warehouse endpoint path (e.g., `/sql/1.0/warehouses/xxx`)

**Authentication Options:**
- `--auth-type TEXT`: Authentication type: `oauth` (default), `pat`, or `sp`
- `--token TEXT`: Personal Access Token (required when `--auth-type pat`)
- `--client-id TEXT`: OAuth client ID (required when `--auth-type sp`)
- `--client-secret TEXT`: OAuth client secret (required when `--auth-type sp`)

**Selective Sync - ADD Options (enabled by default):**
- `--add-tags/--no-add-tags`: Add/update tags from contract
- `--add-columns/--no-add-columns`: Add missing columns from contract
- `--add-descriptions/--no-add-descriptions`: Update descriptions from contract
- `--add-constraints/--no-add-constraints`: Add constraints (PK, NOT NULL) from contract

**Selective Sync - REMOVE Options (disabled by default for safety):**
- `--remove-columns/--no-remove-columns`: Remove columns not in contract
- `--remove-tags/--no-remove-tags`: Remove tags not in contract
- `--remove-constraints/--no-remove-constraints`: Remove constraints not in contract

**Override Options:**
- `--catalog-override TEXT`: Override catalog name from contracts
- `--schema-override TEXT`: Override schema name from contracts
- `--table-prefix TEXT`: Prefix to add to table names

**Output Options:**
- `--verbose, -v`: Enable verbose output
- `--quiet, -q`: Suppress non-error output
- `--junit-xml PATH`: Output results in JUnit XML format (for CI/CD integration)

**Examples:**

```bash
# Apply a single contract
lockstep apply contracts/customer.yaml

# Apply with Service Principal (CI/CD)
lockstep apply contracts/ \
  --host "https://your-workspace.databricks.com" \
  --sql-endpoint "/sql/1.0/warehouses/xxx" \
  --auth-type sp \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"

# Apply with Personal Access Token
lockstep apply contracts/ --auth-type pat --token "dapi..."

# Safe apply - only add/update, never remove (default)
lockstep apply contracts/

# Also remove columns not in contract
lockstep apply contracts/ --remove-columns

# Full sync - remove everything not in contract
lockstep apply contracts/ --remove-columns --remove-tags --remove-constraints

# Override catalog for dev environment
lockstep apply contracts/ --catalog-override dev_catalog

# Apply only tags (skip columns, descriptions, constraints)
lockstep apply contracts/ --no-add-columns --no-add-descriptions --no-add-constraints

# Apply only descriptions
lockstep apply contracts/ --no-add-tags --no-add-columns --no-add-constraints
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
- `--junit-xml PATH`: Output results in JUnit XML format (for CI/CD integration)

**Examples:**

```bash
# Validate a single file
lockstep validate contracts/customers.yaml

# Validate all files in a directory
lockstep validate contracts/

# Validate and output JUnit XML report (for CI/CD)
lockstep validate contracts/ --junit-xml reports/validation-report.xml
```

## ODCS Contract Schema

The tool supports a subset of the ODCS specification focused on Unity Catalog synchronization:

### Contract Metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `apiVersion` | string | No | ODCS spec version (default: v3.1.0) |
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

By default, the tool will **ADD** but not **REMOVE**:

| Operation | Default | Flag to Change |
|-----------|---------|----------------|
| Create tables | ✅ Always | - |
| Add columns | ✅ Enabled | `--no-add-columns` |
| Update descriptions | ✅ Enabled | `--no-add-descriptions` |
| Add/update tags | ✅ Enabled | `--no-add-tags` |
| Add constraints | ✅ Enabled | `--no-add-constraints` |
| Drop columns | ❌ Disabled | `--remove-columns` |
| Remove tags | ❌ Disabled | `--remove-tags` |
| Remove constraints | ❌ Disabled | `--remove-constraints` |

### Full Sync Mode

To fully synchronize Unity Catalog with the contract (including removals):

```bash
lockstep apply contracts/ --remove-columns --remove-tags --remove-constraints
```

### Selective Sync Examples

```bash
# Only sync tags (no columns, descriptions, or constraints)
lockstep apply contracts/ --no-add-columns --no-add-descriptions --no-add-constraints

# Only sync descriptions
lockstep apply contracts/ --no-add-tags --no-add-columns --no-add-constraints

# Add tags but also remove tags not in contract
lockstep apply contracts/ --remove-tags
```

## Exit Codes

The CLI uses specific exit codes for CI/CD integration:

| Exit Code | Meaning |
|-----------|---------|
| **0** | Success - no changes needed (in sync) |
| **1** | Error - sync failed, connection error, or validation error |
| **2** | Drift detected (`lockstep plan` only) |

## JUnit XML Reports

All commands support JUnit XML output for CI/CD integration via the `--junit-xml` flag:

```bash
# Validation report
lockstep validate contracts/ --junit-xml reports/validation.xml

# Drift detection report  
lockstep plan contracts/ --junit-xml reports/drift.xml

# Apply results report
lockstep apply contracts/ --junit-xml reports/apply.xml
```

The JUnit XML format is compatible with:
- **GitHub Actions**: Use `EnricoMi/publish-unit-test-result-action`
- **Azure DevOps**: Built-in test results publishing
- **Jenkins**: JUnit plugin
- **GitLab CI**: Built-in JUnit report support

Example report structure:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="lockstep" tests="1" failures="0" errors="0">
  <testsuite name="drift-check" tests="1" failures="0">
    <testcase name="customer_contract" classname="contracts">
      <system-out>No changes needed</system-out>
    </testcase>
  </testsuite>
</testsuites>
```

### CI/CD Drift Detection

Use `lockstep plan` to detect drift between contracts and Unity Catalog. This command:
- Makes no changes to Unity Catalog
- Exits with code 0 if in sync, code 2 if drift detected
- Optionally outputs JUnit XML reports for CI/CD test reporting

```bash
#!/bin/bash
# Check for drift - exit 0 if in sync, 2 if drift detected
lockstep plan contracts/ \
  --host "$DATABRICKS_HOST" \
  --sql-endpoint "$DATABRICKS_HTTP_PATH" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET"

# With JUnit XML output for CI/CD reporting
lockstep plan contracts/ \
  --host "$DATABRICKS_HOST" \
  --sql-endpoint "$DATABRICKS_HTTP_PATH" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --junit-xml reports/drift-report.xml

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
          mkdir -p reports
          lockstep validate contracts/ --junit-xml reports/validation.xml
          
      - name: Upload validation report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: validation-report
          path: reports/validation.xml

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
          mkdir -p reports
          lockstep plan contracts/ --junit-xml reports/drift-check.xml
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
          
      - name: Upload drift check report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: drift-check-report
          path: reports/drift-check.xml
          
      - name: Publish Test Results
        uses: EnricoMi/publish-unit-test-result-action@v2
        if: always()
        with:
          files: reports/*.xml

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
          lockstep apply contracts/
          
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
              lockstep plan contracts/
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
                    lockstep apply contracts/
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
git clone https://github.com/JHampson/lockstep.git
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

## Code Architecture

```
src/lockstep/
├── __init__.py
├── models/                # Pydantic models
│   ├── contract.py        # ODCS contract models
│   └── catalog_state.py   # Unity Catalog state models
├── databricks/            # Databricks connectivity
│   ├── config.py          # Configuration handling
│   └── connector.py       # SQL connector
├── services/              # Business logic
│   ├── contract_loader.py # YAML loading/validation
│   ├── introspection.py   # Catalog state fetching
│   ├── diff.py            # Contract vs catalog diffing
│   ├── sql_generator.py   # SQL generation
│   └── sync.py            # Orchestration
└── cli/                   # CLI interface
    ├── main.py            # Typer commands
    ├── formatters.py      # Rich terminal output
    └── junit_reporter.py  # JUnit XML generation
```

## Extending

The tool is designed to be extensible:

- **Additional ODCS Fields**: Pydantic models use `extra="allow"` for forward compatibility
- **New Action Types**: Add to `ActionType` enum and implement in `DiffService`
- **Custom SQL**: Extend `SQLGenerator` for additional operations
- **New Commands**: Add Typer commands in `cli/main.py`



