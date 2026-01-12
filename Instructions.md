
You are an expert Python backend engineer.  
Build a production-grade Python CLI application with the following requirements.

## High‑level goal

Create a CLI tool that takes an Open Data Contract Standard (ODCS) YAML specification and synchronises the described contract into Databricks Unity Catalog using a SQL endpoint.

The CLI should:
- Create tables if they do not exist.
- Create columns if they do not exist.
- Update descriptions for tables and columns.
- Set constraints such as primary key and not null (informational constraints in Databricks).
- Create, delete and update tags on tables and columns.
- Set the certified status of the table using Unity Catalog system tags (for example `system.certification_status = 'certified'`).

## Tech stack and architecture

Implement the app with these choices:

- Language: Python 3.11+
- CLI framework: Typer or Click (prefer Typer for type hints and good UX).
- Config / models: Pydantic (v2 if possible).
- Databricks:
  - Connect via Databricks SQL endpoint using the official Databricks SQL connector for Python (or `sqlalchemy` + Databricks driver if more appropriate).
  - The CLI should authenticate using environment variables or command line parameters (e.g. personal access token, host, HTTP path). Prefer oAuth rather than PAT
- YAML parsing: `pyyaml` or `ruamel.yaml`.
- Packaging: standard `pyproject.toml`
- Use uv for package managment 

Use a layered architecture with clear separation:

- `models/` – Pydantic models representing the ODCS YAML contract (at least fundamentals needed for schema, columns, constraints, tags, and certification).
- `services/` – Unity Catalog synchronisation logic (read contract models, generate SQL, apply changes).
- `databricks/` – small data access layer for running SQL against the Databricks SQL endpoint.
- `cli/` – Typer/Click commands, argument parsing, logging configuration.

## ODCS YAML handling

ODCS reference: YAML data contracts as defined by the Open Data Contract Standard.[2][9][1]

Implement:

1. A Pydantic model (or set of models) that can parse a subset of ODCS YAML relevant to:
   - Contract metadata: name, version, status.
   - Target object identity: catalog, schema/database, table name.
   - Schema: list of columns with:
     - name
     - logical/physical type
     - nullable / required
     - description / business definition
     - constraints relevant to primary key and not null
     - tags at column level (e.g. for PII, data domain, etc.).[1][2]
   - Table‑level metadata:
     - description
     - tags (key/value pairs)
     - certification status (e.g. `certified`, `deprecated` via system tag).[8][7]
2. A loader that:
   - Reads one or more ODCS YAML files from a path.
   - Validates them against the Pydantic models.
   - Provides a clear validation error report to the user.

Design the Pydantic models so they are:
- Strict about required fields.
- Easy to extend later (e.g. optional data quality rules, SLAs).

## Unity Catalog synchronisation behaviour

Implement idempotent sync logic:

- For each contract:
  - Resolve target `catalog.schema.table` in Unity Catalog.
  - If table does not exist, create it with:
    - appropriate column definitions inferred from the contract types.
    - table comment/description from the contract.
  - If table exists:
    - Add missing columns.
    - Do not drop columns by default (allow an optional `--allow-destructive` mode that can drop columns to match the spec).
    - Update column comments/descriptions from the contract.
- Constraints:
  - Apply primary key and not null constraints using supported Databricks SQL syntax for Unity Catalog tables.[4][3]
  - Treat constraints as informational but keep them in sync with the contract (create if missing; consider update/drop if they no longer match).
- Tags:
  - Table‑level tags:
    - Use Unity Catalog tag syntax (`ALTER TABLE ... SET TAGS`, or `SET TAG ON TABLE ...` depending on runtime).[6][5]
    - Create tags that are in the contract but not in Unity Catalog.
    - Remove tags that exist in Unity Catalog but are not in the contract (unless a `--preserve-extra-tags` flag is set).
    - Update tag values when they differ.
  - Column‑level tags:
    - Same behavior as table‑level tags but applied to columns.
- Certification:
  - Use the `system.certification_status` tag (or the documented system tag) to set the table as certified/deprecated according to the contract.[7][8]
  - Ensure that the CLI can:
    - Set certification.
    - Clear or update certification when the contract changes.

All sync operations should be:
- Idempotent (running twice with same contract should result in no changes on second run).
- Logged clearly (what was created/updated/skipped).

## CLI design

Use Typer (preferred) with subcommands such as:

- `odcs-sync from-file PATH`  
  - Required args:
    - `PATH` to a YAML file or directory (glob support).
  - Options:
    - `--dry-run` to show planned SQL / changes without executing.
    - `--allow-destructive` to allow drops of columns/tags/constraints.
    - `--preserve-extra-tags` to avoid removing tags not in the contract.
    - `--catalog-override`, `--schema-override`, `--table-prefix` (optional) to adjust mapping without changing contracts.
- `odcs-validate PATH`  
  - Only validate ODCS YAML against Pydantic models and report errors.

Features:

- Include `--verbose` / `--quiet` flags.
- Provide rich `--help` text.
- Return non‑zero exit codes when there are validation or sync errors.

## Databricks connectivity

Implement a small adapter for Databricks SQL:

- Configuration sources:
  - Environment variables: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, plus optional `DATABRICKS_CATALOG_DEFAULT` etc.
  - Optional config file (e.g. `~/.odcs_sync.toml` or `yaml`) with the same fields.
- Connector:
  - Provide a context manager for opening/closing connections.
  - Expose utility methods:
    - `execute(sql: str, params: dict | None = None)` for non‑select.
    - `fetchone`, `fetchall` helpers where needed (e.g. introspection of existing tables/columns/tags).
- Use parameterised queries where possible to avoid injection issues.

## Introspection and diffing

Implement logic to compare the ODCS contract with current Unity Catalog state:

- Functions to:
  - Fetch table metadata (columns, types, nullability, comments, constraints, tags, certification).
  - Build an in‑memory representation of the current state.
- A “diff” function that:
  - Compares contract vs current state.
  - Produces a structured plan of actions: `create_table`, `add_column`, `update_column_description`, `add_tag`, `remove_tag`, `set_certification`, etc.
- The sync command:
  - Optionally prints a human‑readable diff when `--dry-run` is provided.
  - Applies the plan in a deterministic order when not dry‑run.

## Engineering best practices

Please follow strong engineering standards:

- Structure:
  - Use a clear package layout (`src/` layout).
  - Group modules by responsibility (models, services, cli, databricks).
- Type safety:
  - Use type hints everywhere; enable `mypy` configuration.
- Testing:
  - Add unit tests for:
    - ODCS YAML parsing to Pydantic models.
    - Diffing logic (contract vs catalog state).
    - SQL generation for create/alter/tag/certification operations.
- Tooling:
  - Add `ruff` or `flake8` plus `black` or `ruff format` for linting/formatting.
  - Include simple `make` or `tox` tasks (e.g. `test`, `lint`, `format`).
- Logging and errors:
  - Use `logging` with configurable level.
  - Distinguish between validation errors, connectivity errors, and sync errors.
  - Provide clear error messages for end users.

## Developer experience

- Provide a concise `README.md` describing:
  - Installation.
  - Example ODCS YAML snippet showing the fields used by this tool.
  - Example CLI usage, including `--dry-run` and environment config.
- Make the project easy to extend with:
  - Additional ODCS sections (e.g. data quality rules) later.
  - Additional Databricks features (e.g. views, permissions) later.[10][11]

Start by scaffolding the project structure and core Pydantic models, then implement the Databricks adapter, then the diff/sync service, and finally wire up the CLI commands.

[1](https://bitol-io.github.io/open-data-contract-standard/v3.0.1/)
[2](https://bitol-io.github.io/open-data-contract-standard/v3.1.0/)
[3](https://datasavvy.me/2023/11/21/databricks-unity-catalog-primary-key-and-foreign-key-constraints-are-not-enforced/)
[4](https://docs.databricks.com/aws/en/tables/constraints)
[5](https://docs.databricks.com/aws/en/database-objects/tags)
[6](https://learn.microsoft.com/en-us/azure/databricks/database-objects/tags)
[7](https://docs.databricks.com/aws/en/data-governance/unity-catalog/certify-deprecate-data)
[8](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/certify-deprecate-data)
[9](https://datacontract.com)
[10](https://docs.azure.cn/en-us/databricks/machine-learning/feature-store/uc/feature-tables-uc)
[11](https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices)
[12](https://www.maskset.net/blog/2025/07/01/improving-python-clis-with-pydantic-and-dataclasses/)
[13](https://www.reddit.com/r/Python/comments/18j41fv/feud_build_simple_clis_based_on_pydantic_for/)
[14](https://blog.opendataproducts.org/data-contract-support-added-to-open-data-product-specification-90b570ace1d7)
[15](https://www.linkedin.com/posts/bartosz-gajda_databricks-datagovernance-unitycatalog-activity-7385735818197221376-FmGy)