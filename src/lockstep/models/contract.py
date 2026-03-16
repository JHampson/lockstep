"""Pydantic models representing ODCS YAML contract structure.

Based on Open Data Contract Standard (ODCS) v3.x specification.
Reference: https://bitol-io.github.io/open-data-contract-standard/
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ContractStatus(StrEnum):
    """Status of the data contract."""

    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DataType(StrEnum):
    """Logical data types supported in ODCS contracts.

    These map to Databricks Unity Catalog physical types.
    """

    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMP_NTZ = "timestamp_ntz"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"
    NUMBER = "number"  # ODCS v3 type - maps to DOUBLE


# Mapping from ODCS logical types to Databricks SQL types
ODCS_TO_DATABRICKS_TYPE: dict[str, str] = {
    "string": "STRING",
    "integer": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "number": "DOUBLE",  # ODCS v3 'number' type
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "binary": "BINARY",
    "array": "ARRAY",
    "map": "MAP",
    "struct": "STRUCT",
}


class Column(BaseModel):
    """Column definition in an ODCS contract."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str = Field(..., description="Column name")
    logical_type: str = Field(
        ...,
        alias="logicalType",
        description="Logical data type (e.g., string, integer, timestamp)",
    )
    physical_type: str | None = Field(
        default=None,
        alias="physicalType",
        description="Physical type override for Databricks (e.g., VARCHAR(100))",
    )
    description: str | None = Field(
        default=None, description="Column description/business definition"
    )
    required: bool = Field(default=False, description="Whether the column is required (NOT NULL)")
    primary_key: bool = Field(
        default=False,
        alias="primaryKey",
        description="Whether this column is part of the primary key",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Key-value tags for the column (e.g., PII, data domain)",
    )
    # Additional ODCS fields that can be extended
    business_name: str | None = Field(default=None, alias="businessName")
    classification: str | None = Field(default=None, description="Data classification level")
    # ODCS v3 logical type options (validation constraints)
    logical_type_options: dict[str, Any] | None = Field(
        default=None,
        alias="logicalTypeOptions",
        description="Validation constraints (pattern, minimum, maximum, etc.)",
    )

    @field_validator("logical_type", mode="before")
    @classmethod
    def normalize_logical_type(cls, v: str) -> str:
        """Normalize logical type to lowercase."""
        return v.lower() if isinstance(v, str) else v

    def get_databricks_type(self) -> str:
        """Get the Databricks SQL type for this column."""
        if self.physical_type:
            return self.physical_type.upper()
        return ODCS_TO_DATABRICKS_TYPE.get(self.logical_type, "STRING")


class ContractSchema(BaseModel):
    """Schema section of an ODCS contract containing columns."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    columns: list[Column] = Field(
        default_factory=list,
        alias="properties",
        description="List of column definitions",
    )

    @field_validator("columns", mode="before")
    @classmethod
    def parse_columns(cls, v: Any) -> list[dict[str, Any]]:
        """Parse columns from various ODCS formats.

        ODCS can represent columns as:
        - A list of column objects
        - A dict mapping column names to column definitions
        """
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            # Convert dict format to list format
            result = []
            for col_name, col_def in v.items():
                if isinstance(col_def, dict):
                    col_def["name"] = col_name
                    result.append(col_def)
                else:
                    result.append({"name": col_name, "logicalType": str(col_def)})
            return result
        return []


class Server(BaseModel):
    """Server definition in ODCS v3 format."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    server: str | None = Field(default=None, description="Server identifier")
    type: str | None = Field(default=None, description="Server type (e.g., databricks)")
    host: str | None = Field(default=None, description="Server hostname")
    catalog: str = Field(..., description="Unity Catalog name")
    schema_name: str = Field(..., alias="schema", description="Schema/database name")


class SchemaItem(BaseModel):
    """Schema item in ODCS v3 format (represents a table/model)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str = Field(..., description="Table/model name")
    physical_type: str | None = Field(
        default="table",
        alias="physicalType",
        description="Physical type (table, view, etc.)",
    )
    description: str | None = Field(default=None, description="Table description")
    properties: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Column definitions",
    )
    quality: list[dict[str, Any]] | None = Field(
        default=None,
        description="Quality rules (DQX)",
    )


class TableInfo(BaseModel):
    """Target table identification in Unity Catalog."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    catalog: str = Field(default="", description="Unity Catalog name")
    schema_name: str = Field(default="", alias="schema", description="Schema/database name")
    table: str = Field(..., description="Table name")

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class Permission(StrEnum):
    """Unity Catalog table permissions."""

    SELECT = "SELECT"
    MODIFY = "MODIFY"
    ALL_PRIVILEGES = "ALL PRIVILEGES"
    # Additional privileges
    APPLY_TAG = "APPLY TAG"
    READ_METADATA = "READ_METADATA"


class ODCSRole(BaseModel):
    """ODCS v3 role definition for documenting IAM roles.

    This follows the official ODCS format. For actual GRANT/REVOKE operations,
    define principal and privileges in customProperties.

    Example YAML (single principal):
        roles:
          - role: data_engineers
            access: read_write
            description: Engineering team with full access
            customProperties:
              - property: principal
                value: data_engineers
              - property: privileges
                value: [SELECT, MODIFY]

    Example YAML (multiple principals with same privileges):
        roles:
          - role: shared_access
            access: read_only
            description: Multiple teams with read access
            customProperties:
              - property: principal
                value:
                  - data_engineers
                  - data_analysts
                  - admin@company.com
              - property: privileges
                value: [SELECT]
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    role: str = Field(..., description="Name of the IAM role")
    access: str | None = Field(default=None, description="Type of access (read, write, read_write)")
    description: str | None = Field(default=None, description="Description of the role")
    custom_properties: list[dict[str, Any]] = Field(
        default_factory=list,
        alias="customProperties",
        description="Custom properties including permission details",
    )

    def get_permission_grants(self) -> list[PermissionGrant]:
        """Extract PermissionGrant(s) from customProperties if defined.

        Supports both single principal (string) and multiple principals (list).

        Example with single principal:
            customProperties:
              - property: principal
                value: data_engineers
              - property: privileges
                value: [SELECT, MODIFY]

        Example with multiple principals:
            customProperties:
              - property: principal
                value:
                  - data_engineers
                  - data_analysts
              - property: privileges
                value: [SELECT]

        Returns:
            List of PermissionGrant objects, one per principal.
        """
        principals: list[str] = []
        privileges: list[str] = []

        for prop in self.custom_properties:
            prop_name = prop.get("property", "")
            prop_value = prop.get("value")

            if prop_name == "principal":
                if isinstance(prop_value, str):
                    principals = [prop_value]
                elif isinstance(prop_value, list):
                    principals = [str(p) for p in prop_value]
            elif prop_name == "privileges" and isinstance(prop_value, list):
                privileges = [str(p).upper() for p in prop_value]

        # Create a PermissionGrant for each principal
        grants = []
        if principals and privileges:
            for principal in principals:
                grants.append(
                    PermissionGrant(
                        principal=principal,
                        privileges=privileges,
                    )
                )
        return grants


class PermissionGrant(BaseModel):
    """Permission grant for a principal (user or group).

    Used in customProperties to define actual GRANT/REVOKE operations.

    Example in customProperties:
        customProperties:
          - property: principal
            value: data_engineers
          - property: privileges
            value: [SELECT, MODIFY]
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    principal: str = Field(..., description="User email or group name")
    privileges: list[str] = Field(
        default_factory=list,
        description="List of privileges (SELECT, MODIFY, ALL PRIVILEGES, etc.)",
    )

    @field_validator("privileges", mode="before")
    @classmethod
    def normalize_privileges(cls, v: list[str]) -> list[str]:
        """Normalize privileges to uppercase."""
        if isinstance(v, list):
            return [p.upper() if isinstance(p, str) else p for p in v]
        return v


# Keep Role as alias for backward compatibility
Role = PermissionGrant


def parse_tags(tags_input: Any) -> dict[str, str]:
    """Parse tags from ODCS v3 array format or dict format.

    ODCS v3 uses array format: ["domain:transport", "classification:internal"]
    Legacy format uses dict: {"domain": "transport", "classification": "internal"}

    Args:
        tags_input: Tags in either format

    Returns:
        Dictionary of tag key-value pairs
    """
    if tags_input is None:
        return {}

    if isinstance(tags_input, dict):
        # Legacy dict format - ensure all values are strings
        return {str(k): str(v) for k, v in tags_input.items()}

    if isinstance(tags_input, list):
        # ODCS v3 array format: ["key:value", ...]
        result: dict[str, str] = {}
        for tag in tags_input:
            if isinstance(tag, str) and ":" in tag:
                key, value = tag.split(":", 1)
                result[key.strip()] = value.strip()
            elif isinstance(tag, str):
                # Tag without value - use empty string
                result[tag.strip()] = ""
        return result

    return {}


class Contract(BaseModel):
    """Root model for an ODCS YAML contract.

    Supports both ODCS v3 format and legacy formats for backward compatibility.
    ODCS v3 uses:
    - servers: [{catalog, schema, host, type}]
    - schema: [{name, physicalType, properties: [...]}]
    - tags: ["key:value", ...]
    - description: {usage: "..."} or string

    Legacy format uses:
    - dataset: {catalog, schema, table}
    - schema: {properties: [...]}
    - tags: {key: "value"}
    - description: "string"
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Contract metadata
    api_version: str = Field(
        default="v3.1.0",
        alias="apiVersion",
        description="ODCS specification version",
    )
    kind: str = Field(default="DataContract", description="Resource kind")
    id: str | None = Field(default=None, description="Unique contract identifier")
    name: str = Field(..., description="Contract name")
    version: str = Field(default="1.0.0", description="Contract version")
    status: ContractStatus = Field(
        default=ContractStatus.DRAFT,
        description="Contract lifecycle status",
    )

    # Description can be string or object with 'usage' field (ODCS v3)
    description: str | None = Field(default=None, description="Contract description")

    # ODCS v3 servers array
    servers: list[Server] | None = Field(
        default=None,
        description="Server configurations (ODCS v3 format)",
    )

    # Legacy dataset field (for backward compatibility)
    dataset: TableInfo | None = Field(
        default=None,
        description="Target table information (legacy format)",
    )

    # Schema definition - can be dict (legacy) or list (ODCS v3)
    schema_def: ContractSchema | None = Field(
        default=None,
        alias="schema",
        description="Schema definition with columns",
    )

    # Raw schema for ODCS v3 array format (processed in model_validator)
    _schema_items: list[SchemaItem] = []

    # Table-level metadata
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Table-level key-value tags (use 'system.certification_status' for certification)",
    )

    # Owner info (optional, for future use)
    owner: str | None = Field(default=None, description="Data owner")
    team: str | None = Field(default=None, description="Owning team")

    # ODCS v3 roles section (for documentation, not direct GRANT/REVOKE)
    roles: list[ODCSRole] = Field(
        default_factory=list,
        description="ODCS role definitions (documentation only)",
    )

    # Custom properties (can include permissions for GRANT/REVOKE)
    custom_properties: list[dict[str, Any]] = Field(
        default_factory=list,
        alias="customProperties",
        description="Custom properties including permission grants",
    )

    # Computed table info (set in validator)
    _table_info: TableInfo | None = None

    @field_validator("description", mode="before")
    @classmethod
    def parse_description(cls, v: Any) -> str | None:
        """Parse description from string or ODCS v3 object format."""
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            # ODCS v3 format: {usage: "...", ...}
            return v.get("usage") or v.get("purpose") or str(v)
        return str(v)

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags_field(cls, v: Any) -> dict[str, str]:
        """Parse tags from array or dict format."""
        return parse_tags(v)

    @field_validator("schema_def", mode="before")
    @classmethod
    def parse_schema(cls, v: Any) -> dict[str, Any] | None:
        """Parse schema from ODCS v3 array format or legacy dict format."""
        if v is None:
            return None

        if isinstance(v, dict):
            # Legacy format: {properties: [...]}
            return v

        if isinstance(v, list) and len(v) > 0:
            # ODCS v3 format: [{name, physicalType, properties: [...]}]
            # Extract first schema item's properties
            first_item = v[0]
            if isinstance(first_item, dict):
                properties = first_item.get("properties", [])
                return {"properties": properties}

        return None

    @model_validator(mode="before")
    @classmethod
    def extract_table_info_from_servers(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Extract table info from ODCS v3 servers + schema structure."""
        if not isinstance(data, dict):
            return data

        # If dataset is already provided (legacy format), use it
        if data.get("dataset"):
            return data

        # Extract table name from schema array
        servers = data.get("servers", [])
        schema = data.get("schema", [])

        table_name = ""
        if schema and isinstance(schema, list) and len(schema) > 0:
            first_schema = schema[0]
            if isinstance(first_schema, dict):
                table_name = first_schema.get("name", "")

        # Extract catalog/schema from servers if available
        catalog = ""
        schema_name = ""
        if servers and isinstance(servers, list) and len(servers) > 0:
            server = servers[0]
            if isinstance(server, dict):
                catalog = server.get("catalog", "")
                schema_name = server.get("schema", "")

        # Build dataset if we have at least a table name
        # (catalog/schema can be filled later via overrides or defaults)
        if table_name:
            data["dataset"] = {
                "catalog": catalog,
                "schema": schema_name,
                "table": table_name,
            }

        return data

    @property
    def table_info(self) -> TableInfo:
        """Get table info from dataset or servers."""
        if self.dataset:
            return self.dataset
        raise ValueError("No table info available - check dataset or servers configuration")

    @property
    def columns(self) -> list[Column]:
        """Convenience accessor for columns."""
        if self.schema_def:
            return self.schema_def.columns
        return []

    @property
    def primary_key_columns(self) -> list[str]:
        """Get list of primary key column names."""
        return [col.name for col in self.columns if col.primary_key]

    @property
    def permission_grants(self) -> list[PermissionGrant]:
        """Extract permission grants from roles' customProperties.

        Each role can define principal (single or list) and privileges
        in its customProperties section.

        Returns:
            List of PermissionGrant objects for GRANT/REVOKE operations.
        """
        grants = []
        for role in self.roles:
            grants.extend(role.get_permission_grants())
        return grants

    def get_full_table_name(
        self,
        catalog_override: str | None = None,
        schema_override: str | None = None,
        table_prefix: str | None = None,
    ) -> str:
        """Get fully qualified table name with optional overrides."""
        info = self.table_info
        catalog = catalog_override or info.catalog
        schema = schema_override or info.schema_name
        table = info.table

        if not catalog:
            raise ValueError(
                f"Contract '{self.name}' has no catalog defined. "
                "Set it in the contract (dataset or servers) or use --catalog-override."
            )
        if not schema:
            raise ValueError(
                f"Contract '{self.name}' has no schema defined. "
                "Set it in the contract (dataset or servers) or use --schema-override."
            )

        if table_prefix:
            table = f"{table_prefix}{table}"
        return f"{catalog}.{schema}.{table}"
