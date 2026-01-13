"""Pydantic models representing ODCS YAML contract structure.

Based on Open Data Contract Standard (ODCS) v3.x specification.
Reference: https://bitol-io.github.io/open-data-contract-standard/
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ContractStatus(str, Enum):
    """Status of the data contract."""

    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class DataType(str, Enum):
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


# Mapping from ODCS logical types to Databricks SQL types
ODCS_TO_DATABRICKS_TYPE: dict[str, str] = {
    "string": "STRING",
    "integer": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
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


class TableInfo(BaseModel):
    """Target table identification in Unity Catalog."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    catalog: str = Field(..., description="Unity Catalog name")
    schema_name: str = Field(..., alias="schema", description="Schema/database name")
    table: str = Field(..., description="Table name")

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class Contract(BaseModel):
    """Root model for an ODCS YAML contract.

    This model represents the core structure needed for Unity Catalog synchronization.
    Additional ODCS fields can be added as the tool evolves.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Contract metadata
    api_version: str = Field(
        default="v3.0.0",
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
    description: str | None = Field(default=None, description="Contract description")

    # Target table info
    table_info: TableInfo = Field(..., alias="dataset", description="Target table information")

    # Schema definition
    schema_def: ContractSchema = Field(
        ...,
        alias="schema",
        description="Schema definition with columns",
    )

    # Table-level metadata
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Table-level key-value tags (use 'system.certification_status' for certification)",
    )

    # Owner info (optional, for future use)
    owner: str | None = Field(default=None, description="Data owner")
    team: str | None = Field(default=None, description="Owning team")

    @property
    def columns(self) -> list[Column]:
        """Convenience accessor for columns."""
        return self.schema_def.columns

    @property
    def primary_key_columns(self) -> list[str]:
        """Get list of primary key column names."""
        return [col.name for col in self.columns if col.primary_key]

    def get_full_table_name(
        self,
        catalog_override: str | None = None,
        schema_override: str | None = None,
        table_prefix: str | None = None,
    ) -> str:
        """Get fully qualified table name with optional overrides."""
        catalog = catalog_override or self.table_info.catalog
        schema = schema_override or self.table_info.schema_name
        table = self.table_info.table
        if table_prefix:
            table = f"{table_prefix}{table}"
        return f"{catalog}.{schema}.{table}"
