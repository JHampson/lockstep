"""Loader for ODCS YAML contract files."""

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from lockstep.models.contract import Contract

logger = logging.getLogger(__name__)


class ContractLoadError(Exception):
    """Raised when a contract fails to load or validate."""

    def __init__(self, message: str, path: Path | None = None, errors: list[str] | None = None):
        self.path = path
        self.errors = errors or []
        super().__init__(message)


class ContractLoader:
    """Loader for ODCS YAML contract files.

    Supports loading from:
    - Single YAML file
    - Directory containing YAML files
    - Glob patterns
    """

    YAML_EXTENSIONS = {".yaml", ".yml"}

    def __init__(self) -> None:
        """Initialize the contract loader."""
        self._validation_errors: list[ContractLoadError] = []

    @property
    def validation_errors(self) -> list[ContractLoadError]:
        """Get accumulated validation errors."""
        return self._validation_errors

    def clear_errors(self) -> None:
        """Clear accumulated validation errors."""
        self._validation_errors = []

    def _find_yaml_files(self, path: Path) -> Generator[Path, None, None]:
        """Find all YAML files in a path.

        Args:
            path: File or directory path.

        Yields:
            Paths to YAML files.
        """
        if path.is_file():
            if path.suffix.lower() in self.YAML_EXTENSIONS:
                yield path
            else:
                logger.warning(f"Skipping non-YAML file: {path}")
        elif path.is_dir():
            # Use a set to avoid duplicates when recursively finding files
            seen: set[Path] = set()
            for ext in self.YAML_EXTENSIONS:
                for yaml_file in path.glob(f"**/*{ext}"):
                    if yaml_file not in seen:
                        seen.add(yaml_file)
                        yield yaml_file
        else:
            # Try as glob pattern
            parent = path.parent if path.parent.exists() else Path.cwd()
            yield from parent.glob(path.name)

    def _parse_yaml(self, path: Path) -> dict[str, Any] | list[Any] | None:
        """Parse a YAML file.

        Args:
            path: Path to YAML file.

        Returns:
            Parsed YAML content.

        Raises:
            ContractLoadError: If YAML parsing fails.
        """
        try:
            with open(path, encoding="utf-8") as f:
                result: dict[str, Any] | list[Any] | None = yaml.safe_load(f)
                return result
        except yaml.YAMLError as e:
            raise ContractLoadError(
                f"Failed to parse YAML: {e}",
                path=path,
                errors=[str(e)],
            ) from e
        except OSError as e:
            raise ContractLoadError(
                f"Failed to read file: {e}",
                path=path,
                errors=[str(e)],
            ) from e

    def load_one(self, path: Path) -> Contract:
        """Load a single contract from a YAML file.

        Args:
            path: Path to YAML file.

        Returns:
            Parsed and validated Contract.

        Raises:
            ContractLoadError: If loading or validation fails.
        """
        logger.debug(f"Loading contract from {path}")
        data = self._parse_yaml(path)

        if data is None:
            raise ContractLoadError("YAML file is empty", path=path)

        if isinstance(data, list):
            raise ContractLoadError(
                "Expected a single contract, got a list. Use load_many() for multiple contracts.",
                path=path,
            )

        try:
            contract = Contract.model_validate(data)
            logger.info(f"Loaded contract '{contract.name}' from {path}")
            return contract
        except ValidationError as e:
            errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            raise ContractLoadError(
                f"Contract validation failed with {len(errors)} error(s)",
                path=path,
                errors=errors,
            ) from e

    def load_many(self, path: Path) -> list[Contract]:
        """Load multiple contracts from a path.

        Handles:
        - Single file with one contract
        - Single file with multiple documents (YAML multi-doc)
        - Directory with multiple YAML files

        Args:
            path: File, directory, or glob pattern.

        Returns:
            List of validated contracts.
        """
        self.clear_errors()
        contracts: list[Contract] = []

        for yaml_path in self._find_yaml_files(path):
            try:
                # Try loading as multi-document YAML
                with open(yaml_path, encoding="utf-8") as f:
                    docs = list(yaml.safe_load_all(f))

                for i, doc in enumerate(docs):
                    if doc is None:
                        continue
                    try:
                        contract = Contract.model_validate(doc)
                        contracts.append(contract)
                        logger.info(f"Loaded contract '{contract.name}' from {yaml_path}")
                    except ValidationError as e:
                        errors = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
                        error = ContractLoadError(
                            f"Document {i + 1} validation failed",
                            path=yaml_path,
                            errors=errors,
                        )
                        self._validation_errors.append(error)
                        logger.error(f"Validation error in {yaml_path}: {error}")

            except yaml.YAMLError as e:
                error = ContractLoadError(
                    f"YAML parsing failed: {e}",
                    path=yaml_path,
                    errors=[str(e)],
                )
                self._validation_errors.append(error)
                logger.error(f"YAML error in {yaml_path}: {e}")

            except OSError as e:
                error = ContractLoadError(
                    f"Failed to read file: {e}",
                    path=yaml_path,
                    errors=[str(e)],
                )
                self._validation_errors.append(error)
                logger.error(f"IO error reading {yaml_path}: {e}")

        return contracts

    def validate_file(self, path: Path) -> tuple[bool, list[str]]:
        """Validate a contract file without fully loading it.

        Args:
            path: Path to YAML file.

        Returns:
            Tuple of (is_valid, list of error messages).
        """
        try:
            self.load_one(path)
            return True, []
        except ContractLoadError as e:
            return False, e.errors if e.errors else [str(e)]

    def format_validation_report(self) -> str:
        """Format accumulated validation errors as a report.

        Returns:
            Human-readable validation report.
        """
        if not self._validation_errors:
            return "All contracts validated successfully."

        lines = [f"Validation failed for {len(self._validation_errors)} file(s):\n"]

        for error in self._validation_errors:
            lines.append(f"\n📄 {error.path or 'Unknown file'}:")
            lines.append(f"   Error: {error}")
            if error.errors:
                for err_msg in error.errors:
                    lines.append(f"   • {err_msg}")

        return "\n".join(lines)
