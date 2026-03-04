"""
Python ETL toolkit for building data pipelines in NbS and pharma BI contexts.

Provides reusable extract, transform, and load components: configurable
transform pipelines, data type coercion, deduplication, and basic CDC
(Change Data Capture) pattern for incremental loads.

Author: github.com/achmadnaufal
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable
import hashlib
import json


class ETLToolkit:
    """
    Reusable ETL pipeline components.

    Supports configurable transform chains, data validation, deduplication,
    and incremental load patterns for typical NbS/pharma data pipelines.

    Args:
        config: Optional dict with keys:
            - dedup_keys: Columns to use for deduplication
            - transforms: List of transform step names to apply

    Example:
        >>> etl = ETLToolkit(config={"dedup_keys": ["record_id", "period"]})
        >>> df = etl.extract("data/raw.csv")
        >>> df = etl.transform_pipeline(df, steps=["normalize", "deduplicate"])
        >>> summary = etl.load_summary(df)
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.dedup_keys = self.config.get("dedup_keys", [])

    def load_data(self, filepath: str) -> pd.DataFrame:
        """
        Extract data from CSV or Excel source.

        Args:
            filepath: Path to source file.

        Returns:
            Raw DataFrame.

        Raises:
            FileNotFoundError: If file does not exist.
        """
        p = Path(filepath)
        if not p.exists():
            raise FileNotFoundError(f"Source file not found: {filepath}")
        if p.suffix in (".xlsx", ".xls"):
            return pd.read_excel(filepath)
        return pd.read_csv(filepath)

    def extract(self, filepath: str) -> pd.DataFrame:
        """Alias for load_data. Extracts raw data from source."""
        return self.load_data(filepath)

    def validate(self, df: pd.DataFrame) -> bool:
        """
        Validate DataFrame is non-empty.

        Args:
            df: DataFrame to validate.

        Returns:
            True if valid.

        Raises:
            ValueError: If empty.
        """
        if df.empty:
            raise ValueError("Input DataFrame is empty")
        return True

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names and fill missing values."""
        df = df.copy()
        df.dropna(how="all", inplace=True)
        df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
        num_cols = df.select_dtypes(include="number").columns
        for col in num_cols:
            if df[col].isnull().any():
                df[col].fillna(df[col].median(), inplace=True)
        return df

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names (snake_case, lowercase)."""
        df = df.copy()
        df.columns = [c.lower().strip().replace(" ", "_").replace("-", "_") for c in df.columns]
        return df

    def deduplicate(self, df: pd.DataFrame, keys: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Remove duplicate records based on key columns.

        Args:
            df: Input DataFrame.
            keys: Columns to use as dedup keys. Uses config dedup_keys if None.
                  Uses all columns if empty.

        Returns:
            Deduplicated DataFrame with count of removed records in attrs.
        """
        k = keys or self.dedup_keys or list(df.columns)
        available_keys = [c for c in k if c in df.columns]
        before = len(df)
        df = df.drop_duplicates(subset=available_keys if available_keys else None, keep="first")
        df.attrs["dedup_removed"] = before - len(df)
        return df

    def coerce_types(self, df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
        """
        Coerce DataFrame columns to specified types.

        Args:
            df: Input DataFrame.
            schema: Dict mapping column names to types ('int', 'float', 'str', 'datetime').

        Returns:
            DataFrame with coerced column types.
        """
        df = df.copy()
        for col, dtype in schema.items():
            if col not in df.columns:
                continue
            try:
                if dtype in ("int", "integer"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                elif dtype in ("float", "numeric"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "str":
                    df[col] = df[col].astype(str).str.strip()
                elif dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
        return df

    def add_row_hash(self, df: pd.DataFrame, hash_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Add a MD5 row hash column for CDC (Change Data Capture) detection.

        Args:
            df: Input DataFrame.
            hash_cols: Columns to include in hash. Uses all columns if None.

        Returns:
            DataFrame with added 'row_hash' column.
        """
        df = df.copy()
        cols = hash_cols or list(df.columns)
        cols = [c for c in cols if c in df.columns]
        df["row_hash"] = df[cols].apply(
            lambda row: hashlib.md5(json.dumps(row.astype(str).tolist(), sort_keys=True).encode()).hexdigest(),
            axis=1,
        )
        return df

    def transform_pipeline(
        self, df: pd.DataFrame, steps: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Apply a configurable sequence of transform steps.

        Available steps: 'normalize', 'deduplicate', 'hash'

        Args:
            df: Input DataFrame.
            steps: List of step names. Defaults to ['normalize', 'deduplicate'].

        Returns:
            Transformed DataFrame.

        Raises:
            ValueError: If an unknown step name is provided.
        """
        if steps is None:
            steps = ["normalize", "deduplicate"]

        step_map: Dict[str, Callable] = {
            "normalize": self.normalize_columns,
            "deduplicate": self.deduplicate,
            "hash": self.add_row_hash,
        }

        unknown = [s for s in steps if s not in step_map]
        if unknown:
            raise ValueError(f"Unknown pipeline steps: {unknown}. Valid: {list(step_map.keys())}")

        for step in steps:
            df = step_map[step](df)
        return df

    def load_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate a load summary report after ETL pipeline run.

        Args:
            df: Final transformed DataFrame.

        Returns:
            Dict with record count, column list, null counts, and dedup stats.
        """
        return {
            "records_loaded": len(df),
            "columns": list(df.columns),
            "null_counts": df.isnull().sum().to_dict(),
            "dedup_removed": df.attrs.get("dedup_removed", 0),
            "has_row_hash": "row_hash" in df.columns,
        }

    def analyze(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run descriptive analysis and return summary metrics."""
        df = self.preprocess(df)
        result = {
            "total_records": len(df),
            "columns": list(df.columns),
            "missing_pct": (df.isnull().sum() / len(df) * 100).round(1).to_dict(),
        }
        numeric_df = df.select_dtypes(include="number")
        if not numeric_df.empty:
            result["summary_stats"] = numeric_df.describe().round(3).to_dict()
            result["totals"] = numeric_df.sum().round(2).to_dict()
            result["means"] = numeric_df.mean().round(3).to_dict()
        return result

    def run(self, filepath: str) -> Dict[str, Any]:
        """Full pipeline: load → validate → analyze."""
        df = self.load_data(filepath)
        self.validate(df)
        return self.analyze(df)

    def to_dataframe(self, result: Dict) -> pd.DataFrame:
        """Convert result dict to flat DataFrame for export."""
        rows = []
        for k, v in result.items():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    rows.append({"metric": f"{k}.{kk}", "value": vv})
            else:
                rows.append({"metric": k, "value": v})
        return pd.DataFrame(rows)
