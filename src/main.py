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


    def incremental_load(
        self,
        df_new: pd.DataFrame,
        df_existing: pd.DataFrame,
        key_cols: Optional[List[str]] = None,
        updated_col: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Perform an incremental load: detect new and changed records (CDC pattern).

        Compares new data against existing using row hashes or updated_at timestamps.
        Returns inserts (new records) and updates (changed records).

        Args:
            df_new: Incoming DataFrame with fresh data.
            df_existing: Current DataFrame representing the target state.
            key_cols: Primary key columns for record matching.
            updated_col: Optional timestamp column; if provided, uses timestamp
                         comparison instead of hash comparison.

        Returns:
            Dict with:
                - inserts: DataFrame of new records not in existing
                - updates: DataFrame of records with changed non-key values
                - unchanged: count of unchanged records
                - total_new: count in df_new
                - total_existing: count in df_existing
        """
        df_new = self.preprocess(df_new)
        df_existing = self.preprocess(df_existing)
        keys = key_cols or self.dedup_keys

        if not keys:
            # Without keys, use full hash comparison
            df_new = self.add_row_hash(df_new)
            df_existing = self.add_row_hash(df_existing)
            existing_hashes = set(df_existing["row_hash"])
            inserts = df_new[~df_new["row_hash"].isin(existing_hashes)].drop(columns=["row_hash"])
            return {
                "inserts": inserts.reset_index(drop=True),
                "updates": pd.DataFrame(),
                "unchanged": len(df_new) - len(inserts),
                "total_new": len(df_new),
                "total_existing": len(df_existing),
            }

        available_keys = [k for k in keys if k in df_new.columns and k in df_existing.columns]
        if not available_keys:
            raise ValueError(f"Key columns {keys} not found in both DataFrames")

        merged = df_new.merge(
            df_existing, on=available_keys, how="left", suffixes=("_new", "_existing"), indicator=True
        )
        inserts = df_new[~df_new[available_keys[0]].isin(df_existing[available_keys[0]])]

        # Detect updates: records that exist but have changed values
        updates_list = []
        if updated_col and updated_col in df_new.columns and updated_col in df_existing.columns:
            common_keys = df_new[available_keys[0]].isin(df_existing[available_keys[0]])
            new_common = df_new[common_keys].set_index(available_keys[0])
            exist_common = df_existing.set_index(available_keys[0])
            for key_val in new_common.index:
                if key_val in exist_common.index:
                    new_ts = new_common.loc[key_val, updated_col]
                    exist_ts = exist_common.loc[key_val, updated_col]
                    if pd.to_datetime(new_ts) > pd.to_datetime(exist_ts):
                        updates_list.append(df_new[df_new[available_keys[0]] == key_val])

        updates = pd.concat(updates_list) if updates_list else pd.DataFrame()
        unchanged = len(df_new) - len(inserts) - len(updates)

        return {
            "inserts": inserts.reset_index(drop=True),
            "updates": updates.reset_index(drop=True) if not updates.empty else pd.DataFrame(),
            "unchanged": max(0, unchanged),
            "total_new": len(df_new),
            "total_existing": len(df_existing),
        }

    def data_quality_report(self, df: pd.DataFrame, rules: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Run data quality checks against configurable rules.

        Args:
            df: DataFrame to validate.
            rules: List of rule dicts with keys:
                - column: Column name to check
                - rule: 'not_null', 'unique', 'min', 'max', 'regex'
                - value: Threshold value for min/max/regex rules

        Returns:
            Dict with overall pass/fail, rule results list, and quality score (0-100).
        """
        df = self.preprocess(df)
        if rules is None:
            rules = [{"column": c, "rule": "not_null"} for c in df.columns]

        results = []
        for rule in rules:
            col = rule.get("column", "")
            r = rule.get("rule", "not_null")
            val = rule.get("value")

            if col not in df.columns:
                results.append({"column": col, "rule": r, "pass": False, "detail": "Column not found"})
                continue

            if r == "not_null":
                null_count = int(df[col].isnull().sum())
                results.append({"column": col, "rule": r, "pass": null_count == 0, "detail": f"{null_count} nulls"})
            elif r == "unique":
                dup_count = int(df[col].duplicated().sum())
                results.append({"column": col, "rule": r, "pass": dup_count == 0, "detail": f"{dup_count} duplicates"})
            elif r == "min" and val is not None:
                fails = int((pd.to_numeric(df[col], errors="coerce") < val).sum())
                results.append({"column": col, "rule": f"min>={val}", "pass": fails == 0, "detail": f"{fails} below min"})
            elif r == "max" and val is not None:
                fails = int((pd.to_numeric(df[col], errors="coerce") > val).sum())
                results.append({"column": col, "rule": f"max<={val}", "pass": fails == 0, "detail": f"{fails} above max"})
            elif r == "regex" and val is not None:
                import re
                fails = int(~df[col].astype(str).str.match(str(val)).all())
                results.append({"column": col, "rule": f"regex:{val}", "pass": fails == 0, "detail": f"{fails} non-matching"})

        passed = sum(1 for r in results if r["pass"])
        score = round(passed / len(results) * 100, 1) if results else 100.0
        return {
            "total_rules": len(results),
            "passed": passed,
            "failed": len(results) - passed,
            "quality_score": score,
            "overall_pass": score == 100.0,
            "rule_results": results,
        }
