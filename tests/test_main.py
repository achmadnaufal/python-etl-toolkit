"""Unit tests for ETLToolkit."""
import pytest
import pandas as pd
import sys
sys.path.insert(0, "/Users/johndoe/projects/python-etl-toolkit")
from src.main import ETLToolkit


@pytest.fixture
def raw_df():
    return pd.DataFrame({
        "Record ID": ["R001", "R002", "R003", "R001", "R004"],
        "Period": ["2026-01", "2026-01", "2026-02", "2026-01", "2026-02"],
        "Value USD": [1000.0, 2000.0, 1500.0, 1000.0, 3000.0],
        "Category": ["Carbon", "Biodiversity", "Carbon", "Carbon", None],
    })


@pytest.fixture
def etl():
    return ETLToolkit(config={"dedup_keys": ["record_id", "period"]})


class TestNormalizeColumns:
    def test_lowercase(self, etl, raw_df):
        result = etl.normalize_columns(raw_df)
        assert all(c == c.lower() for c in result.columns)

    def test_spaces_replaced(self, etl, raw_df):
        result = etl.normalize_columns(raw_df)
        assert all(" " not in c for c in result.columns)

    def test_expected_col_names(self, etl, raw_df):
        result = etl.normalize_columns(raw_df)
        assert "record_id" in result.columns
        assert "value_usd" in result.columns


class TestDeduplicate:
    def test_removes_duplicates(self, etl, raw_df):
        df = etl.normalize_columns(raw_df)
        result = etl.deduplicate(df, keys=["record_id", "period"])
        assert len(result) == 4  # R001 duplicate removed

    def test_dedup_count_tracked(self, etl, raw_df):
        df = etl.normalize_columns(raw_df)
        result = etl.deduplicate(df, keys=["record_id", "period"])
        assert result.attrs.get("dedup_removed") == 1


class TestCoerceTypes:
    def test_float_coercion(self, etl):
        df = pd.DataFrame({"amount": ["1000.5", "2000", "bad"]})
        result = etl.coerce_types(df, {"amount": "float"})
        assert pd.api.types.is_float_dtype(result["amount"])

    def test_str_coercion_strips(self, etl):
        df = pd.DataFrame({"name": ["  Alpha  ", "Beta"]})
        result = etl.coerce_types(df, {"name": "str"})
        assert result["name"][0] == "Alpha"

    def test_missing_column_ignored(self, etl):
        df = pd.DataFrame({"a": [1, 2]})
        result = etl.coerce_types(df, {"nonexistent": "float"})
        assert list(result.columns) == ["a"]


class TestAddRowHash:
    def test_hash_column_added(self, etl, raw_df):
        result = etl.add_row_hash(raw_df)
        assert "row_hash" in result.columns

    def test_hash_is_string(self, etl, raw_df):
        result = etl.add_row_hash(raw_df)
        assert result["row_hash"].dtype in (object, "str", "string")

    def test_identical_rows_same_hash(self, etl):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        result = etl.add_row_hash(df)
        assert result["row_hash"][0] == result["row_hash"][1]
        assert result["row_hash"][0] != result["row_hash"][2]


class TestTransformPipeline:
    def test_normalize_step(self, etl, raw_df):
        result = etl.transform_pipeline(raw_df, steps=["normalize"])
        assert "record_id" in result.columns

    def test_unknown_step_raises(self, etl, raw_df):
        with pytest.raises(ValueError, match="Unknown pipeline steps"):
            etl.transform_pipeline(raw_df, steps=["normalize", "mystery_step"])

    def test_full_pipeline(self, etl, raw_df):
        result = etl.transform_pipeline(raw_df, steps=["normalize", "deduplicate", "hash"])
        assert "row_hash" in result.columns
        assert len(result) < len(raw_df)


class TestIncrementalLoad:
    def test_detects_inserts(self, etl, raw_df):
        existing = raw_df.iloc[:2].copy()
        result = etl.incremental_load(raw_df, existing)
        assert isinstance(result["inserts"], pd.DataFrame)

    def test_no_inserts_when_same_data(self, etl, raw_df):
        result = etl.incremental_load(raw_df, raw_df)
        # Without keys, uses hash — same data should yield 0 inserts
        assert len(result["inserts"]) == 0

    def test_returns_dict_with_expected_keys(self, etl, raw_df):
        existing = raw_df.iloc[:1].copy()
        result = etl.incremental_load(raw_df, existing)
        assert all(k in result for k in ["inserts", "updates", "unchanged", "total_new", "total_existing"])

    def test_total_new_matches_input(self, etl, raw_df):
        existing = pd.DataFrame(columns=raw_df.columns)
        result = etl.incremental_load(raw_df, existing)
        assert result["total_new"] == len(raw_df)


class TestDataQualityReport:
    def test_returns_dict(self, etl, raw_df):
        result = etl.data_quality_report(raw_df)
        assert isinstance(result, dict)

    def test_quality_score_in_range(self, etl, raw_df):
        result = etl.data_quality_report(raw_df)
        assert 0 <= result["quality_score"] <= 100

    def test_has_rule_results(self, etl, raw_df):
        result = etl.data_quality_report(raw_df)
        assert isinstance(result["rule_results"], list)

    def test_custom_not_null_rule(self, etl, raw_df):
        rules = [{"column": "record_id", "rule": "not_null"}]
        result = etl.data_quality_report(etl.normalize_columns(raw_df), rules=rules)
        assert result["rule_results"][0]["rule"] == "not_null"

    def test_min_rule(self, etl, raw_df):
        df = etl.normalize_columns(raw_df)
        rules = [{"column": "value_usd", "rule": "min", "value": 0}]
        result = etl.data_quality_report(df, rules=rules)
        # All values are positive, so should pass
        assert result["rule_results"][0]["pass"] is True
