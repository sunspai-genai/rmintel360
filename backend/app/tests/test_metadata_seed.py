import duckdb

from backend.app.synthetic_data.metadata_seed import seed_governance_metadata


def test_metadata_seed_creates_governed_catalog() -> None:
    with duckdb.connect(":memory:") as conn:
        seed_governance_metadata(conn)

        metric_count = conn.execute("SELECT COUNT(*) FROM metadata_metric").fetchone()[0]
        dimension_count = conn.execute("SELECT COUNT(*) FROM metadata_dimension").fetchone()[0]
        join_count = conn.execute("SELECT COUNT(*) FROM metadata_join_path").fetchone()[0]

        assert metric_count >= 10
        assert dimension_count >= 10
        assert join_count >= 10


def test_ambiguous_phrases_return_governed_candidates() -> None:
    with duckdb.connect(":memory:") as conn:
        seed_governance_metadata(conn)

        average_balance_candidates = conn.execute(
            """
            SELECT target_id
            FROM metadata_synonym
            WHERE phrase = 'average balance'
            ORDER BY confidence DESC
            """
        ).fetchall()
        segment_candidates = conn.execute(
            """
            SELECT target_id
            FROM metadata_synonym
            WHERE phrase = 'segment'
            ORDER BY confidence DESC
            """
        ).fetchall()

        assert ("metric.average_deposit_ledger_balance",) in average_balance_candidates
        assert ("metric.average_deposit_collected_balance",) in average_balance_candidates
        assert ("metric.average_loan_outstanding_balance",) in average_balance_candidates
        assert ("dimension.customer_segment",) in segment_candidates
        assert ("dimension.product_segment",) in segment_candidates


def test_transaction_amount_and_channel_are_governed_assets() -> None:
    with duckdb.connect(":memory:") as conn:
        seed_governance_metadata(conn)

        metric = conn.execute(
            """
            SELECT metric_id, base_table, required_columns
            FROM metadata_metric
            WHERE metric_id = 'metric.total_deposit_transaction_amount'
            """
        ).fetchone()
        dimension = conn.execute(
            """
            SELECT dimension_id, table_name, column_name
            FROM metadata_dimension
            WHERE dimension_id = 'dimension.transaction_channel'
            """
        ).fetchone()
        synonym = conn.execute(
            """
            SELECT target_id
            FROM metadata_synonym
            WHERE phrase = 'channel' AND target_type = 'dimension'
            """
        ).fetchone()

        assert metric == (
            "metric.total_deposit_transaction_amount",
            "fact_deposit_transaction",
            "amount,transaction_date,account_id",
        )
        assert dimension == ("dimension.transaction_channel", "fact_deposit_transaction", "channel")
        assert synonym == ("dimension.transaction_channel",)


def test_customer_name_is_governed_but_restricted() -> None:
    with duckdb.connect(":memory:") as conn:
        seed_governance_metadata(conn)

        dimension = conn.execute(
            """
            SELECT dimension_id, table_name, column_name
            FROM metadata_dimension
            WHERE dimension_id = 'dimension.customer_name'
            """
        ).fetchone()
        column = conn.execute(
            """
            SELECT pii_flag, restricted_flag
            FROM metadata_column
            WHERE table_name = 'dim_customer' AND column_name = 'customer_name'
            """
        ).fetchone()
        synonym = conn.execute(
            """
            SELECT target_id
            FROM metadata_synonym
            WHERE phrase = 'customer names' AND target_type = 'dimension'
            """
        ).fetchone()

        assert dimension == ("dimension.customer_name", "dim_customer", "customer_name")
        assert column == (True, True)
        assert synonym == ("dimension.customer_name",)
