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

