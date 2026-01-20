import pytest

pytestmark = pytest.mark.layer2


def test_sales_with_details_rowcount_matches_filtered_sales(dwh_engine, read_sql):
    """
    intermediate_sales_with_details should have the same number of rows
    as intermediate_filtered_sales (no row loss or duplication during joins).
    """

    details_count_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_sales_with_details
        """,
        dwh_engine
    )
    details_count = details_count_df.iloc[0]["cnt"]

    filtered_count_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_filtered_sales
        """,
        dwh_engine
    )
    filtered_count = filtered_count_df.iloc[0]["cnt"]

    print("intermediate_sales_with_details count:", details_count)
    print("intermediate_filtered_sales count:", filtered_count)

    assert details_count == filtered_count, (
        f"Rowcount mismatch: intermediate_sales_with_details={details_count}, "
        f"intermediate_filtered_sales={filtered_count}"
    )
