from pathlib import Path

from src.bookkeeping import (
    load_raw_transactions,
    categorize_transactions,
    detect_recurring,
    compute_bookkeeping_summaries,
    build_pnl_table,
)


def main():
    input_path = Path("data/transactions_sample.xlsx")

    df = load_raw_transactions(input_path)
    df = categorize_transactions(df)
    df = detect_recurring(df)

    summaries = compute_bookkeeping_summaries(df, tax_rate=0.19)
    pnl = build_pnl_table(summaries)

    print("=== CARDS ===")
    for k, v in summaries["cards"].items():
        print(f"{k}: {v:,.2f}")

    print("\n=== P&L TABLE ===")
    print(pnl)

    print("\n=== BY CATEGORY ===")
    print(summaries["by_category"])

    print("\n=== MONTHLY SUMMARY (for charts) ===")
    print(summaries["monthly"].head())


if __name__ == "__main__":
    main()
