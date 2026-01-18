#!/usr/bin/env python3
"""
NAV Data Validation Report Generator

Generates a validation report showing scheme counts per date in the final cleaned data.
Uses rolling 5-day average as baseline to detect incomplete fetches.
"""

import argparse
from datetime import datetime
from pathlib import Path
from config.settings import R2, Paths


def parse_args():
    parser = argparse.ArgumentParser(description='Generate NAV data validation report')
    parser.add_argument('--threshold', type=float, default=0.95,
                        help='Completeness threshold (default: 0.95 = 95%%)')
    parser.add_argument('--output', type=str, help='Output CSV path (optional)')
    parser.add_argument('--start-date', type=str, help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date filter (YYYY-MM-DD)')
    parser.add_argument('--window', type=int, default=5,
                        help='Rolling window size for baseline (default: 5 days)')
    return parser.parse_args()


def generate_validation_report(conn, data_path: str, threshold: float = 0.95,
                                window: int = 5, start_date: str = None, end_date: str = None):
    """
    Generate validation report for daily NAV data using rolling average baseline.

    Args:
        conn: DuckDB connection
        data_path: Path to nav_daily_growth_plan.parquet
        threshold: Completeness threshold (default 0.95)
        window: Rolling window size for calculating expected count
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)

    Returns:
        tuple: (summary_df, stats_dict)
    """
    # Build date filter clause
    date_filter = ""
    if start_date:
        date_filter += f" AND date >= '{start_date}'"
    if end_date:
        date_filter += f" AND date <= '{end_date}'"

    # Get scheme counts per date with rolling average
    summary = conn.sql(f"""
        WITH daily_counts AS (
            SELECT
                date,
                COUNT(DISTINCT scheme_code) as scheme_count
            FROM read_parquet('{data_path}')
            WHERE 1=1 {date_filter}
            GROUP BY date
        )
        SELECT
            date,
            scheme_count,
            CAST(AVG(scheme_count) OVER (
                ORDER BY date
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) AS INTEGER) as rolling_avg
        FROM daily_counts
        ORDER BY date
    """).df()

    if summary.empty:
        print("No data found in the parquet file")
        return None, None

    # Fill NaN rolling_avg (first few rows) with global median
    median_count = int(summary['scheme_count'].median())
    summary['rolling_avg'] = summary['rolling_avg'].fillna(median_count)
    summary['expected_count'] = summary['rolling_avg'].astype(int)

    # Calculate completeness ratio
    summary['completeness_ratio'] = summary['scheme_count'] / summary['expected_count']

    # Determine status - mark as incomplete only if significantly below expected
    # Allow for market holidays (< 50% is likely a holiday, not a failed fetch)
    def get_status(row):
        ratio = row['completeness_ratio']
        if ratio >= threshold:
            return 'COMPLETE'
        elif ratio < 0.5:
            return 'HOLIDAY'  # likely market holiday
        else:
            return 'INCOMPLETE'

    summary['status'] = summary.apply(get_status, axis=1)

    # Calculate summary statistics
    total_dates = len(summary)
    complete_dates = len(summary[summary['status'] == 'COMPLETE'])
    incomplete_dates = len(summary[summary['status'] == 'INCOMPLETE'])
    holiday_dates = len(summary[summary['status'] == 'HOLIDAY'])

    stats = {
        'total_dates': total_dates,
        'complete_dates': complete_dates,
        'incomplete_dates': incomplete_dates,
        'holiday_dates': holiday_dates,
        'threshold': threshold,
        'window': window
    }

    return summary, stats


def print_report(summary_df, stats: dict, show_all: bool = False):
    """Print formatted validation report to console."""
    report_date = datetime.now().strftime('%Y-%m-%d')

    print("=" * 70)
    print(f"NAV Data Validation Report - {report_date}")
    print(f"Using {stats['window']}-day rolling average as baseline")
    print("=" * 70)
    print()
    print(f"{'Date':<14} {'Schemes':>8} {'Expected':>9} {'Ratio':>8} {'Status':<12}")
    print("-" * 70)

    # Only show incomplete/interesting rows unless show_all
    rows_to_show = summary_df if show_all else summary_df[
        (summary_df['status'] != 'COMPLETE') |
        (summary_df.index >= len(summary_df) - 20)  # always show last 20 rows
    ]

    if not show_all and len(rows_to_show) < len(summary_df):
        print(f"  ... showing {len(rows_to_show)} of {len(summary_df)} dates (incomplete + last 20)")
        print("-" * 70)

    for _, row in rows_to_show.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])
        ratio_pct = f"{row['completeness_ratio']*100:.1f}%"
        status_marker = ''
        if row['status'] == 'INCOMPLETE':
            status_marker = ' <-- CHECK'
        elif row['status'] == 'HOLIDAY':
            status_marker = ' (holiday)'
        print(f"{date_str:<14} {row['scheme_count']:>8,} {row['expected_count']:>9,} "
              f"{ratio_pct:>8} {row['status']:<10}{status_marker}")

    print()
    print("=" * 70)
    print("Summary:")
    print(f"  - Total dates: {stats['total_dates']}")
    print(f"  - Complete (>={stats['threshold']*100:.0f}%): {stats['complete_dates']}")
    print(f"  - Incomplete (50-{stats['threshold']*100:.0f}%): {stats['incomplete_dates']}")
    print(f"  - Market holidays (<50%): {stats['holiday_dates']}")
    print("=" * 70)


def save_report(summary_df, output_path: Path):
    """Save validation report to CSV."""
    summary_df.to_csv(output_path, index=False)
    print(f"\nReport saved to: {output_path}")


def main():
    args = parse_args()

    try:
        r2 = R2()
        conn = r2.setup_connection()

        data_path = r2.get_full_path('clean', 'nav_daily_growth_plan')
        summary, stats = generate_validation_report(
            conn, data_path, args.threshold, args.window,
            args.start_date, args.end_date
        )

        if summary is None:
            return False

        print_report(summary, stats)

        # Save to CSV
        reports_dir = Paths.DATA_ROOT / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        if args.output:
            output_path = Path(args.output)
        else:
            date_str = datetime.now().strftime('%Y%m%d')
            output_path = reports_dir / f"nav_validation_{date_str}.csv"

        save_report(summary, output_path)

        # Return exit code based on truly incomplete dates (not holidays)
        if stats['incomplete_dates'] > 0:
            print(f"\nWarning: {stats['incomplete_dates']} dates need investigation")
            return False

        print("\nAll data complete!")
        return True

    except Exception as e:
        print(f"Error generating report: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
