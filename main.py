"""Minimal test script to verify fetchers work"""

from datetime import datetime, timedelta
from src.infrastructure.fetchers.ruonia import RuoniaFetcher
from src.infrastructure.fetchers.required_reserves import RequiredReservesFetcher


def main():
    print("=" * 80)
    print("Testing RUONIA Fetcher")
    print("=" * 80)

    ruonia_fetcher = RuoniaFetcher()
    to_date = datetime.now()
    from_date = to_date - timedelta(days=7)

    print(f"Fetching RUONIA data from {from_date.date()} to {to_date.date()}")
    ruonia_result = ruonia_fetcher.fetch(from_date, to_date)

    print(f"Status: {ruonia_result.status}")
    if ruonia_result.status_message:
        print(f"Message: {ruonia_result.status_message}")
    print(f"Records: {len(ruonia_result.data)}")

    if ruonia_result.data:
        print("\nFirst record:")
        row = ruonia_result.data[0]
        print(f"  DT: {row.dt}")
        print(f"  Rate: {row.rate}")
        print(f"  Volume: {row.volume}")
        print(f"  Deals: {row.deals_count}")

    print("\n" + "=" * 80)
    print("Testing RequiredReserves Fetcher")
    print("=" * 80)

    reserves_fetcher = RequiredReservesFetcher()
    print("Fetching Required Reserves data...")
    reserves_result = reserves_fetcher.fetch()

    print(f"Status: {reserves_result.status}")
    if reserves_result.status_message:
        print(f"Message: {reserves_result.status_message}")
    print(f"Records: {len(reserves_result.data)}")

    if reserves_result.data:
        print("\nFirst record:")
        row = reserves_result.data[0]
        print(f"  Period: {row.period_beining}")
        print(f"  Reserve Amount: {row.reserve_amount}")
        print(f"  Min Reserve: {row.min_reserve}")
        print(f"  Total Orgs: {row.total_organizations}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
