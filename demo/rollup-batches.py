#!/usr/bin/env python3
"""
Roll up the per-chunk JSONL the streaming inbound produced into one report.

    python3 rollup-batches.py <order-batches.jsonl>

The inbound emits one JSON object per chunk - counts, rejects, value by country and
any orders flagged for review. Summing those is a second pass over a file thousands of
times smaller than the source CSV, which is the point of aggregating in the sequence.
"""
import collections
import json
import sys


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 1

    chunks = rows = valid = 0
    rejects = collections.Counter()
    by_country = collections.Counter()
    flagged = []
    total_usd = 0.0

    with open(sys.argv[1], encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            b = json.loads(line)
            chunks += 1
            rows += b["rows"]
            valid += b["valid"]
            total_usd += b["value_usd"]
            rejects.update(b["rejected"])
            for country, amount in b["value_by_country"].items():
                by_country[country] += amount
            flagged.extend(b["flagged_for_review"])

    rejected = sum(rejects.values())
    print(f"chunks          {chunks:,}")
    print(f"rows            {rows:,}")
    print(f"valid           {valid:,}  ({valid / rows:.1%})")
    print(f"rejected        {rejected:,}  ({rejected / rows:.1%})")
    for reason, n in rejects.most_common():
        print(f"  {reason:<18} {n:,}")
    print(f"total value     USD {total_usd:,.2f}")
    print(f"flagged review  {len(flagged):,} orders")

    print("\nvalue by country")
    for country, amount in by_country.most_common():
        print(f"  {country}  USD {amount:>18,.2f}")

    print("\ntop 5 flagged orders")
    for o in sorted(flagged, key=lambda x: -x["net_usd"])[:5]:
        print(f"  #{o['order_id']:<9} {o['country']}  {o['currency']}  "
              f"qty {o['quantity']:>3}  USD {o['net_usd']:>10,.2f}  {o['channel']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
