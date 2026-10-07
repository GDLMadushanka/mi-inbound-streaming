#!/usr/bin/env python3
"""
Generate a large, deliberately messy orders CSV for exercising the streaming inbound.

    python3 generate-orders.py <output.csv> <target-size-GB>

The data is realistic enough to process for real: prices, quantities, discounts and
currencies that have to be combined and converted. Roughly 2% of rows are dirty on
purpose - blank discounts, non-numeric quantities, unknown currency codes, lower-case
status values, and product names containing commas and quotes - so the type coercion
and the validation path in the sequence both get exercised.
"""
import random
import sys

HEADER = ("order_id,order_ts,customer_id,customer_email,country,currency,"
          "sku,product_name,unit_price,quantity,discount_pct,status,channel\n")

COUNTRIES = [("US", "USD"), ("DE", "EUR"), ("FR", "EUR"), ("GB", "GBP"),
             ("JP", "JPY"), ("AU", "AUD"), ("CA", "CAD"), ("ES", "EUR")]
PRODUCTS = ['Wireless Mouse, Compact', 'Mechanical Keyboard', '27" 4K Monitor',
            'USB-C Hub, 7-port', 'Laptop Stand', 'Noise-Cancelling Headphones',
            'Webcam 1080p', 'Desk Lamp, Adjustable', 'External SSD 1TB',
            'Ergonomic Chair', 'Cable Organiser, 10-pack', 'Docking Station']
STATUSES = ["COMPLETED"] * 70 + ["PENDING"] * 15 + ["CANCELLED"] * 10 + ["REFUNDED"] * 5
CHANNELS = ["web", "mobile", "partner", "store"]


def quote(value):
    """RFC 4180 quoting - product names carry commas and inch marks."""
    if '"' in value or "," in value:
        return '"' + value.replace('"', '""') + '"'
    return value


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 1
    path, target = sys.argv[1], float(sys.argv[2]) * 1024 ** 3
    rnd = random.Random(20260923)
    written, order_id = 0, 0
    buf, buf_len = [], 0

    with open(path, "w", encoding="utf-8", newline="\n") as out:
        out.write(HEADER)
        written += len(HEADER)
        while written < target:
            order_id += 1
            country, currency = rnd.choice(COUNTRIES)
            price = round(rnd.uniform(4.99, 1899.0), 2)
            qty = rnd.randint(1, 25)
            discount = rnd.choice([0, 0, 0, 5, 10, 15, 20, 25])
            status = rnd.choice(STATUSES)
            product = rnd.choice(PRODUCTS)

            # ~2% dirty rows, one defect each, so every validation branch is hit.
            defect = rnd.random()
            if defect < 0.005:
                qty = "N/A"                  # non-numeric quantity
            elif defect < 0.010:
                discount = ""                # blank discount
            elif defect < 0.015:
                currency = "XYZ"             # unknown currency code
            elif defect < 0.020:
                status = status.lower()      # inconsistent casing

            row = (f"{order_id},2026-{rnd.randint(1,9):02d}-{rnd.randint(1,28):02d}"
                   f"T{rnd.randint(0,23):02d}:{rnd.randint(0,59):02d}:{rnd.randint(0,59):02d}Z,"
                   f"C{rnd.randint(1, 900000):06d},user{rnd.randint(1, 900000)}@example.com,"
                   f"{country},{currency},SKU-{rnd.randint(1000, 9999)},{quote(product)},"
                   f"{price},{qty},{discount},{status},{rnd.choice(CHANNELS)}\n")
            buf.append(row)
            buf_len += len(row)
            if buf_len >= 8 << 20:           # flush every 8 MB
                out.write("".join(buf))
                written += buf_len
                buf, buf_len = [], 0
        if buf:
            out.write("".join(buf))
            written += buf_len

    print(f"{path}: {written / 1024 ** 3:.2f} GB, {order_id:,} data rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
