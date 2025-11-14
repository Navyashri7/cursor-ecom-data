import sqlite3
import csv
from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
DATA_DIR = BASE / "data"
DB_PATH = BASE / "ecom.db"


TABLES = {
    "customers": ["customer_id", "first_name", "last_name", "email", "signup_date", "country"],
    "products": ["product_id", "name", "category", "price", "sku"],
    "orders": ["order_id", "customer_id", "order_date", "total_amount", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    "reviews": ["review_id", "product_id", "customer_id", "rating", "review_date", "comment"],
}


DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customers (
  customer_id TEXT PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  email TEXT,
  signup_date TEXT,
  country TEXT
);

CREATE TABLE IF NOT EXISTS products (
  product_id TEXT PRIMARY KEY,
  name TEXT,
  category TEXT,
  price REAL,
  sku TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_date TEXT,
  total_amount REAL,
  status TEXT,
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id TEXT PRIMARY KEY,
  order_id TEXT,
  product_id TEXT,
  quantity INTEGER,
  unit_price REAL,
  FOREIGN KEY(order_id) REFERENCES orders(order_id),
  FOREIGN KEY(product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS reviews (
  review_id TEXT PRIMARY KEY,
  product_id TEXT,
  customer_id TEXT,
  rating INTEGER,
  review_date TEXT,
  comment TEXT,
  FOREIGN KEY(product_id) REFERENCES products(product_id),
  FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);
"""


def create_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(DDL)
    conn.commit()


def load_csv(conn: sqlite3.Connection, table: str, path: Path, cols: list[str]) -> None:
    cur = conn.cursor()
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            vals = [row[c] for c in cols]
            rows.append(tuple(vals))
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    conn.commit()


def main() -> None:
    if not DATA_DIR.exists():
        raise SystemExit("Data folder missing")

    conn = sqlite3.connect(DB_PATH)
    create_db(conn)

    for table, cols in TABLES.items():
        csv_path = DATA_DIR / f"{table}.csv"
        print(f"Loading {csv_path}...")
        load_csv(conn, table, csv_path, cols)

    conn.close()
    print("Database created successfully at:", DB_PATH)


if __name__ == "__main__":
    main()


