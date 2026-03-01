import os
import sys
import time
from typing import List, Tuple, Dict, Optional

from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
from twelvedata import TDClient

# -------------------- Load Environment --------------------
load_dotenv()

TD_API_KEY       = os.getenv("TWELVEDATA_API_KEY")
DB_HOST          = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT          = int(os.getenv("DB_PORT", "3306"))
DB_USER          = os.getenv("DB_USER", "root")
DB_PASSWORD      = os.getenv("DB_PASSWORD", "")
DB_NAME          = os.getenv("DB_NAME", "FinTech_portfolio")

# Free-plan friendly
REQUEST_DELAY_SECONDS = 8
DEBUG = True

if not TD_API_KEY:
    print("ERROR: TWELVEDATA_API_KEY is not set.")
    sys.exit(1)

# -------------------- DB Connection --------------------
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("MySQL access denied.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"MySQL error: {err}")
        raise


def fetch_symbols_from_db(cnx) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Returns list of (symbol, name, sector)
    """
    with cnx.cursor() as cur:
        cur.execute("SELECT symbol, name, sector FROM stocks")
        rows = list(cur.fetchall())
    return rows


def upsert_price(
    cnx,
    symbol: str,
    name: Optional[str],
    sector: Optional[str],
    price: float
):
    """
    Insert or update the stock record.
    - Inserts symbol, name, sector, current_price, last_updated
    - On duplicate:
        * current_price always updates
        * last_updated always updates
        * name updates only if provided non-NULL
        * sector updates only if provided non-NULL
    """
    sql = """
    INSERT INTO stocks (symbol, name, sector, current_price, last_updated)
    VALUES (%s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE
      current_price = VALUES(current_price),
      last_updated  = NOW(),
      name   = COALESCE(VALUES(name), name),
      sector = COALESCE(VALUES(sector), sector)
    """
    with cnx.cursor() as cur:
        cur.execute(sql, (symbol, name or symbol, sector, price))
    cnx.commit()

# -------------------- Twelve Data Symbol Logic --------------------

INDIAN_EXCHANGE_SUFFIX = ":NSE"

# Add other NSE tickers here if needed
INDIAN_SYMBOLS = {
    "INFY", "TCS", "RELIANCE", "HDFCBANK", "SBIN", "WIPRO",
}


def to_api_symbol(db_symbol: str) -> str:
    """
    Add :NSE only for Indian stock tickers.
    Leave US tickers unchanged.
    """
    sym = db_symbol.strip().upper()

    # NSE stocks → append ":NSE"
    if sym in INDIAN_SYMBOLS:
        return f"{sym}{INDIAN_EXCHANGE_SUFFIX}"

    # US stocks or unknown → let TwelveData resolve
    return sym


def fetch_price_single(td_client: TDClient, api_symbol: str) -> Dict[str, float]:
    endpoint = td_client.price(symbol=api_symbol)
    data = endpoint.as_json()

    if DEBUG:
        print(f"API raw response for {api_symbol}: {data}")

    # Valid response
    if isinstance(data, dict) and "price" in data:
        try:
            return {api_symbol: float(data["price"])}
        except Exception:
            return {}

    # Error response from API
    return {}


# -------------------- MAIN --------------------
def main():
    cnx = get_db_connection()
    try:
        db_rows = fetch_symbols_from_db(cnx)

        if not db_rows:
            print("No stocks found in database.")
            return

        if DEBUG:
            print("\nStocks retrieved from DB:")
            for sym, name, sector in db_rows:
                print(f"  {sym} - {name} - {sector}")

        td = TDClient(apikey=TD_API_KEY)
        updated = 0

        for symbol, name, sector in db_rows:
            api_symbol = to_api_symbol(symbol)

            print("\n---------------------------------")
            print(f"Requesting price for: {api_symbol}")

            try:
                prices = fetch_price_single(td, api_symbol)

                if prices:
                    price = prices[api_symbol]
                    print(f"Price received: {api_symbol} -> {price}")

                    upsert_price(cnx, symbol, name, sector, price)
                    updated += 1
                else:
                    print(f"No valid price data for {api_symbol}")

                # Respect free-tier limits
                time.sleep(REQUEST_DELAY_SECONDS)

            except Exception as e:
                print(f"Error fetching {api_symbol}: {e}")
                print("Waiting 60 seconds due to possible rate limit...")
                time.sleep(60)

        print(f"\nUpdated prices for {updated} symbols.")

    finally:
        cnx.close()


if __name__ == "__main__":
    main()