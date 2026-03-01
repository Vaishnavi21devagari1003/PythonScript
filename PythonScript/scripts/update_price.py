import os, sys, json, time, threading, re
from typing import List, Tuple, Dict, Optional, Set

from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode

# Market data
from twelvedata import TDClient
import websocket              # pip install websocket-client
import requests               # for discovery against /stocks

# -------------------- Load env --------------------
load_dotenv()

TD_API_KEY = os.getenv("TWELVEDATA_API_KEY")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "FinTech_portfolio")

DISCOVER_EXCHANGES = [e.strip() for e in os.getenv("DISCOVER_EXCHANGES", "NASDAQ,NSE").split(",") if e.strip()]
DISCOVER_LIMIT_PER_EXCHANGE = int(os.getenv("DISCOVER_LIMIT_PER_EXCHANGE", "60"))
TARGET_TOTAL_FREE_SYMBOLS = int(os.getenv("TARGET_TOTAL_FREE_SYMBOLS", "30"))

# Respect Basic plan limits: ~8 requests/min
DISCOVERY_REQUEST_DELAY_SECONDS = int(os.getenv("DISCOVERY_REQUEST_DELAY_SECONDS", "8"))

# Runtime throttles
REQUEST_DELAY_SECONDS   = int(os.getenv("REQUEST_DELAY_SECONDS", "8"))
POLL_INTERVAL_SECONDS   = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
MAX_WS_SUBS             = int(os.getenv("MAX_WS_SUBS", "8"))
DEBUG = os.getenv("DEBUG", "true").lower() in {"1", "true", "yes"}

if not TD_API_KEY:
    print("ERROR: TWELVEDATA_API_KEY is not set.")
    sys.exit(1)

# -------------------- DB helpers --------------------
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("MySQL access denied.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"MySQL error: {err}")
        raise

def ensure_table():
    cnx = get_db_connection()
    try:
        sql = """
        CREATE TABLE IF NOT EXISTS stocks (
          symbol VARCHAR(64) NOT NULL PRIMARY KEY,
          name VARCHAR(255) NULL,
          sector VARCHAR(255) NULL,
          current_price DECIMAL(18,6) NULL,
          last_updated TIMESTAMP NULL
        )
        """
        with cnx.cursor() as cur:
            cur.execute(sql)
        cnx.commit()
    finally:
        cnx.close()

def fetch_symbols_from_db() -> List[Tuple[str, Optional[str], Optional[str]]]:
    cnx = get_db_connection()
    try:
        with cnx.cursor() as cur:
            cur.execute("SELECT symbol, name, sector FROM stocks")
            return list(cur.fetchall())
    finally:
        cnx.close()

def _upsert_price(cnx, symbol: str, name: Optional[str], sector: Optional[str], price: float):
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

def upsert_price_threadsafe(symbol: str, name: Optional[str], sector: Optional[str], price: float):
    cnx = get_db_connection()
    try:
        _upsert_price(cnx, symbol, name, sector, price)
    finally:
        cnx.close()

def upsert_stock_identity(symbol: str, name: Optional[str], sector: Optional[str]):
    cnx = get_db_connection()
    try:
        sql = """
        INSERT INTO stocks (symbol, name, sector, current_price, last_updated)
        VALUES (%s, %s, %s, NULL, NULL)
        ON DUPLICATE KEY UPDATE
          name   = COALESCE(VALUES(name), name),
          sector = COALESCE(VALUES(sector), sector)
        """
        with cnx.cursor() as cur:
            cur.execute(sql, (symbol, name, sector))
        cnx.commit()
    finally:
        cnx.close()

# -------------------- Symbol mapping --------------------
INDIAN_EXCHANGE_SUFFIX = ":NSE"
INDIAN_SYMBOLS = {"INFY", "TCS", "RELIANCE", "HDFCBANK", "SBIN", "WIPRO"}

def to_api_symbol(db_symbol: str) -> str:
    sym = db_symbol.strip().upper()
    return f"{sym}{INDIAN_EXCHANGE_SUFFIX}" if sym in INDIAN_SYMBOLS else sym

def ws_symbol_key(symbol: str) -> str:
    return symbol.split(":")[0].upper()

# -------------------- Discovery via /stocks --------------------
# Docs: https://api.twelvedata.com/stocks supports filtering by exchange/country/type (reference data).  [Support article]
# We'll use REST because older SDK builds may not expose TDClient.stocks().                        (Your log showed this)
# We further filter out test-looking symbols and non-common stocks.
# References: /stocks reference listing and filtering.                                             (Support center)
# (Citations in the chat answer)

def discover_from_exchange(exchange: str, limit_per_exchange: int) -> List[Dict]:
    url = "https://api.twelvedata.com/stocks"
    params = {
        "exchange": exchange,
        "apikey": TD_API_KEY,
        # You can add "type": "Common Stock" if supported in your plan/version:
        "type": "Common Stock",
        # Optional: "format": "JSON"
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        obj = resp.json()
        data = obj["data"] if isinstance(obj, dict) and "data" in obj else (obj if isinstance(obj, list) else [])
    except Exception as e:
        print(f"[discover_from_exchange] Error for {exchange}: {e}")
        return []

    # Remove test symbols or clearly invalid items
    def looks_ok(x: Dict) -> bool:
        sym = (x.get("symbol") or "").strip().upper()
        typ = (x.get("type") or "").lower()
        if not sym or "test" in sym or "test" in (x.get("name") or "").lower():
            return False
        if not typ.startswith("common"):
            return False
        # Avoid obviously synthetic NSE test symbols like "161NSETEST", leading digits only, etc.
        if re.match(r"^\d", sym) and "NSE" in sym:
            return False
        return True

    filtered = [x for x in data if looks_ok(x)]
    return filtered[:limit_per_exchange] if limit_per_exchange > 0 else filtered

# -------------------- Verify free-tier price availability --------------------
td = TDClient(apikey=TD_API_KEY)

def symbol_returns_price_now(api_symbol: str) -> bool:
    """
    Probe /price once. If it returns {"price": "..."} keep it.
    If it raises or returns plan errors -> reject.
    """
    try:
        obj = td.price(symbol=api_symbol).as_json()
        if isinstance(obj, dict) and "price" in obj:
            return True
        return False
    except Exception as e:
        # Typical error includes plan-upgrade hint; treat as not-free
        if DEBUG:
            print(f"[verify] {api_symbol} not free: {e}")
        return False

def build_free_universe(exchanges: List[str], limit_per_exchange: int, target_total: int) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    1) Pull from /stocks per exchange
    2) For each symbol, test via /price under current key
    3) Keep until we have target_total that actually return price
    """
    candidates: List[Tuple[str, Optional[str], Optional[str]]] = []
    for ex in exchanges:
        items = discover_from_exchange(ex, limit_per_exchange)
        if DEBUG:
            show = [i.get("symbol") for i in items[:10]]
            print(f"[discover] {ex}: {len(items)} items. Sample: {show} ...")

        for x in items:
            sym = (x.get("symbol") or "").strip().upper()
            name = x.get("name")
            # NSE suffix only for known Indian names you want real-time for
            api_sym = to_api_symbol(sym)
            if symbol_returns_price_now(api_sym):
                candidates.append((sym, name, None))
            else:
                # Skip non-free or restricted
                pass

            # respect rpm on Basic plan even during discovery probing
            time.sleep(DISCOVERY_REQUEST_DELAY_SECONDS)

            if len(candidates) >= target_total:
                break
        if len(candidates) >= target_total:
            break

    return candidates

# -------------------- REST poller --------------------
def fetch_price_rest(api_symbol: str) -> Optional[float]:
    try:
        data = td.price(symbol=api_symbol).as_json()
        if DEBUG:
            print(f"REST /price raw for {api_symbol}: {data}")
        if isinstance(data, dict) and "price" in data:
            return float(data["price"])
    except Exception as e:
        # Plan restriction or transient error
        print(f"REST error for {api_symbol}: {e}")
    return None

class RestPoller(threading.Thread):
    def __init__(self, symbols_set: Set[str], reverse_map: Dict[str, Tuple[str, Optional[str], Optional[str]]],
                 stop_event: threading.Event):
        super().__init__(daemon=True)
        self.symbols_set = symbols_set
        self.reverse_map = reverse_map
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            snapshot = list(self.symbols_set)
            if DEBUG:
                print(f"[REST Poller] polling: {snapshot}")
            for api_sym in snapshot:
                if self.stop_event.is_set():
                    break
                price = fetch_price_rest(api_sym)
                if price is not None:
                    key = ws_symbol_key(api_sym)
                    s, n, sec = self.reverse_map.get(key, (api_sym, api_sym, None))
                    upsert_price_threadsafe(s, n, sec, price)
                time.sleep(REQUEST_DELAY_SECONDS)
            slept = 0
            while slept < POLL_INTERVAL_SECONDS and not self.stop_event.is_set():
                time.sleep(1)
                slept += 1

# -------------------- WebSocket + Poll orchestration --------------------
def run():
    ensure_table()

    # 1) Build a free-tier universe
    universe = build_free_universe(
        exchanges=DISCOVER_EXCHANGES,
        limit_per_exchange=DISCOVER_LIMIT_PER_EXCHANGE,
        target_total=TARGET_TOTAL_FREE_SYMBOLS
    )

    if not universe:
        print("No free-tier symbols found right now. Try NASDAQ only or increase DISCOVER_LIMIT_PER_EXCHANGE.")
        return

    if DEBUG:
        print(f"[free-universe] {len(universe)} symbols (free): {[s for s,_,_ in universe[:15]]} ...")

    # 2) Ensure they exist in DB
    for s, n, sector in universe:
        upsert_stock_identity(s, n, sector)

    # 3) Maps
    api_symbols = [to_api_symbol(s) for s,_,_ in universe]
    reverse_map: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {}
    for s, n, sec in universe:
        api = to_api_symbol(s)
        reverse_map[api.upper()]     = (s, n, sec)
        reverse_map[ws_symbol_key(api)] = (s, n, sec)

    # 4) WS + REST split
    subs = api_symbols[:MAX_WS_SUBS]
    print("Subscribing to (WS):", subs)

    ws_url = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={TD_API_KEY}"

    # Poll the rest initially; also WS rejects will be added here
    fallback_symbols: Set[str] = set(api_symbols[MAX_WS_SUBS:])
    stop_event = threading.Event()

    poller = RestPoller(fallback_symbols, reverse_map, stop_event)
    poller.start()

    def on_message(ws, message):
        try:
            msg = json.loads(message)
            if DEBUG:
                print("WS message:", msg)

            ev = msg.get("event")
            if ev == "subscribe-status":
                fails = msg.get("fails", [])
                if fails:
                    rejected = []
                    for f in fails:
                        if isinstance(f, dict) and "symbol" in f:
                            rejected.append(f["symbol"])
                        elif isinstance(f, str):
                            rejected.append(f)
                    if rejected:
                        print("WS rejected; adding to REST polling:", rejected)
                        fallback_symbols.update(rejected)

            elif ev == "price":
                api_sym = msg.get("symbol")
                price_val = msg.get("price")
                if api_sym and price_val is not None:
                    try:
                        price = float(price_val)
                        key = ws_symbol_key(api_sym)
                        s, n, sec = reverse_map.get(key, (api_sym, api_sym, None))
                        upsert_price_threadsafe(s, n, sec, price)
                    except Exception as e:
                        print("Error handling price event:", e)

            elif ev == "heartbeat":
                pass
        except Exception as e:
            print("Error in on_message:", e)

    def on_open(ws):
        payload = {"action": "subscribe", "params": {"symbols": ",".join(subs)}}
        ws.send(json.dumps(payload))

        def heartbeat():
            while not stop_event.is_set():
                try:
                    ws.send(json.dumps({"action": "heartbeat"}))
                except Exception:
                    break
                time.sleep(10)
        threading.Thread(target=heartbeat, daemon=True).start()

    def on_error(ws, err): print("WS error:", err)
    def on_close(ws, a, b): print("WS closed")

    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message,
                                on_error=on_error, on_close=on_close)

    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("Interrupted, shutting down...")
    finally:
        stop_event.set()
        poller.join(timeout=5)

# -------------------- MAIN --------------------
if __name__ == "__main__":
    run()