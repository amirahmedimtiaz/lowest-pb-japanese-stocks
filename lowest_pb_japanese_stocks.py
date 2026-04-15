import pandas as pd
import yfinance as yf
import requests
import io
import time
import logging
import os
import smtplib
import json
from collections import deque
from datetime import datetime
from curl_cffi import requests as curl_requests
from email.message import EmailMessage
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Suppress yfinance's noisy logs
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ── yfinance cache location ────────────────────────────────────────────────────
YF_CACHE_DIR = os.path.join("cache", "yfinance")
os.makedirs(YF_CACHE_DIR, exist_ok=True)
yf.set_tz_cache_location(YF_CACHE_DIR)

# ── Global Session for Browser Impersonation ───────────────────────────────────
SESSION = curl_requests.Session(impersonate="chrome")

# ── Request Tracker ────────────────────────────────────────────────────────────
class RequestTracker:
    MINUTE_WINDOW   = 60
    HOURLY_WINDOW   = 3600
    MAX_HOURLY_WAIT = 90 * 60

    BURST_THRESHOLD  = 40
    HOURLY_THRESHOLD = 200

    def __init__(self):
        self._log: deque[float] = deque()

    def record(self):
        self._log.append(time.time())

    def _prune(self):
        cutoff = time.time() - self.HOURLY_WINDOW
        while self._log and self._log[0] < cutoff:
            self._log.popleft()

    def calls_in_last(self, seconds: float) -> int:
        cutoff = time.time() - seconds
        return sum(1 for t in self._log if t >= cutoff)

    def compute_wait(self, consecutive_429s: int) -> float | None:
        self._prune()
        now = time.time()
        hourly_count = self.calls_in_last(self.HOURLY_WINDOW)
        burst_count  = self.calls_in_last(self.MINUTE_WINDOW)

        if hourly_count >= self.HOURLY_THRESHOLD or consecutive_429s >= 4:
            if self._log:
                oldest_in_window = self._log[0]
                wait = (oldest_in_window + self.HOURLY_WINDOW) - now + 5
            else:
                wait = self.HOURLY_WINDOW
            if wait > self.MAX_HOURLY_WAIT:
                return None
            log.warning("Hourly rate limit: sleeping %.0fs", wait)
            return wait
        else:
            wait = min(10 * (2 ** (consecutive_429s - 1)), 300)
            log.warning("Burst rate limit: sleeping %ds", wait)
            return wait

TRACKER = RequestTracker()
_RL_KEYWORDS = ("429", "too many requests", "rate limit", "throttl")

def _is_rate_limit(exc: Exception) -> bool:
    return any(kw in str(exc).lower() for kw in _RL_KEYWORDS)

class _RetryTicker:
    def __init__(self, symbol):
        self._symbol = symbol
        self._ticker = yf.Ticker(symbol)

    def _fetch(self, attr):
        consecutive_429s = 0
        non_rl_attempts  = 0
        while True:
            TRACKER.record()
            try:
                result = getattr(self._ticker, attr)
                return result
            except Exception as e:
                if _is_rate_limit(e):
                    consecutive_429s += 1
                    wait = TRACKER.compute_wait(consecutive_429s)
                    if wait is None: return None
                    time.sleep(wait)
                else:
                    non_rl_attempts += 1
                    if non_rl_attempts >= 3: return None
                    time.sleep(5)

    @property
    def info(self):
        return self._fetch("info")
    
    @property
    def balance_sheet(self):
        return self._fetch("balance_sheet")

# ── Config ─────────────────────────────────────────────────────────────────────
JPX_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)
FINANCIAL_CACHE_FILE = os.path.join(CACHE_DIR, "stock_financials.json")
JPX_CACHE_FILE = os.path.join(CACHE_DIR, "jpx_master.csv")
LIMIT = None # Set to small number for testing

def load_cache(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_jpx_tickers():
    if os.path.exists(JPX_CACHE_FILE):
        mtime = os.path.getmtime(JPX_CACHE_FILE)
        if (time.time() - mtime) < 86400:
            log.info("Loading JPX list from cache...")
            df = pd.read_csv(JPX_CACHE_FILE)
            return list(zip(df['Ticker'], df['Name'], df['Sector']))

    log.info("Downloading JPX list...")
    try:
        resp = SESSION.get(JPX_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content))
        code_col = next(c for c in df.columns if "Local Code" in str(c))
        name_col = next(c for c in df.columns if "Name (English)" in str(c))
        sector_col = next(c for c in df.columns if "33 Sector(name)" in str(c))
        tickers = df[code_col].astype(str).str.strip()
        yf_tickers = [f"{t}.T" if len(t) >= 4 else t for t in tickers]
        result_df = pd.DataFrame({'Ticker': yf_tickers, 'Name': df[name_col], 'Sector': df[sector_col]})
        result_df.to_csv(JPX_CACHE_FILE, index=False)
        return list(zip(result_df['Ticker'], result_df['Name'], result_df['Sector']))
    except Exception as exc:
        log.error("Failed to fetch JPX list: %s", exc)
        return []

def analyze_stocks(ticker_info):
    cache = load_cache(FINANCIAL_CACHE_FILE)
    results = []
    total = len(ticker_info)
    now = time.time()
    
    # Constants for cache expiration
    PB_EXPIRY = 20 * 3600      # 20 hours (Daily)
    NCA_EXPIRY = 30 * 86400    # 30 days (Monthly)

    log.info("Analyzing %d stocks...", total)
    for i, (ticker, name, sector) in enumerate(ticker_info, 1):
        cached_data = cache.get(ticker, {})
        
        # Determine if we need to fetch NEW data
        needs_pb = True
        needs_nca = True
        
        if cached_data:
            last_pb_update = cached_data.get("pb_timestamp")
            last_nca_update = cached_data.get("nca_timestamp")
            
            # Migration/Cache Check
            if last_pb_update is not None and (now - last_pb_update) < PB_EXPIRY:
                needs_pb = False
            if last_nca_update is not None and (now - last_nca_update) < NCA_EXPIRY:
                needs_nca = False

        if needs_pb or needs_nca:
            log.info("[%d/%d] Fetching %s (%s%s)", i, total, ticker, 
                     "P/B" if needs_pb else "", 
                     ", NCA/BV" if needs_nca and needs_pb else "NCA/BV" if needs_nca else "")
            
            t_obj = _RetryTicker(ticker)
            
            # Fetch info for P/B and Liquidity Check
            info = t_obj.info
            if info and isinstance(info, dict):
                # 1. Check Liquidity / Listing Status
                vol = info.get("averageVolume10days") or info.get("averageVolume") or 0
                price = info.get("regularMarketPrice") or info.get("currentPrice") or 0
                
                # Exclude if no volume or price
                if vol < 100 or price <= 0:
                    cached_data["is_active"] = False
                else:
                    cached_data["is_active"] = True

                # 2. Fetch P/B
                if needs_pb:
                    pb = info.get("priceToBook")
                    if isinstance(pb, (int, float)):
                        cached_data["P/B Ratio"] = round(pb, 2)
                    else:
                        cached_data["P/B Ratio"] = 0
                    cached_data["pb_timestamp"] = now
            
            # Fetch NCA/BV and Equity Ratio from Balance Sheet
            if needs_nca:
                try:
                    bs = t_obj.balance_sheet
                    if bs is not None and not bs.empty:
                        col = bs.columns[0]
                        
                        # Working Capital (Net Current Assets)
                        wc = bs.loc["Working Capital", col] if "Working Capital" in bs.index else None
                        if wc is None and "Current Assets" in bs.index and "Current Liabilities" in bs.index:
                            wc = bs.loc["Current Assets", col] - bs.loc["Current Liabilities", col]
                        
                        # Book Value (Stockholders Equity)
                        bv = None
                        for label in ["Stockholders Equity", "Total Equity", "Common Stock Equity"]:
                            if label in bs.index:
                                bv = bs.loc[label, col]
                                break
                        
                        # Total Assets for Equity Ratio
                        total_assets = bs.loc["Total Assets", col] if "Total Assets" in bs.index else None

                        if isinstance(wc, (int, float)) and isinstance(bv, (int, float)) and bv != 0:
                            cached_data["NCA/BV Ratio"] = round(wc / bv, 3)
                            
                        if isinstance(total_assets, (int, float)) and isinstance(bv, (int, float)) and total_assets > 0:
                            cached_data["Equity Ratio %"] = round((bv / total_assets) * 100, 2)
                        
                        cached_data["nca_timestamp"] = now
                except Exception as e:
                    log.warning("Could not get balance sheet ratios for %s: %s", ticker, e)
            
            # Update cache
            cached_data.update({"Ticker": ticker, "Name": name, "Sector": sector})
            cache[ticker] = cached_data
            if i % 10 == 0: save_cache(FINANCIAL_CACHE_FILE, cache)
            time.sleep(0.5)
        
        pb = cached_data.get("P/B Ratio", 0)
        is_active = cached_data.get("is_active", True)
        
        if pb > 0 and is_active:
            results.append({
                "Ticker": ticker,
                "Name": name,
                "Sector": sector,
                "P/B Ratio": pb,
                "NCA/BV Ratio": cached_data.get("NCA/BV Ratio", "N/A"),
                "Equity Ratio %": cached_data.get("Equity Ratio %", "N/A")
            })

    save_cache(FINANCIAL_CACHE_FILE, cache)
    return results

def send_email(file_path, total_hits):
    sender = os.environ.get("EMAIL_SENDER")
    password = os.environ.get("EMAIL_PASSWORD")
    receiver = os.environ.get("EMAIL_RECEIVER")
    
    if not all([sender, password, receiver]):
        log.warning("Email not sent: one or more credentials (SENDER, PASSWORD, RECEIVER) missing.")
        return

    msg = EmailMessage()
    msg['Subject'] = "Daily Japan Stock Report: Top 20 Lowest P/B per Sector"
    msg['From'] = sender
    msg['To'] = receiver
    msg.set_content(f"Found {total_hits} stocks in the top 20 lowest P/B performers in each sector.\n\nCSV attached.")

    with open(file_path, 'rb') as f:
        msg.add_attachment(f.read(), maintype='application', subtype='csv', filename=file_path)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)
    log.info("Email sent to %s", receiver)

if __name__ == "__main__":
    t0 = time.time()
    ticker_info = get_jpx_tickers()
    if LIMIT: ticker_info = ticker_info[:LIMIT]

    if ticker_info:
        raw_results = analyze_stocks(ticker_info)
        if raw_results:
            df = pd.DataFrame(raw_results)
            # Filter, Sort and Group
            df_top20 = (
                df[df["P/B Ratio"] > 0]
                .sort_values(["Sector", "P/B Ratio"], ascending=[True, True])
                .groupby("Sector")
                .head(20)
            )
            
            fname = f"lowest_pb_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
            df_top20.to_csv(fname, index=False, encoding="utf-8-sig")
            
            # ── Daily Archive Logic ────────────────────────────────────────────
            reports_dir = "reports"
            os.makedirs(reports_dir, exist_ok=True)
            archive_name = os.path.join(reports_dir, fname)
            df_top20.to_csv(archive_name, index=False, encoding="utf-8-sig")
            log.info("Daily archive saved: %s", archive_name)

            send_email(fname, len(df_top20))
            log.info("Results saved to %s. Matches: %d", fname, len(df_top20))
    
    log.info("Total time: %.1f seconds", time.time() - t0)
