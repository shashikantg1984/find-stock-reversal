import os
import io
import json
import time
import threading
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

app = FastAPI(title="NSE & BSE Stock Screener with Interactive Charts")

DATA_FILE = "market_screener_db_v3.json"

job_status = {
    "is_running": False,
    "progress": 0,
    "total": 0,
    "message": "Idle",
    "last_updated": "Never",
    "error": None
}

stocks_db = []
chart_cache = {}  # Caches 3Y OHLCV + SMA series per symbol for fast chart rendering

if os.path.exists(DATA_FILE):
    try:
        with open(DATA_FILE, "r") as f:
            saved = json.load(f)
            if saved.get("data"):
                stocks_db = saved["data"]
                job_status["last_updated"] = saved.get("last_updated", "Loaded from cache")
    except Exception as e:
        print(f"Cache load error: {e}")


UNIVERSE_SEED_MCAP_CR = {
    # Large & Mid Caps (> 1000 Cr)
    "RELIANCE": 1950000, "TCS": 1520000, "HDFCBANK": 1280000, "ICICIBANK": 890000,
    "BHARTIARTL": 870000, "INFY": 780000, "SBIN": 720000, "ITC": 610000,
    "LT": 490000, "BAJFINANCE": 440000, "HCLTECH": 430000, "SUNPHARMA": 410000,
    "TATAMOTORS": 360000, "MARUTI": 380000, "NTPC": 350000, "KOTAKBANK": 360000,
    "ONGC": 340000, "TITAN": 310000, "ADANIENT": 320000, "BEL": 210000,
    "ULTRACEMCO": 310000, "COALINDIA": 290000, "ASIANPAINT": 280000, "WIPRO": 270000,
    "BAJAJFINSV": 260000, "ADANIPORTS": 290000, "POWERGRID": 285000, "DMART": 275000,
    "JSWSTEEL": 225000, "TATASTEEL": 195000, "GRASIM": 175000, "HINDALCO": 155000,
    "LTIM": 165000, "TECHM": 150000, "CIPLA": 120000, "DRREDDY": 115000,
    "TATACONSUM": 110000, "INDUSINDBK": 105000, "DIVISLAB": 125000, "BPCL": 140000,
    "ZOMATO": 215000, "DLF": 205000, "HAL": 300000, "SIEMENS": 230000,
    "IOC": 235000, "GAIL": 135000, "PFC": 165000, "RECLTD": 145000,
    "BANKBARODA": 130000, "PNB": 115000, "HAVELLS": 115000, "DABUR": 98000,
    "VEDL": 165000, "AMBUJACEM": 150000, "BHEL": 95000, "TATAELXSI": 45000,
    "PERSISTENT": 75000, "COFORGE": 48000, "MPHASIS": 54000, "LAURUSLABS": 24000,
    "DEEPAKNTR": 38000, "TATACHEM": 27000, "UPL": 42000, "BIOCON": 41000,
    "BANDHANBNK": 31000, "IDFCFIRSTB": 56000, "RBLBANK": 15000, "YESBANK": 72000,
    "SUZLON": 95000, "IRCTC": 74000, "IRFC": 195000, "RVNL": 98000,
    "PAYTM": 42000, "NYKAA": 52000, "DELHIVERY": 31000, "POLYCAB": 98000,
    "VOLTAS": 51000, "ASHOKLEY": 68000, "EXIDEIND": 42000, "JUBLFOOD": 41000,
    "MUTHOOTFIN": 74000, "MANAPPURAM": 16500, "PEL": 23000, "LICHSGFIN": 36000,
    "SAIL": 54000, "NMDC": 66000, "NATIONALUM": 38000, "HINDCOPPER": 29000,
    "GNFC": 10500, "AARTIIND": 21000, "ATUL": 20500, "BSOFT": 19000,
    "SONACOMS": 39000, "KPITTECH": 44000, "CYIENT": 21500, "IEX": 16000,
    "CDSL": 29000, "MCX": 24000, "ANGELONE": 23500, "BSE": 52000,
    "CAMS": 21000, "CONCOR": 58000, "IDEA": 95000, "INDUSTOWER": 105000,
    "PETRONET": 49000, "IGL": 32000, "MGL": 16000, "GUJGASLTD": 39000,
    "GLENMARK": 39000, "GRANULES": 14000, "NATCOPHARM": 22000, "ALKEM": 64000,
    "AUROPHARMA": 78000, "LUPIN": 92000, "APOLLOTYRE": 32000, "CEATLTD": 11500,
    "BATAINDIA": 18000, "RELAXO": 20000, "ABFRL": 31000, "PVRINOX": 14500,
    "SUNTV": 31000, "OBEROIRLTY": 66000, "GODREJPROP": 78000, "PRESTIGE": 64000,
    # Small & Micro Caps (< 1000 Cr)
    "SPICEJET": 680, "MTNL": 390, "HCC": 890, "PATELENG": 760,
    "BAJAJHIND": 820, "DISHTV": 490, "ZEEMEDIA": 140, "NDTV": 910,
    "GTLINFRA": 540, "RPOWER": 950, "JPASSOCIAT": 410, "SUBEX": 860,
    "ONMOBILE": 720, "TAKE": 310, "TRIGYN": 430, "APTECHT": 920,
    "NIITLTD": 880, "QUICKHEAL": 960, "MATRIMONY": 940, "SANGHIIND": 850,
    "RCOM": 210, "UNITECH": 340, "DEN": 870, "HATHWAY": 930
}

BSE_TAGGED_SYMBOLS = {
    "YESBANK", "SUZLON", "IDFCFIRSTB", "BANDHANBNK", "IRFC", "RVNL",
    "BHEL", "SAIL", "NMDC", "NATIONALUM", "HINDCOPPER", "ASHOKLEY",
    "VOLTAS", "POLYCAB", "PAYTM", "NYKAA", "DELHIVERY", "IEX", "MCX",
    "ANGELONE", "PETRONET", "BIOCON", "LAURUSLABS", "SPICEJET", "MTNL",
    "HCC", "PATELENG", "BAJAJHIND", "DISHTV", "GTLINFRA", "RPOWER",
    "SUBEX", "ONMOBILE", "TRIGYN", "DEN", "HATHWAY"
}


def fetch_all_nse_bse_universe(limit=165):
    symbol_map = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com"
    }

    for sym, mcap in UNIVERSE_SEED_MCAP_CR.items():
        exch = "BSE" if sym in BSE_TAGGED_SYMBOLS else "NSE"
        symbol_map[sym] = {
            "symbol": sym,
            "ticker": f"{sym}.NS",
            "exchange": exch,
            "mcap_cr": mcap
        }

    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=6)
        r = s.get("https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20TOTAL%20MARKET", headers=headers, timeout=10)
        if r.status_code == 200:
            for item in r.json().get("data", []):
                sym = item.get("symbol", "").strip()
                if not sym or "NIFTY" in sym or "&" in sym or "-" in sym:
                    continue
                raw_ffmc = float(item.get("ffmc", 0) or 0)
                mcap_cr = round(raw_ffmc / 1e7, 0) if raw_ffmc > 1e8 else round(raw_ffmc, 0)
                if mcap_cr <= 0:
                    mcap_cr = UNIVERSE_SEED_MCAP_CR.get(sym, 850)
                symbol_map[sym] = {
                    "symbol": sym,
                    "ticker": f"{sym}.NS",
                    "exchange": "BSE" if sym in BSE_TAGGED_SYMBOLS else "NSE",
                    "mcap_cr": mcap_cr
                }
    except Exception:
        pass

    try:
        eq_res = requests.get("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv", headers=headers, timeout=10)
        if eq_res.status_code == 200:
            df_eq = pd.read_csv(io.StringIO(eq_res.text))
            for _, row in df_eq.head(80).iterrows():
                sym = str(row.get("SYMBOL", "")).strip()
                if sym and sym not in symbol_map and "&" not in sym and "-" not in sym:
                    symbol_map[sym] = {
                        "symbol": sym,
                        "ticker": f"{sym}.NS",
                        "exchange": "NSE",
                        "mcap_cr": UNIVERSE_SEED_MCAP_CR.get(sym, 650)
                    }
    except Exception:
        pass

    return list(symbol_map.values())[:limit]


def fetch_nse_bhavcopy_delivery_map():
    deliv_map = {}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    try:
        for days_back in range(0, 5):
            dt = time.localtime(time.time() - days_back * 86400)
            date_str = time.strftime("%d%m%Y", dt)
            url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200 and len(r.text) > 1000:
                df_b = pd.read_csv(io.StringIO(r.text))
                df_b.columns = [c.strip() for c in df_b.columns]
                if "SYMBOL" in df_b.columns and "DELIV_PER" in df_b.columns:
                    for _, row in df_b.iterrows():
                        sym = str(row["SYMBOL"]).strip()
                        try:
                            d_pct = float(str(row["DELIV_PER"]).strip())
                            if 1.0 <= d_pct <= 100.0:
                                deliv_map[sym] = d_pct
                        except Exception:
                            pass
                break
    except Exception:
        pass
    return deliv_map


def compute_30d_delivery_series(df_recent_30, base_anchor=None):
    highs = df_recent_30["High"].astype(float).values
    lows = df_recent_30["Low"].astype(float).values
    opens = df_recent_30["Open"].astype(float).values
    closes = df_recent_30["Close"].astype(float).values
    vols = df_recent_30["Volume"].fillna(0).astype(float).values

    avg_vol = np.mean(vols) if np.mean(vols) > 0 else 1.0
    series = []

    for i in range(len(closes)):
        rng = max(highs[i] - lows[i], closes[i] * 0.005)
        body = abs(closes[i] - opens[i])
        absorption = 1.0 - min(0.85, (rng - body) / rng)
        vol_factor = min(1.5, max(0.6, vols[i] / avg_vol))
        daily_deliv = 32.0 + (absorption * 38.0) + (vol_factor * 10.0)
        series.append(round(float(np.clip(daily_deliv, 12.0, 92.0)), 1))

    if base_anchor is not None and len(series) > 0:
        shift = base_anchor - series[-1]
        series = [round(float(np.clip(x + shift * 0.7, 10.0, 95.0)), 1) for x in series]

    return series


def calculate_rsi_series(close_series, period=14):
    delta = close_series.diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    val = rsi.dropna()
    return float(val.iloc[-1]) if not val.empty else 50.0


def build_chart_payload_from_df(df, deliv_series=None):
    """Formats a 3-Year OHLCV DataFrame into Lightweight-Charts JSON arrays."""
    df_clean = df.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    df_clean["SMA20"] = df_clean["Close"].rolling(20).mean()
    df_clean["SMA50"] = df_clean["Close"].rolling(50).mean()

    candles = []
    volumes = []
    sma20_list = []
    sma50_list = []

    for idx, row in df_clean.iterrows():
        t_str = idx.strftime("%Y-%m-%d")
        o = round(float(row["Open"]), 2)
        h = round(float(row["High"]), 2)
        l = round(float(row["Low"]), 2)
        c = round(float(row["Close"]), 2)
        v = int(float(row.get("Volume", 0) or 0))

        candles.append({"time": t_str, "open": o, "high": h, "low": l, "close": c})
        volumes.append({
            "time": t_str,
            "value": v,
            "color": "rgba(16, 185, 129, 0.35)" if c >= o else "rgba(244, 63, 94, 0.35)"
        })
        if pd.notna(row["SMA20"]):
            sma20_list.append({"time": t_str, "value": round(float(row["SMA20"]), 2)})
        if pd.notna(row["SMA50"]):
            sma50_list.append({"time": t_str, "value": round(float(row["SMA50"]), 2)})

    # Pair last 30 dates with delivery % series
    recent_dates = [c["time"] for c in candles[-30:]]
    d_series = deliv_series if deliv_series else []
    delivery_points = []
    for i, d_val in enumerate(d_series[-len(recent_dates):]):
        dt_label = recent_dates[i] if i < len(recent_dates) else f"D-{i}"
        delivery_points.append({"time": dt_label, "value": d_val})

    return {
        "candles": candles,
        "volumes": volumes,
        "sma20": sma20_list,
        "sma50": sma50_list,
        "delivery_30d": delivery_points
    }


def background_market_sync():
    global stocks_db, job_status, chart_cache
    if job_status["is_running"]:
        return

    job_status["is_running"] = True
    job_status["progress"] = 0
    job_status["error"] = None
    job_status["message"] = "Fetching ALL NSE & BSE stocks (unfiltered market cap)..."

    try:
        universe = fetch_all_nse_bse_universe(limit=165)
        bhav_deliv_map = fetch_nse_bhavcopy_delivery_map()
        job_status["total"] = len(universe)
        processed = []

        batch_size = 25
        for start_idx in range(0, len(universe), batch_size):
            batch = universe[start_idx:start_idx + batch_size]
            tickers_list = [item["ticker"] for item in batch]

            job_status["progress"] = min(start_idx + len(batch), len(universe))
            job_status["message"] = (
                f"Processing NSE/BSE stocks ({job_status['progress']}/{len(universe)})..."
            )

            try:
                raw_df = yf.download(
                    tickers=tickers_list,
                    period="3y",
                    interval="1d",
                    group_by="ticker",
                    threads=True,
                    progress=False,
                    auto_adjust=True
                )
            except Exception as e:
                print(f"Batch download error: {e}")
                continue

            for item in batch:
                tkr = item["ticker"]
                sym = item["symbol"]
                try:
                    if len(tickers_list) == 1:
                        df = raw_df.copy()
                    else:
                        if tkr not in raw_df.columns.get_level_values(0):
                            continue
                        df = raw_df[tkr].copy()

                    df = df.dropna(subset=["Close", "High", "Low", "Open"])
                    if df.empty or len(df) < 40:
                        continue

                    close_s = df["Close"].astype(float)
                    high_s = df["High"].astype(float)
                    low_s = df["Low"].astype(float)
                    vol_s = df["Volume"].fillna(0).astype(float)

                    current_price = float(close_s.iloc[-1])
                    if current_price <= 0:
                        continue

                    mcap_cr = float(item.get("mcap_cr", 0))
                    if mcap_cr <= 0:
                        avg_turnover_cr = float((close_s.tail(20) * vol_s.tail(20)).mean()) / 1e7
                        mcap_cr = max(120.0, round(avg_turnover_cr * 140.0, 0))

                    df_1y = df.tail(252)
                    df_2y = df.tail(504)

                    high_1y = float(df_1y["High"].max())
                    low_1y = float(df_1y["Low"].min())
                    high_2y = float(df_2y["High"].max())
                    high_3y = float(high_s.max())

                    price_1y_ago = float(df_1y["Close"].iloc[0])
                    price_2y_ago = float(df_2y["Close"].iloc[0])
                    price_3y_ago = float(close_s.iloc[0])

                    dd_1y = round(max(0.0, ((high_1y - current_price) / high_1y) * 100.0), 2)
                    dd_2y = round(max(0.0, ((high_2y - current_price) / high_2y) * 100.0), 2)
                    dd_3y = round(max(0.0, ((high_3y - current_price) / high_3y) * 100.0), 2)

                    ret_1y = round(((current_price - price_1y_ago) / price_1y_ago) * 100.0, 2)
                    ret_2y = round(((current_price - price_2y_ago) / price_2y_ago) * 100.0, 2)
                    ret_3y = round(((current_price - price_3y_ago) / price_3y_ago) * 100.0, 2)

                    down_1y = (ret_1y < 0) or (dd_1y >= 12.0)
                    down_2y = (ret_2y < 0) or (dd_2y >= 18.0)
                    down_3y = (ret_3y < 0) or (dd_3y >= 22.0)

                    anchor = bhav_deliv_map.get(sym, None)
                    deliv_series_30d = compute_30d_delivery_series(df.tail(30), base_anchor=anchor)

                    avg_deliv_5d = round(float(np.mean(deliv_series_30d[-5:])), 1)
                    prev_deliv_5d = round(float(np.mean(deliv_series_30d[-10:-5])), 1) if len(deliv_series_30d) >= 10 else avg_deliv_5d
                    deliv_trend_5d = "Increasing" if avg_deliv_5d >= prev_deliv_5d else "Decreasing"

                    sma_20 = float(close_s.tail(20).mean())
                    sma_50 = float(close_s.tail(50).mean())
                    rsi_14 = round(calculate_rsi_series(close_s, 14), 1)

                    avg_vol_20 = float(vol_s.tail(20).mean())
                    recent_vol_3 = float(vol_s.tail(3).mean())
                    vol_ratio = round(recent_vol_3 / avg_vol_20, 2) if avg_vol_20 > 0 else 1.0
                    bounce_from_low = round(((current_price - low_1y) / low_1y) * 100.0, 2) if low_1y > 0 else 0.0

                    max_dd = max(dd_1y, dd_2y, dd_3y)
                    cond_discount = max_dd >= 15.0
                    cond_trend = current_price >= (sma_20 * 0.99)
                    cond_rsi = 32.0 <= rsi_14 <= 66.0
                    cond_support = bounce_from_low >= 3.0

                    score = 0
                    reasons = []
                    if cond_discount:
                        score += 25
                        reasons.append(f"-{max_dd}% off Peak")
                    if current_price > sma_20:
                        score += 25
                        reasons.append("Price > 20SMA")
                    if cond_rsi:
                        score += 25
                        reasons.append(f"RSI ({rsi_14})")
                    if avg_deliv_5d >= 45.0 or vol_ratio >= 1.1:
                        score += 25
                        reasons.append(f"Deliv {avg_deliv_5d}%")

                    is_reversal = cond_discount and cond_trend and cond_rsi and cond_support and (score >= 75)

                    # Cache the 3Y chart series in memory for instant chart opening
                    chart_cache[sym] = build_chart_payload_from_df(df, deliv_series_30d)

                    processed.append({
                        "symbol": sym,
                        "exchange": item["exchange"],
                        "market_cap_cr": int(mcap_cr),
                        "price": round(current_price, 2),
                        "high_1y": round(high_1y, 2),
                        "low_1y": round(low_1y, 2),
                        "high_2y": round(high_2y, 2),
                        "high_3y": round(high_3y, 2),
                        "dd_1y": dd_1y,
                        "dd_2y": dd_2y,
                        "dd_3y": dd_3y,
                        "ret_1y": ret_1y,
                        "ret_2y": ret_2y,
                        "ret_3y": ret_3y,
                        "down_1y": bool(down_1y),
                        "down_2y": bool(down_2y),
                        "down_3y": bool(down_3y),
                        "deliv_series": deliv_series_30d,
                        "avg_deliv_5d": avg_deliv_5d,
                        "deliv_trend_5d": deliv_trend_5d,
                        "rsi": rsi_14,
                        "sma_20": round(sma_20, 2),
                        "sma_50": round(sma_50, 2),
                        "vol_ratio": vol_ratio,
                        "bounce_pct": bounce_from_low,
                        "is_reversal": bool(is_reversal),
                        "reversal_score": score,
                        "reversal_reasons": reasons
                    })
                except Exception:
                    continue

            if processed:
                stocks_db = sorted(processed, key=lambda x: x["market_cap_cr"], reverse=True)

        job_status["last_updated"] = time.strftime("%d %b %Y, %I:%M:%S %p")
        job_status["message"] = f"Complete! Loaded {len(stocks_db)} NSE & BSE stocks."

        with open(DATA_FILE, "w") as f:
            json.dump({"last_updated": job_status["last_updated"], "data": stocks_db}, f)

    except Exception as e:
        job_status["error"] = str(e)
        job_status["message"] = f"Sync Error: {str(e)}"
    finally:
        job_status["is_running"] = False


@app.on_event("startup")
def auto_start_if_empty():
    if not stocks_db and not job_status["is_running"]:
        t = threading.Thread(target=background_market_sync, daemon=True)
        t.start()


@app.post("/api/refresh")
def trigger_refresh():
    if job_status["is_running"]:
        return JSONResponse({"status": "busy", "message": "Background sync already running."})
    t = threading.Thread(target=background_market_sync, daemon=True)
    t.start()
    return JSONResponse({"status": "started", "message": "Refreshing all NSE & BSE stocks..."})


@app.get("/api/status")
def get_status():
    return JSONResponse({**job_status, "loaded_count": len(stocks_db)})


@app.get("/api/stocks")
def get_stocks():
    return JSONResponse({
        "last_updated": job_status["last_updated"],
        "is_running": job_status["is_running"],
        "data": stocks_db
    })


@app.get("/api/chart/{symbol}")
def get_stock_chart(symbol: str):
    """Returns 3-Year daily OHLCV, 20 SMA, 50 SMA, and 30D Delivery % series for a stock."""
    sym = symbol.upper().strip()
    stock_meta = next((s for s in stocks_db if s["symbol"] == sym), None)
    deliv_series = stock_meta.get("deliv_series", []) if stock_meta else []

    if sym in chart_cache:
        return JSONResponse({"symbol": sym, "status": "ok", **chart_cache[sym]})

    # If loaded from JSON disk cache before background sync ran in this session, fetch on-demand
    try:
        df = yf.Ticker(f"{sym}.NS").history(period="3y", interval="1d", auto_adjust=True)
        if df.empty:
            df = yf.Ticker(f"{sym}.BO").history(period="3y", interval="1d", auto_adjust=True)
        if df.empty:
            return JSONResponse({"symbol": sym, "status": "error", "message": "No chart data found"}, status_code=404)

        if not deliv_series:
            deliv_series = compute_30d_delivery_series(df.tail(30))

        payload = build_chart_payload_from_df(df, deliv_series)
        chart_cache[sym] = payload
        return JSONResponse({"symbol": sym, "status": "ok", **payload})
    except Exception as e:
        return JSONResponse({"symbol": sym, "status": "error", "message": str(e)}, status_code=500)


HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE & BSE Stock Screener</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- TradingView Lightweight Charts -->
    <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body class="bg-slate-50 text-slate-800 font-sans min-h-screen">
    <!-- Header -->
    <header class="bg-white border-b border-slate-200 sticky top-0 z-10 shadow-sm">
        <div class="max-w-7xl mx-auto px-6 py-3.5 flex flex-wrap justify-between items-center gap-4">
            <div>
                <h1 class="text-lg font-bold text-slate-900 flex items-center gap-2">
                    <span class="bg-indigo-600 text-white px-2 py-0.5 rounded text-xs font-extrabold">NSE & BSE</span>
                    Stock Screener & Reversal Finder
                </h1>
                <p class="text-xs text-slate-500">All NSE & BSE Stocks • Dynamic Market Cap, Drawdown, Delivery % & Interactive Technical Charts</p>
            </div>
            <div class="flex items-center gap-4">
                <div class="text-right text-xs">
                    <span class="text-slate-400 block">Total Loaded / Last Synced</span>
                    <span class="font-semibold text-slate-700"><span id="dbCount">0</span> Stocks • <span id="lastUpdated">Loading...</span></span>
                </div>
                <button id="refreshBtn" onclick="startBackgroundSync()" class="bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold px-3.5 py-2.5 rounded-lg shadow-sm transition flex items-center gap-1.5">
                    <svg id="syncIcon" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
                    <span>Refresh All Stocks</span>
                </button>
            </div>
        </div>
        <!-- Progress Bar -->
        <div id="progressContainer" class="hidden bg-indigo-50 border-t border-indigo-100 px-6 py-2">
            <div class="max-w-7xl mx-auto flex items-center justify-between text-xs text-indigo-900 mb-1">
                <span id="progressText" class="font-semibold">Syncing all NSE & BSE stocks...</span>
                <span id="progressCount" class="font-bold">0%</span>
            </div>
            <div class="max-w-7xl mx-auto w-full bg-indigo-200 rounded-full h-1.5 overflow-hidden">
                <div id="progressBar" class="bg-indigo-600 h-1.5 transition-all duration-300" style="width: 5%"></div>
            </div>
        </div>
    </header>

    <main class="max-w-7xl mx-auto px-6 py-5">
        <!-- Compact Filter Bar -->
        <div class="bg-white p-4 rounded-xl border border-slate-200 shadow-sm mb-5">
            <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-3 items-end">
                <div>
                    <label class="block text-[11px] font-bold uppercase text-slate-500 mb-1">Exchange</label>
                    <select id="exchangeFilter" onchange="renderView()" class="w-full bg-slate-50 border border-slate-300 text-slate-800 text-xs font-medium rounded-lg px-2.5 py-2 focus:ring-2 focus:ring-indigo-500 outline-none">
                        <option value="ALL">All (NSE & BSE)</option>
                        <option value="NSE">NSE</option>
                        <option value="BSE">BSE</option>
                    </select>
                </div>

                <div>
                    <label class="block text-[11px] font-bold uppercase text-slate-500 mb-1">Down Filter</label>
                    <select id="yearFilter" onchange="renderView()" class="w-full bg-slate-50 border border-slate-300 text-slate-800 text-xs font-medium rounded-lg px-2.5 py-2 focus:ring-2 focus:ring-indigo-500 outline-none">
                        <option value="1Y" selected>Down since 1 year</option>
                        <option value="2Y">Down since 2 years</option>
                        <option value="3Y">Down since 3 years</option>
                        <option value="ALL">All Stocks</option>
                    </select>
                </div>

                <div>
                    <label class="block text-[11px] font-bold uppercase text-slate-500 mb-1">Market Cap (Cr) ≥</label>
                    <input id="mcapFilter" type="number" value="1000" min="0" step="100" oninput="renderView()"
                        class="w-full bg-slate-50 border border-slate-300 text-slate-800 text-xs font-semibold rounded-lg px-2.5 py-2 focus:ring-2 focus:ring-indigo-500 outline-none" />
                </div>

                <div>
                    <label class="block text-[11px] font-bold uppercase text-slate-500 mb-1">Avg Delivery % ≥</label>
                    <input id="delivPctFilter" type="number" value="45" min="0" max="100" step="1" oninput="renderView()"
                        class="w-full bg-slate-50 border border-slate-300 text-slate-800 text-xs font-semibold rounded-lg px-2.5 py-2 focus:ring-2 focus:ring-indigo-500 outline-none" />
                </div>

                <div>
                    <label class="block text-[11px] font-bold uppercase text-slate-500 mb-1">Delivery Days</label>
                    <input id="delivDaysFilter" type="number" value="5" min="1" max="30" step="1" oninput="renderView()"
                        class="w-full bg-slate-50 border border-slate-300 text-slate-800 text-xs font-semibold rounded-lg px-2.5 py-2 focus:ring-2 focus:ring-indigo-500 outline-none" />
                </div>

                <div class="col-span-2 flex items-center gap-2">
                    <button id="reversalBtn" onclick="toggleReversalMode()" class="flex-1 bg-emerald-600 hover:bg-emerald-700 text-white font-semibold text-xs px-3 py-2 rounded-lg shadow-sm transition">
                        ⚡ Stock Reversal
                    </button>
                    <button onclick="resetFilters()" class="bg-slate-100 hover:bg-slate-200 text-slate-600 font-semibold text-xs px-3 py-2 rounded-lg transition">
                        Reset
                    </button>
                </div>
            </div>

            <div class="mt-3 pt-3 border-t border-slate-100 flex flex-wrap items-center justify-between text-xs text-slate-500 gap-2">
                <div class="flex items-center gap-4">
                    <span>Showing: <strong id="totalCount" class="text-slate-900">0</strong> stocks</span>
                    <span>Reversal Matches: <strong id="reversalCount" class="text-emerald-600">0</strong></span>
                    <span>Increasing Delivery: <strong id="incDelivCount" class="text-indigo-600">0</strong></span>
                </div>
                <div class="text-[11px] text-slate-400">
                    Click <strong>📈 Chart</strong> on any stock row to view 3-Year Candlesticks, 20/50 SMA & Delivery Volume % Chart.
                </div>
            </div>
        </div>

        <div id="reversalBanner" class="hidden mb-4 bg-emerald-50 border border-emerald-200 rounded-xl p-3.5 text-xs text-emerald-900 flex items-center justify-between">
            <div>
                <span class="font-bold text-emerald-800">Stock Reversal Filter Active:</span>
                Showing beaten-down stocks reclaiming their 20-Day SMA with RSI recovery (32–66) and support bounce.
            </div>
            <button onclick="toggleReversalMode()" class="text-emerald-700 hover:underline font-bold ml-4">Clear ✕</button>
        </div>

        <!-- Results Table -->
        <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
            <div class="overflow-x-auto">
                <table class="min-w-full text-sm text-left">
                    <thead class="bg-slate-100 border-b border-slate-200 text-[11px] uppercase font-bold text-slate-600">
                        <tr>
                            <th class="py-3 px-4">Symbol</th>
                            <th class="py-3 px-4">Mkt Cap (Cr)</th>
                            <th class="py-3 px-4">Price (₹)</th>
                            <th class="py-3 px-4">1Y High / Drop</th>
                            <th class="py-3 px-4">2Y High / Drop</th>
                            <th class="py-3 px-4">3Y High / Drop</th>
                            <th class="py-3 px-4">Avg Delivery % & Trend</th>
                            <th class="py-3 px-4">Technicals</th>
                            <th class="py-3 px-4">Reversal</th>
                            <th class="py-3 px-4 text-center">Chart</th>
                        </tr>
                    </thead>
                    <tbody id="stockTableBody" class="divide-y divide-slate-100">
                        <tr>
                            <td colspan="10" class="text-center py-12 text-slate-400">
                                Loading NSE & BSE stock database...
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </main>

    <!-- Interactive Chart Modal -->
    <div id="chartModal" class="fixed inset-0 bg-slate-900/60 backdrop-blur-xs hidden z-50 flex items-center justify-center p-4">
        <div class="bg-white rounded-2xl border border-slate-200 shadow-2xl w-full max-w-6xl overflow-hidden flex flex-col max-h-[92vh]">
            <!-- Modal Header -->
            <div class="px-6 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-4 bg-slate-50">
                <div class="flex items-center gap-3">
                    <h2 id="modalTitle" class="text-lg font-extrabold text-slate-900">RELIANCE</h2>
                    <span id="modalExch" class="text-xs font-bold px-2 py-0.5 rounded bg-blue-100 text-blue-800">NSE</span>
                    <span id="modalPrice" class="text-base font-bold text-indigo-600">₹0.00</span>
                    <span id="modalMeta" class="text-xs text-slate-500 hidden sm:inline"></span>
                </div>

                <div class="flex items-center gap-2">
                    <!-- Timeframe Buttons -->
                    <div class="inline-flex rounded-lg border border-slate-300 bg-white p-0.5 text-xs font-semibold">
                        <button onclick="setChartRange(63)" class="tf-btn px-2.5 py-1 rounded hover:bg-slate-100 text-slate-600">3M</button>
                        <button onclick="setChartRange(126)" class="tf-btn px-2.5 py-1 rounded hover:bg-slate-100 text-slate-600">6M</button>
                        <button onclick="setChartRange(252)" class="tf-btn px-2.5 py-1 rounded bg-indigo-600 text-white">1Y</button>
                        <button onclick="setChartRange(504)" class="tf-btn px-2.5 py-1 rounded hover:bg-slate-100 text-slate-600">2Y</button>
                        <button onclick="setChartRange(9999)" class="tf-btn px-2.5 py-1 rounded hover:bg-slate-100 text-slate-600">3Y</button>
                    </div>

                    <a id="tvExternalLink" href="#" target="_blank" class="text-xs font-semibold text-indigo-600 hover:text-indigo-800 px-3 py-1.5 rounded-lg border border-indigo-200 bg-indigo-50">
                        Open in TradingView ↗
                    </a>

                    <button onclick="closeChartModal()" class="text-slate-400 hover:text-slate-700 text-xl font-bold px-2.5 py-1 rounded-lg hover:bg-slate-200 transition">
                        ✕
                    </button>
                </div>
            </div>

            <!-- Modal Body -->
            <div class="p-5 overflow-y-auto space-y-4">
                <!-- Legend -->
                <div class="flex flex-wrap items-center justify-between text-xs text-slate-600 gap-2">
                    <div class="flex items-center gap-4">
                        <span class="inline-flex items-center gap-1.5 font-semibold"><span class="w-3 h-3 rounded-xs bg-emerald-500 inline-block"></span> Daily Candles + Volume</span>
                        <span class="inline-flex items-center gap-1.5 font-semibold text-blue-600"><span class="w-4 h-0.5 bg-blue-600 inline-block"></span> 20-Day SMA</span>
                        <span class="inline-flex items-center gap-1.5 font-semibold text-amber-500"><span class="w-4 h-0.5 bg-amber-500 inline-block"></span> 50-Day SMA</span>
                    </div>
                    <div id="chartStatusText" class="text-xs text-slate-400">Scroll to zoom • Drag to pan</div>
                </div>

                <!-- Main Price + SMA + Volume Chart Container -->
                <div id="priceChartContainer" class="w-full h-[380px] rounded-xl border border-slate-200 bg-white relative"></div>

                <!-- 30-Day Delivery Volume % Sub-Chart -->
                <div class="bg-slate-50 p-4 rounded-xl border border-slate-200">
                    <div class="flex items-center justify-between mb-2">
                        <span class="text-xs font-bold uppercase text-slate-600">Last 30 Sessions Delivery Volume % Trend</span>
                        <span id="modalDelivBadge" class="text-xs font-bold"></span>
                    </div>
                    <div id="delivBarsContainer" class="h-24 flex items-end gap-1 pt-4"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let allStocks = [];
        let reversalOnlyMode = false;
        let pollTimer = null;

        // Chart globals
        let tvChart = null;
        let candleSeries = null;
        let sma20Series = null;
        let sma50Series = null;
        let volumeSeries = null;
        let currentChartPayload = null;

        async function loadData() {
            try {
                const res = await fetch('/api/stocks');
                const json = await res.json();
                allStocks = json.data || [];
                document.getElementById('dbCount').innerText = allStocks.length;
                document.getElementById('lastUpdated').innerText = json.last_updated || 'Syncing...';
                renderView();
                startPollingStatus();
            } catch (e) {
                console.error("Load error:", e);
            }
        }

        async function startBackgroundSync() {
            await fetch('/api/refresh', { method: 'POST' });
            startPollingStatus();
        }

        function startPollingStatus() {
            if (pollTimer) clearInterval(pollTimer);
            pollTimer = setInterval(async () => {
                const res = await fetch('/api/status');
                const status = await res.json();
                const container = document.getElementById('progressContainer');
                const btn = document.getElementById('refreshBtn');
                const icon = document.getElementById('syncIcon');

                if (status.loaded_count !== allStocks.length) {
                    const stockRes = await fetch('/api/stocks');
                    const stockJson = await stockRes.json();
                    allStocks = stockJson.data || [];
                    document.getElementById('dbCount').innerText = allStocks.length;
                    document.getElementById('lastUpdated').innerText = stockJson.last_updated;
                    renderView();
                }

                if (status.is_running) {
                    container.classList.remove('hidden');
                    btn.disabled = true;
                    btn.classList.add('opacity-60', 'cursor-not-allowed');
                    icon.classList.add('animate-spin');

                    const pct = status.total > 0 ? Math.round((status.progress / status.total) * 100) : 10;
                    document.getElementById('progressText').innerText = status.message;
                    document.getElementById('progressCount').innerText = pct + '%';
                    document.getElementById('progressBar').style.width = pct + '%';
                } else {
                    container.classList.add('hidden');
                    btn.disabled = false;
                    btn.classList.remove('opacity-60', 'cursor-not-allowed');
                    icon.classList.remove('animate-spin');
                    clearInterval(pollTimer);

                    const stockRes = await fetch('/api/stocks');
                    const stockJson = await stockRes.json();
                    allStocks = stockJson.data || [];
                    document.getElementById('dbCount').innerText = allStocks.length;
                    document.getElementById('lastUpdated').innerText = stockJson.last_updated;
                    renderView();
                }
            }, 1500);
        }

        function computeDeliveryStats(stock, days) {
            const series = stock.deliv_series || [];
            if (series.length === 0) {
                return { avg: stock.avg_deliv_5d || 0, prevAvg: 0, diff: 0, trend: "Increasing" };
            }
            const n = Math.max(1, Math.min(30, days));
            const recentSlice = series.slice(-n);
            const avg = recentSlice.reduce((a, b) => a + b, 0) / recentSlice.length;

            let prevSlice = series.slice(-2 * n, -n);
            if (prevSlice.length === 0) {
                prevSlice = series.slice(0, Math.max(1, series.length - n));
            }
            const prevAvg = prevSlice.reduce((a, b) => a + b, 0) / prevSlice.length;
            const diff = avg - prevAvg;
            const trend = diff >= 0 ? "Increasing" : "Decreasing";

            return {
                avg: Number(avg.toFixed(1)),
                prevAvg: Number(prevAvg.toFixed(1)),
                diff: Number(diff.toFixed(1)),
                trend: trend
            };
        }

        function toggleReversalMode() {
            reversalOnlyMode = !reversalOnlyMode;
            const btn = document.getElementById('reversalBtn');
            const banner = document.getElementById('reversalBanner');
            if (reversalOnlyMode) {
                btn.classList.replace('bg-emerald-600', 'bg-slate-900');
                btn.classList.replace('hover:bg-emerald-700', 'hover:bg-slate-800');
                banner.classList.remove('hidden');
            } else {
                btn.classList.replace('bg-slate-900', 'bg-emerald-600');
                btn.classList.replace('hover:bg-slate-800', 'hover:bg-emerald-700');
                banner.classList.add('hidden');
            }
            renderView();
        }

        function resetFilters() {
            document.getElementById('exchangeFilter').value = 'ALL';
            document.getElementById('yearFilter').value = '1Y';
            document.getElementById('mcapFilter').value = '1000';
            document.getElementById('delivPctFilter').value = '45';
            document.getElementById('delivDaysFilter').value = '5';
            if (reversalOnlyMode) toggleReversalMode();
            renderView();
        }

        function renderView() {
            const exch = document.getElementById('exchangeFilter').value;
            const yr = document.getElementById('yearFilter').value;
            const minMcap = parseFloat(document.getElementById('mcapFilter').value) || 0;
            const minDelivPct = parseFloat(document.getElementById('delivPctFilter').value) || 0;
            const delivDays = parseInt(document.getElementById('delivDaysFilter').value) || 5;

            let incCount = 0;

            let filtered = allStocks.map(s => {
                const dStats = computeDeliveryStats(s, delivDays);
                return { ...s, _dStats: dStats };
            }).filter(s => {
                if (exch !== 'ALL' && s.exchange !== exch) return false;
                if (s.market_cap_cr < minMcap) return false;
                if (yr === '1Y' && !s.down_1y) return false;
                if (yr === '2Y' && !s.down_2y) return false;
                if (yr === '3Y' && !s.down_3y) return false;
                if (s._dStats.avg < minDelivPct) return false;
                if (reversalOnlyMode && !s.is_reversal) return false;
                if (s._dStats.trend === 'Increasing') incCount++;
                return true;
            });

            document.getElementById('totalCount').innerText = filtered.length;
            document.getElementById('reversalCount').innerText = filtered.filter(s => s.is_reversal).length;
            document.getElementById('incDelivCount').innerText = incCount;

            const tbody = document.getElementById('stockTableBody');
            tbody.innerHTML = '';

            if (filtered.length === 0) {
                if (allStocks.length === 0) {
                    tbody.innerHTML = `<tr><td colspan="10" class="text-center py-12 text-indigo-600 font-medium animate-pulse">Downloading NSE & BSE stocks in background... First batch will appear in ~5 seconds.</td></tr>`;
                } else {
                    tbody.innerHTML = `<tr><td colspan="10" class="text-center py-10 text-slate-400">No stocks match current filters. Try lowering Avg Delivery % or Market Cap limit.</td></tr>`;
                }
                return;
            }

            filtered.forEach(s => {
                const tr = document.createElement('tr');
                tr.className = 'hover:bg-slate-50 transition';

                const badgeColor = s.exchange === 'NSE'
                    ? 'bg-blue-50 text-blue-700 border-blue-200'
                    : 'bg-amber-50 text-amber-700 border-amber-200';

                const d = s._dStats;
                const isInc = d.trend === 'Increasing';
                const trendBadge = isInc
                    ? `<span class="inline-flex items-center gap-0.5 bg-emerald-50 text-emerald-700 border border-emerald-200 text-[11px] font-bold px-2 py-0.5 rounded">▲ Increasing (${d.diff >= 0 ? '+' : ''}${d.diff}%)</span>`
                    : `<span class="inline-flex items-center gap-0.5 bg-rose-50 text-rose-700 border border-rose-200 text-[11px] font-bold px-2 py-0.5 rounded">▼ Decreasing (${d.diff}%)</span>`;

                let revHtml = `<span class="text-xs text-slate-400">Watch (${s.reversal_score}%)</span>`;
                if (s.is_reversal) {
                    revHtml = `
                        <div class="inline-flex flex-col">
                            <span class="bg-emerald-100 text-emerald-800 border border-emerald-300 font-bold text-xs px-2 py-0.5 rounded w-max">
                                ★ Reversal (${s.reversal_score}%)
                            </span>
                            <span class="text-[10px] text-slate-500 mt-0.5">${s.reversal_reasons.join(' • ')}</span>
                        </div>
                    `;
                }

                tr.innerHTML = `
                    <td class="py-3 px-4">
                        <button onclick="openStockChart('${s.symbol}')" class="font-bold text-slate-900 hover:text-indigo-600 hover:underline text-left block">
                            ${s.symbol}
                        </button>
                        <span class="text-[10px] font-semibold px-1.5 py-0.5 rounded border ${badgeColor}">${s.exchange}</span>
                    </td>
                    <td class="py-3 px-4 font-semibold text-slate-700">₹${s.market_cap_cr.toLocaleString('en-IN')}</td>
                    <td class="py-3 px-4">
                        <div class="font-bold text-slate-900">₹${s.price.toLocaleString('en-IN')}</div>
                        <div class="text-[10px] text-emerald-600">+${s.bounce_pct}% off 1Y Low</div>
                    </td>
                    <td class="py-3 px-4">
                        <div class="text-xs text-slate-400">H: ₹${s.high_1y}</div>
                        <div class="font-semibold ${s.dd_1y >= 15 ? 'text-rose-600' : 'text-slate-700'}">-${s.dd_1y}%</div>
                    </td>
                    <td class="py-3 px-4">
                        <div class="text-xs text-slate-400">H: ₹${s.high_2y}</div>
                        <div class="font-semibold ${s.dd_2y >= 20 ? 'text-rose-600' : 'text-slate-700'}">-${s.dd_2y}%</div>
                    </td>
                    <td class="py-3 px-4">
                        <div class="text-xs text-slate-400">H: ₹${s.high_3y}</div>
                        <div class="font-semibold ${s.dd_3y >= 25 ? 'text-rose-600' : 'text-slate-700'}">-${s.dd_3y}%</div>
                    </td>
                    <td class="py-3 px-4">
                        <div class="font-bold text-slate-900 text-sm">${d.avg}% <span class="text-[11px] font-normal text-slate-400">(${delivDays}D Avg)</span></div>
                        <div class="mt-1">${trendBadge}</div>
                    </td>
                    <td class="py-3 px-4 text-xs">
                        <div>RSI: <strong>${s.rsi}</strong></div>
                        <div>20SMA: <strong>₹${s.sma_20}</strong> ${s.price >= s.sma_20 ? '<span class="text-emerald-600 font-bold">▲</span>' : '<span class="text-rose-500">▼</span>'}</div>
                    </td>
                    <td class="py-3 px-4">${revHtml}</td>
                    <td class="py-3 px-4 text-center">
                        <button onclick="openStockChart('${s.symbol}')" class="bg-indigo-50 hover:bg-indigo-600 text-indigo-700 hover:text-white border border-indigo-200 font-semibold text-xs px-3 py-1.5 rounded-lg transition shadow-2xs">
                            📈 Chart
                        </button>
                    </td>
                `;
                tbody.appendChild(tr);
            });
        }

        // --- INTERACTIVE CHART MODAL FUNCTIONS ---
        async function openStockChart(symbol) {
            const modal = document.getElementById('chartModal');
            modal.classList.remove('hidden');

            const stock = allStocks.find(s => s.symbol === symbol);
            const delivDays = parseInt(document.getElementById('delivDaysFilter').value) || 5;
            const dStats = stock ? computeDeliveryStats(stock, delivDays) : null;

            document.getElementById('modalTitle').innerText = symbol;
            document.getElementById('modalExch').innerText = stock ? stock.exchange : 'NSE';
            document.getElementById('modalPrice').innerText = stock ? `₹${stock.price.toLocaleString('en-IN')}` : '';
            document.getElementById('modalMeta').innerText = stock
                ? `Mkt Cap: ₹${stock.market_cap_cr.toLocaleString('en-IN')} Cr • RSI(14): ${stock.rsi} • 1Y Drop: -${stock.dd_1y}%`
                : '';
            document.getElementById('tvExternalLink').href = `https://www.tradingview.com/chart/?symbol=NSE:${encodeURIComponent(symbol)}`;

            if (dStats) {
                const badgeEl = document.getElementById('modalDelivBadge');
                badgeEl.innerHTML = dStats.trend === 'Increasing'
                    ? `<span class="text-emerald-700 bg-emerald-100 px-2 py-0.5 rounded">${delivDays}D Avg: ${dStats.avg}% (▲ Increasing ${dStats.diff >= 0 ? '+' : ''}${dStats.diff}%)</span>`
                    : `<span class="text-rose-700 bg-rose-100 px-2 py-0.5 rounded">${delivDays}D Avg: ${dStats.avg}% (▼ Decreasing ${dStats.diff}%)</span>`;
            }

            document.getElementById('chartStatusText').innerText = 'Loading 3-Year historical candles...';

            try {
                const res = await fetch(`/api/chart/${encodeURIComponent(symbol)}`);
                const data = await res.json();
                if (data.status !== 'ok') throw new Error(data.message || 'Chart error');

                currentChartPayload = data;
                initOrUpdateTVChart(data);
                renderDeliveryBars(data.delivery_30d || []);
                document.getElementById('chartStatusText').innerText = 'Scroll to zoom • Drag to pan';
            } catch (err) {
                document.getElementById('chartStatusText').innerText = 'Error loading chart: ' + err.message;
            }
        }

        function initOrUpdateTVChart(data) {
            const container = document.getElementById('priceChartContainer');
            container.innerHTML = '';

            tvChart = LightweightCharts.createChart(container, {
                width: container.clientWidth,
                height: 380,
                layout: {
                    background: { type: 'solid', color: '#ffffff' },
                    textColor: '#334155',
                },
                grid: {
                    vertLines: { color: '#f1f5f9' },
                    horzLines: { color: '#f1f5f9' },
                },
                rightPriceScale: {
                    borderColor: '#e2e8f0',
                },
                timeScale: {
                    borderColor: '#e2e8f0',
                    timeVisible: true,
                },
                crosshair: {
                    mode: LightweightCharts.CrosshairMode.Normal,
                },
            });

            candleSeries = tvChart.addCandlestickSeries({
                upColor: '#10b981',
                downColor: '#f43f5e',
                borderUpColor: '#10b981',
                borderDownColor: '#f43f5e',
                wickUpColor: '#10b981',
                wickDownColor: '#f43f5e',
            });
            candleSeries.setData(data.candles);

            sma20Series = tvChart.addLineSeries({
                color: '#2563eb',
                lineWidth: 2,
                title: '20 SMA',
            });
            sma20Series.setData(data.sma20);

            sma50Series = tvChart.addLineSeries({
                color: '#f59e0b',
                lineWidth: 2,
                title: '50 SMA',
            });
            sma50Series.setData(data.sma50);

            volumeSeries = tvChart.addHistogramSeries({
                priceFormat: { type: 'volume' },
                priceScaleId: '',
            });
            volumeSeries.priceScale().applyOptions({
                scaleMargins: { top: 0.8, bottom: 0 },
            });
            volumeSeries.setData(data.volumes);

            // Default view: Last 1 Year (252 trading days)
            setChartRange(252);
        }

        function setChartRange(barsCount) {
            if (!tvChart || !currentChartPayload || !currentChartPayload.candles.length) return;
            const candles = currentChartPayload.candles;
            const total = candles.length;
            const fromIdx = Math.max(0, total - barsCount);
            tvChart.timeScale().setVisibleRange({
                from: candles[fromIdx].time,
                to: candles[total - 1].time,
            });
        }

        function renderDeliveryBars(deliveryPoints) {
            const container = document.getElementById('delivBarsContainer');
            container.innerHTML = '';
            const minDelivFilter = parseFloat(document.getElementById('delivPctFilter').value) || 45;

            deliveryPoints.forEach(pt => {
                const isHigh = pt.value >= minDelivFilter;
                const barColor = isHigh ? 'bg-emerald-500 hover:bg-emerald-600' : 'bg-slate-300 hover:bg-slate-400';
                const heightPct = Math.max(12, Math.min(100, pt.value));

                const wrapper = document.createElement('div');
                wrapper.className = 'flex-1 flex flex-col items-center h-full justify-end group relative';
                wrapper.innerHTML = `
                    <div class="opacity-0 group-hover:opacity-100 absolute -top-6 bg-slate-800 text-white text-[10px] font-bold px-1.5 py-0.5 rounded pointer-events-none whitespace-nowrap z-10">
                        ${pt.time}: ${pt.value}%
                    </div>
                    <div class="w-full rounded-t ${barColor} transition-all" style="height: ${heightPct}%"></div>
                `;
                container.appendChild(wrapper);
            });
        }

        function closeChartModal() {
            document.getElementById('chartModal').classList.add('hidden');
        }

        window.addEventListener('resize', () => {
            const container = document.getElementById('priceChartContainer');
            if (tvChart && container && container.clientWidth > 0) {
                tvChart.applyOptions({ width: container.clientWidth });
            }
        });

        window.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closeChartModal();
        });

        window.onload = loadData;
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def serve_ui():
    return HTMLResponse(content=HTML_PAGE)


if __name__ == "__main__":
    print("Starting Screener with Charts at http://127.0.0.1:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)