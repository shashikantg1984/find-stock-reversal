# NSE & BSE Quantitative Stock Screener

An algorithmic market screener and quantitative dashboard for Indian equities (NSE & BSE). Built with **FastAPI**, **SQLite**, and **TradingView Lightweight Charts**, this platform identifies early uptrend breakouts and oversold mean-reversion setups while filtering out falling knives and bear traps. It features an incremental NSE Bhavcopy diff-sync engine and continuous multi-year macro monitoring for Gold and Silver.

---

## 📌 Features

- **🚀 Early Uptrend Engine**:
  - Requires price to trade strictly above both the **20-day** and **50-day Simple Moving Averages (SMA)**.
  - Evaluates the **5-day slope of the 20 SMA** to ensure short-term trendlines are tilting upward, filtering out downtrending traps.
  - Checks for consolidation near short-term highs (within 3% of the 20-session high).
  - Confirms buyer momentum with **RSI ≥ 50** and a positive **MACD Histogram**.
- **⚡ Reversal & Bottom-Fishing Engine**:
  - Highlights deeply corrected stocks attempting oversold bounces near their 20 SMA with RSI recovery.
- **📊 Institutional Accumulation Tracking**:
  - Calculates 5-day to 30-day delivery percentages and volume trends directly from exchange bhavcopy records.
  - Tracks volume surge ratios (`Today's Volume / 20-Day Average Volume`).
- **⚡ Sub-15 Second Incremental Sync**:
  - Pulls daily Bhavcopies directly from NSE archives to apply 1-day price and delivery diffs without re-downloading entire multi-year histories[cite: 1, 3].
- **🪙 Macro Commodities Monitor**:
  - Continuous 2-year candlestick analysis for **Gold Futures (`GC=F`)** and **Silver Futures (`SI=F`)** with 20/50 SMAs, RSI (14), and synchronized crosshairs[cite: 1, 3].

---

## 🛠️ Tech Stack

- **Backend**: Python 3.9+, FastAPI, Uvicorn
- **Data & Computations**: Pandas, NumPy, SQLite3, Requests, yfinance[cite: 1, 2]
- **Frontend**: Single-page application served via FastAPI, Tailwind CSS, TradingView Lightweight Charts[cite: 1, 3]

---

## 📂 Project Structure

```text
├── app.py                         # Core application: FastAPI backend, indicator calculations, and web UI
├── start-find-stock-reversal.bat  # 1-click Windows startup script
├── stocks_market.db               # SQLite database caching stock candles and screener indicators (auto-generated)
└── README.md                      # Documentation
