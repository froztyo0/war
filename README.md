# Conflict Intelligence Dashboard

Real-time Iran-US conflict tracker with live news, map markers, and market data.

## Architecture

```
frontend/
  index.html          ← Single HTML file, Leaflet.js map, WebSocket client

backend/
  main.py             ← FastAPI + WebSocket server
  requirements.txt    ← Python deps
  .env.example        ← Copy to .env and fill in your keys

api/
  index.py            ← Vercel Python function entrypoint

build.py              ← Copies frontend/ into public/ for Vercel
vercel.json           ← Vercel build config
```

## Free APIs Used

| API | What it provides | Key required? |
|-----|-----------------|---------------|
| **GDELT Project** | Global conflict news, no rate limit | ❌ Free, no key |
| **RSS Feeds** | BBC, Al Jazeera, Reuters, Guardian | ❌ No key |
| **CoinGecko** | Bitcoin, Ethereum prices | ❌ Free, no key |
| **NewsAPI.org** | 100 req/day free tier | ✅ Free at newsapi.org |
| **GNews.io** | 100 req/day free tier | ✅ Free at gnews.io |
| **Alpha Vantage** | WTI/Brent crude, Gold, S&P 500 | ✅ Free at alphavantage.co |
| **Open Exchange Rates** | USD/IRR live rate | ✅ Free at openexchangerates.org |
| **OpenStreetMap + CARTO** | Dark map tiles | ❌ Free, no key |

**The dashboard works without any API keys** — GDELT + RSS + CoinGecko cover news and crypto prices. Keys just add oil/gold/stock prices and more news sources.

---

## Setup

### 1. Backend

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure API keys (optional but recommended)
cp .env.example .env
# Edit .env and add your free API keys

# Run
python main.py
# Server starts at http://localhost:8000
```

### 2. Frontend

Option A — just open the file:
```bash
open frontend/index.html
# or double-click it
```

Option B — serve with Python (avoids any CORS issues):
```bash
cd frontend
python -m http.server 3000
# Open http://localhost:3000
```

Option C — serve with Node:
```bash
npx serve frontend/
```

---

## How it works

### WebSocket flow
```
Browser  ──── WS connect ────►  FastAPI (/ws)
         ◄── news (initial) ───
         ◄── markets (initial)─
         ──── {action:refresh}─►  triggers fresh fetch
         ◄── markets (every 30s)─
         ◄── news (every 2min) ─
```

### News pipeline
1. RSS feeds (BBC, Al Jazeera, Reuters, Guardian) — parsed with stdlib xml
2. GDELT Project API — free, indexes thousands of news sources globally
3. NewsAPI.org — optional, adds major wire services
4. GNews.io — optional backup

Each article is:
- Filtered for Middle East / Iran-US relevance
- Classified as: `attack | diplomacy | military | economy`
- Geo-tagged with approximate coordinates based on location mentions
- Deduplicated by title

### Map
- **OpenStreetMap** tiles via CARTO dark theme (free, no key)
- **Leaflet.js** open-source map library
- Static markers: nuclear sites, US military bases, strategic chokepoints
- Dynamic markers: geo-tagged news articles (click for popup with article link)
- Heatmap: shows event density using leaflet.heat

### Markets
- CoinGecko WebAPI for BTC/ETH (no key, updates every 30s via WS)
- Alpha Vantage for WTI crude, Brent crude, Gold, SPY (free key)
- Open Exchange Rates for USD/IRR

---

## Get free API keys

1. **NewsAPI**: https://newsapi.org/register (instant, no credit card)
2. **GNews**: https://gnews.io/ → Sign up → Dashboard → API Key
3. **Alpha Vantage**: https://www.alphavantage.co/support/#api-key (instant)
4. **Open Exchange Rates**: https://openexchangerates.org/signup/free

---

## Deploy to production

### Vercel

This repo is ready for Vercel:

- `api/index.py` exposes the FastAPI app as a Vercel Python Function
- `build.py` copies `frontend/` into `public/` during deployment
- the frontend uses same-origin `/api/...` requests in production
- if native WebSockets are unavailable, the dashboard falls back to HTTP polling automatically

Deploy steps:

1. Import the repo into Vercel
2. Add the environment variables from `backend/.env`
3. Deploy

Recommended environment variables:

- `NEWS_API_KEY`
- `GNEWS_API_KEY`
- `ALPHA_VANTAGE_KEY`
- `OXR_APP_ID`
- `OPENSKY_USER`
- `OPENSKY_PASS`
- `TWITTER_BEARER_TOKEN`

Notes:

- Cache files are written to `/tmp` on Vercel because the deployment filesystem is ephemeral
- On Vercel-style deployments, realtime updates use polling fallback instead of relying on a native WebSocket server

### Docker (quickest)
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Nginx reverse proxy
Point `location /api` and `location /ws` to `localhost:8000`.
Serve `frontend/index.html` as static file.
Change `API_BASE` and `WS_URL` in index.html to your domain.

---

## Customization

**Add more news sources** — add entries to `RSS_FEEDS` dict in main.py.

**Add more search terms** — extend `SEARCH_TERMS` list.

**Add more static map sites** — extend `staticSites` array in index.html.

**Change map tile style** — swap the tileLayer URL (OpenStreetMap, Stamen, Mapbox all work).
