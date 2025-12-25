/* ============================================================================
   FRONTEND ENTRY FILE : main.ts
============================================================================
   PURPOSE
   -------
   - Load universes and stocks
   - Render candlestick chart
   - Toggle volume safely (no chart distortion)
   - Handle timeframe changes
============================================================================ */

import { createChart } from "lightweight-charts";
import { getUniverses, getStocks, getChart } from "./api";

/* ============================================================================
   CONFIGURATION
============================================================================ */

const DEFAULT_TIMEFRAME = "1D";
const DEBUG = true;

/* ============================================================================
   DOM REFERENCES
============================================================================ */

const universesEl = document.getElementById("universes")!;
const stocksEl = document.getElementById("stocks")!;
const stockSearchEl = document.getElementById("stockSearch")!;

const chartEl = document.getElementById("chart")!;
const chartTitleEl = document.getElementById("chartTitle")!;
const volumeBtn = document.getElementById("toggleVolume")!;

/* ============================================================================
   STATE
============================================================================ */

let currentUniverse: string | null = null;
let currentSymbol: string | null = null;
let currentTF: string = DEFAULT_TIMEFRAME;

let showVolume = false;

/* Cache stocks per universe */
const universeStockCache: Record<string, string[]> = {};
let visibleStocks: string[] = [];

/* Cache last chart data (IMPORTANT for volume toggle) */
let lastChartData: any = null;

/* ============================================================================
   DEBUG HELPER
============================================================================ */

function log(...args: any[]) {
  if (DEBUG) console.log("[UI]", ...args);
}

/* ============================================================================
   CHART INITIALIZATION
============================================================================ */

const chart = createChart(chartEl, {
  width: chartEl.clientWidth,
  layout: {
    background: { color: "#0f172a" },
    textColor: "#e5e7eb",
  },
  grid: {
    vertLines: { color: "#1e293b" },
    horzLines: { color: "#1e293b" },
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
  },
});

/* ---------------- PRICE SERIES ---------------- */

const candleSeries = chart.addCandlestickSeries({
  upColor: "#16a34a",
  downColor: "#dc2626",
  wickUpColor: "#16a34a",
  wickDownColor: "#dc2626",
  borderVisible: false,
});

/* ---------------- VOLUME SERIES (FIXED) ---------------- */

/*
  KEY POINTS:
  - Separate priceScaleId ("volume")
  - Scale margins give priority to candles
  - Volume NEVER re-fetches data
*/

const volumeSeries = chart.addHistogramSeries({
  priceFormat: { type: "volume" },
  priceScaleId: "volume",
});

/* Configure volume scale so candles dominate */
chart.priceScale("volume").applyOptions({
  scaleMargins: {
    top: 0.85,   // 75% space for candles
    bottom: 0.03, // volume stays at bottom
  },
});

/* Resize handler */
window.addEventListener("resize", () => {
  chart.applyOptions({ width: chartEl.clientWidth });
});

/* ============================================================================
   LOAD UNIVERSes
============================================================================ */

async function loadUniverses() {
  log("Loading universes");

  universesEl.innerHTML = "";
  stocksEl.innerHTML = "";
  stockSearchEl.value = "";

  currentUniverse = null;
  currentSymbol = null;

  candleSeries.setData([]);
  volumeSeries.setData([]);
  chartTitleEl.innerText = "Select a stock";

  const universes = await getUniverses();

  universes.forEach((u: any) => {
    const el = document.createElement("div");
    el.className = "universe";
    el.innerText = u.label;

    el.onclick = async () => {
      document
        .querySelectorAll(".universe")
        .forEach((x) => x.classList.remove("active"));

      el.classList.add("active");

      currentUniverse = u.key;
      currentSymbol = null;

      stocksEl.innerHTML = "";
      candleSeries.setData([]);
      volumeSeries.setData([]);
      chartTitleEl.innerText = "Select a stock";

      if (!universeStockCache[u.key]) {
        log("Fetching stocks for", u.key);
        const res = await getStocks(u.key);

        /* Defensive filter: no universe/index names */
        universeStockCache[u.key] = (res.symbols || []).filter(
          (s: string) =>
            s &&
            !s.startsWith("NIFTY") &&
            s !== u.key &&
            s.trim().length > 1
        );
      }

      visibleStocks = universeStockCache[u.key];
      renderStocks(visibleStocks);
    };

    universesEl.appendChild(el);
  });
}

/* ============================================================================
   RENDER STOCK LIST
============================================================================ */

function renderStocks(symbols: string[]) {
  stocksEl.innerHTML = "";

  symbols.forEach((symbol) => {
    const el = document.createElement("div");
    el.innerText = symbol;

    el.onclick = () => {
      document
        .querySelectorAll("#stocks div")
        .forEach((x) => x.classList.remove("active"));

      el.classList.add("active");
      loadChart(symbol);
    };

    stocksEl.appendChild(el);
  });
}

/* ============================================================================
   STOCK SEARCH
============================================================================ */

stockSearchEl.addEventListener("input", () => {
  const q = stockSearchEl.value.toLowerCase();
  renderStocks(
    visibleStocks.filter((s) => s.toLowerCase().includes(q))
  );
});

/* ============================================================================
   LOAD CHART DATA
============================================================================ */

async function loadChart(symbol: string) {
  currentSymbol = symbol;
  chartTitleEl.innerText = `${symbol} · ${currentTF}`;

  log("Loading chart", symbol, currentTF);

  const res = await getChart(symbol, currentTF);

  if (!res || !res.data || !res.data.date?.length) {
    candleSeries.setData([]);
    volumeSeries.setData([]);
    return;
  }

  lastChartData = res.data;

  /* ----- Candles ----- */
  const candles = res.data.date.map((d: string, i: number) => ({
    time: d,
    open: res.data.open[i],
    high: res.data.high[i],
    low: res.data.low[i],
    close: res.data.close[i],
  }));

  candleSeries.setData(candles);

  /* ----- Volume ----- */
  updateVolume();

  chart.timeScale().scrollToRealTime();
}

/* ============================================================================
   VOLUME TOGGLE (NO REFETCH, NO RESCALE ISSUES)
============================================================================ */

function updateVolume() {
  if (!lastChartData || !showVolume) {
    volumeSeries.setData([]);
    return;
  }

  const vols = lastChartData.date.map((d: string, i: number) => ({
    time: d,
    value: lastChartData.volume[i],
    color:
      lastChartData.close[i] >= lastChartData.open[i]
        ? "#16a34a"
        : "#dc2626",
  }));

  volumeSeries.setData(vols);
}

volumeBtn.onclick = () => {
  showVolume = !showVolume;
  volumeBtn.classList.toggle("active", showVolume);
  updateVolume();
};

/* ============================================================================
   TIMEFRAME BUTTONS
============================================================================ */

document.querySelectorAll<HTMLButtonElement>(".tf button").forEach((btn) => {
  btn.onclick = () => {
    if (!currentSymbol) return;

    document
      .querySelectorAll(".tf button")
      .forEach((b) => b.classList.remove("active"));

    btn.classList.add("active");
    currentTF = btn.dataset.tf!;
    loadChart(currentSymbol);
  };
});

/* ============================================================================
   START APPLICATION
============================================================================ */

log("Starting frontend");
loadUniverses();
