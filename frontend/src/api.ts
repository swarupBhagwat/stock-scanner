import type { Universe, StocksResponse, ChartResponse } from "./types";

const API = "/api";

export async function getUniverses(): Promise<Universe[]> {
  const r = await fetch(`${API}/universes`, { cache: "no-store" });
  return r.json();
}

export async function getStocks(universe: string): Promise<StocksResponse> {
  const r = await fetch(
    `${API}/stocks?universe=${universe}`,
    { cache: "no-store" }
  );
  return r.json();
}

export async function getChart(
  symbol: string,
  tf: string
): Promise<ChartResponse> {
  const r = await fetch(
    `${API}/chart?symbol=${symbol}&tf=${tf}`,
    { cache: "no-store" }
  );
  return r.json();
}
