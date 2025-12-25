export interface Universe {
  key: string;
  label: string;
}

export interface StocksResponse {
  universe: string;
  count: number;
  symbols: string[];
}

export interface ChartResponse {
  symbol: string;
  tf: string;
  date: string[];
  open: number[];
  high: number[];
  low: number[];
  close: number[];
  volume: number[];
}
