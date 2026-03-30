export interface DataColumn {
  name: string;
  type: 'string' | 'number' | 'date' | 'boolean';
  sample: string[];
  subtype?: string | null;
}

export interface KPI {
  id: string;
  label: string;
  value: string;
  change?: number;
  icon: string;
  description?: string;
  businessImpact?: string;
}

export interface ChartConfig {
  id: string;
  type: 'line' | 'bar' | 'pie' | 'area';
  title: string;
  xKey: string;
  yKey: string;
  data: Record<string, any>[];
  rationale?: string;
}

export interface AIInsights {
  key_findings: string[];
  anomalies: string[];
  trends: string[];
  opportunities: string[];
  risks: string[];
  correlations: string[];
  recommendations: string[];
}

export interface DashboardBlueprintPayload {
  dataset_id?: number;
  detected_domain?: string;
  recommended_kpis?: Record<string, any>[];
  chart_recommendations?: Record<string, any>[];
  dashboard_layout?: Record<string, any>;
  filter_options?: Record<string, any>[];
  ai_insights?: AIInsights;
}

export interface Dataset {
  id: string;
  datasetId: number;
  dashboardId?: number;
  name: string;
  columns: DataColumn[];
  rows: Record<string, any>[];
  uploadedAt: Date;
  rowCount: number;
  domain?: string | null;
  kpis: KPI[];
  charts: ChartConfig[];
  aiInsights: AIInsights;
  dashboardBlueprint?: DashboardBlueprintPayload;
  qualityMetrics?: Record<string, any>;
  statistics?: Record<string, any>;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  sql?: string;
  chart?: ChartConfig;
  insights?: string[];
  timestamp: Date;
}

const ICON_HINTS: Record<string, string> = {
  revenue: 'dollar',
  sales: 'dollar',
  payroll: 'dollar',
  expense: 'percent',
  order: 'package',
  inventory: 'package',
  rating: 'star',
  score: 'star',
  margin: 'percent',
  rate: 'percent',
};

export const getKpiIcon = (label: string): string => {
  const lower = label.toLowerCase();
  const matched = Object.entries(ICON_HINTS).find(([key]) => lower.includes(key));
  return matched?.[1] ?? 'dollar';
};
