import { Dataset, KPI, ChartConfig, AIInsights, DataColumn, DashboardBlueprintPayload, getKpiIcon } from '@/lib/data-store';

const API_BASE_URL = (import.meta.env.VITE_API_URL ?? '/api/v1').replace(/\/$/, '');
const STORAGE_KEYS = {
  email: 'insightflow.email',
  password: 'insightflow.password',
  accessToken: 'insightflow.accessToken',
  refreshToken: 'insightflow.refreshToken',
  dataset: 'insightflow.dataset',
};

type AuthPayload = {
  user: Record<string, any>;
  tokens: {
    access_token: string;
    refresh_token: string;
  };
};

type RequestOptions = RequestInit & {
  token?: string;
};

const randomId = () => Math.random().toString(36).slice(2, 10);

const inferColumnType = (dataType?: string): DataColumn['type'] => {
  if (dataType === 'numerical') return 'number';
  if (dataType === 'temporal') return 'date';
  if (dataType === 'boolean') return 'boolean';
  return 'string';
};

const toChartType = (frontendType?: string): ChartConfig['type'] => {
  if (frontendType === 'pie') return 'pie';
  if (frontendType === 'area') return 'area';
  if (frontendType === 'line') return 'line';
  return 'bar';
};

async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { token, headers, ...rest } = options;
  const isFormData = rest.body instanceof FormData;
  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...rest,
      headers: {
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(isFormData ? {} : headers),
      },
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `Request failed with status ${response.status}`);
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return response.json() as Promise<T>;
  } catch (error) {
    if (error instanceof Error && error.message === 'Failed to fetch') {
      throw new Error('Backend server is not reachable. Please ensure the backend server is running.');
    }
    throw error;
  }
}

const buildStoredCredentials = () => {
  const existingEmail = localStorage.getItem(STORAGE_KEYS.email);
  const existingPassword = localStorage.getItem(STORAGE_KEYS.password);
  if (existingEmail && existingPassword && !existingEmail.endsWith('@insightflow.local')) {
    return { email: existingEmail, password: existingPassword };
  }
  const email = `demo-${randomId()}@gmail.com`;
  const password = `InsightFlow#${randomId()}2026`;
  localStorage.setItem(STORAGE_KEYS.email, email);
  localStorage.setItem(STORAGE_KEYS.password, password);
  return { email, password };
};

const persistTokens = (tokens: AuthPayload['tokens']) => {
  localStorage.setItem(STORAGE_KEYS.accessToken, tokens.access_token);
  localStorage.setItem(STORAGE_KEYS.refreshToken, tokens.refresh_token);
};

export async function ensureSession(): Promise<string> {
  const existingToken = localStorage.getItem(STORAGE_KEYS.accessToken);
  if (existingToken) {
    try {
      await apiRequest('/users/me', { token: existingToken });
      return existingToken;
    } catch {
      localStorage.removeItem(STORAGE_KEYS.accessToken);
    }
  }

  const credentials = buildStoredCredentials();
  const registerPayload = {
    email: credentials.email,
    full_name: 'InsightFlow Demo User',
    password: credentials.password,
  };

  let authResponse: AuthPayload;
  try {
    authResponse = await apiRequest<AuthPayload>('/auth/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(registerPayload),
    });
  } catch {
    authResponse = await apiRequest<AuthPayload>('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: credentials.email, password: credentials.password }),
    });
  }

  persistTokens(authResponse.tokens);
  return authResponse.tokens.access_token;
}

const defaultInsights: AIInsights = {
  key_findings: [],
  anomalies: [],
  trends: [],
  opportunities: [],
  risks: [],
  correlations: [],
  recommendations: [],
};

function mapKpis(items: Record<string, any>[] = []): KPI[] {
  return items.map((item) => ({
    id: item.id,
    label: item.title,
    value: item.value,
    icon: getKpiIcon(item.title),
    description: item.description,
    businessImpact: item.business_impact,
  }));
}

function mapCharts(items: Record<string, any>[] = []): ChartConfig[] {
  return items
    .filter((item) => Array.isArray(item.data) && item.data.length > 0)
    .map((item) => ({
      id: item.id,
      type: toChartType(item.frontend_type),
      title: item.title,
      xKey: item.x_key ?? 'label',
      yKey: item.y_key ?? 'value',
      data: item.data,
      rationale: item.rationale,
    }));
}

export function mapDatasetPayload(payload: Record<string, any>, dashboardPayload?: Record<string, any>): Dataset {
  const preview = payload.data_preview ?? {};
  const schema = payload.schema ?? payload.columns_schema ?? [];
  const resolvedKpis = dashboardPayload?.recommended_kpis ?? payload.recommended_kpis ?? [];
  const resolvedCharts = dashboardPayload?.chart_recommendations ?? payload.chart_recommendations ?? [];
  const aiInsights = (dashboardPayload?.ai_insights ?? payload.ai_insights ?? defaultInsights) as AIInsights;
  const rows = preview.sample_rows ?? [];

  return {
    id: `dataset-${payload.dataset_id}`,
    datasetId: Number(payload.dataset_id),
    name: String(payload.filename ?? 'Uploaded Dataset').replace(/\.[^/.]+$/, ''),
    columns: schema.map((column: Record<string, any>) => ({
      name: column.name ?? column.column_name,
      type: inferColumnType(column.inferred_type ?? column.data_type),
      sample: column.sample_values ?? [],
      subtype: column.subtype,
    })),
    rows,
    uploadedAt: new Date(),
    rowCount: payload.row_count ?? rows.length,
    domain: payload.detected_domain,
    kpis: mapKpis(resolvedKpis),
    charts: mapCharts(resolvedCharts),
    aiInsights,
    dashboardBlueprint: dashboardPayload as DashboardBlueprintPayload | undefined,
    qualityMetrics: preview.data_quality_metrics ?? {},
    statistics: preview.column_statistics ?? {},
  };
}

export async function uploadAndAnalyzeDataset(file: File): Promise<Dataset> {
  const token = await ensureSession();
  const formData = new FormData();
  formData.append('file', file);
  const uploadResponse = await apiRequest<{ dataset_id: number }>('/datasets/upload', {
    method: 'POST',
    body: formData,
    token,
  });
  const analysis = await apiRequest<Record<string, any>>(`/datasets/${uploadResponse.dataset_id}/analyze`, {
    method: 'POST',
    token,
  });
  const dashboard = await apiRequest<{ dashboard_id: number; blueprint: Record<string, any> }>('/dashboards/generate', {
    method: 'POST',
    token,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dataset_id: uploadResponse.dataset_id }),
  });
  const dataset = mapDatasetPayload(analysis, dashboard.blueprint);
  dataset.dashboardId = dashboard.dashboard_id;
  localStorage.setItem(STORAGE_KEYS.dataset, JSON.stringify(analysis));
  return dataset;
}

export async function loadPersistedDataset(): Promise<Dataset | null> {
  const stored = localStorage.getItem(STORAGE_KEYS.dataset);
  if (!stored) {
    return null;
  }
  try {
    return mapDatasetPayload(JSON.parse(stored));
  } catch {
    localStorage.removeItem(STORAGE_KEYS.dataset);
    return null;
  }
}
