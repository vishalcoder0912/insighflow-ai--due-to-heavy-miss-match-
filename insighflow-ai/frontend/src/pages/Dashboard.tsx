import { Suspense, lazy } from 'react';
import { motion } from 'framer-motion';
import { useData } from '@/contexts/DataContext';
import KPICard from '@/components/kpi/KPICard';
import { Table2, ArrowRight, Upload } from 'lucide-react';
import { Link } from 'react-router-dom';

const AnalyticsChart = lazy(() => import('@/components/charts/AnalyticsChart'));

const ChartFallback = () => <div className="glass h-64 rounded-xl animate-pulse" />;

const Dashboard = () => {
  const { dataset, isReady } = useData();

  if (!isReady) {
    return <div className="p-6 text-sm text-muted-foreground">Connecting to backend and PostgreSQL...</div>;
  }

  if (!dataset) {
    return (
      <div className="p-6 space-y-4">
        <h1 className="text-2xl font-bold text-foreground">Dashboard</h1>
        <p className="text-sm text-muted-foreground">No analyzed dataset is loaded yet.</p>
        <Link
          to="/upload"
          className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
        >
          <Upload className="w-4 h-4" />
          Upload Dataset
        </Link>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        className="flex items-center justify-between"
      >
        <div>
          <h1 className="text-2xl font-bold text-foreground">Dashboard</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Analyzing <span className="font-mono text-primary">{dataset.name}</span> · {dataset.rowCount.toLocaleString()} records · {dataset.domain ?? 'unclassified'}
          </p>
        </div>
        <div className="flex gap-2">
          <Link
            to="/chat"
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
          >
            Ask AI
            <ArrowRight className="w-3 h-3" />
          </Link>
        </div>
      </motion.div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {dataset.kpis.map((kpi, i) => (
          <KPICard key={kpi.id} kpi={kpi} index={i} />
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {dataset.charts.map((chart, i) => (
          <Suspense key={chart.id} fallback={<ChartFallback />}>
            <AnalyticsChart config={chart} index={i} />
          </Suspense>
        ))}
      </div>

      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.5 }}
        className="glass rounded-xl p-5"
      >
        <div className="flex items-center gap-2 mb-4">
          <Table2 className="w-4 h-4 text-primary" />
          <h3 className="text-sm font-semibold text-foreground">Data Preview</h3>
          <span className="text-xs text-muted-foreground font-mono ml-auto">
            {dataset.columns.length} columns · first {Math.min(dataset.rows.length, 20)} rows
          </span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border">
                {dataset.columns.map((col) => (
                  <th key={col.name} className="text-left py-2 px-3 font-mono text-muted-foreground font-medium">
                    {col.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {dataset.rows.slice(0, 10).map((row, i) => (
                <tr key={i} className="border-b border-border/50 hover:bg-muted/30 transition-colors">
                  {dataset.columns.map((col) => (
                    <td key={col.name} className="py-2 px-3 font-mono text-foreground">
                      {typeof row[col.name] === 'number' ? row[col.name].toLocaleString() : String(row[col.name] ?? '')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </motion.div>
    </div>
  );
};

export default Dashboard;
