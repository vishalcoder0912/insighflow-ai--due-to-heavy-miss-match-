import { motion } from 'framer-motion';
import { TrendingUp, TrendingDown, DollarSign, Package, Percent, Star } from 'lucide-react';
import { KPI } from '@/lib/data-store';

const iconMap: Record<string, React.ElementType> = {
  dollar: DollarSign,
  package: Package,
  percent: Percent,
  star: Star,
};

interface KPICardProps {
  kpi: KPI;
  index: number;
}

const KPICard = ({ kpi, index }: KPICardProps) => {
  const Icon = iconMap[kpi.icon] || DollarSign;
  const isPositive = (kpi.change ?? 0) >= 0;

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.08, duration: 0.4 }}
      className="glass rounded-xl p-5 hover:border-primary/30 transition-colors group"
    >
      <div className="flex items-start justify-between mb-3">
        <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center group-hover:bg-primary/20 transition-colors">
          <Icon className="w-5 h-5 text-primary" />
        </div>
        {kpi.change !== undefined && (
          <div className={`flex items-center gap-1 text-xs font-mono ${isPositive ? 'text-success' : 'text-destructive'}`}>
            {isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
            {Math.abs(kpi.change)}%
          </div>
        )}
      </div>
      <p className="text-2xl font-bold text-foreground font-mono">{kpi.value}</p>
      <p className="text-xs text-muted-foreground mt-1">{kpi.label}</p>
    </motion.div>
  );
};

export default KPICard;
