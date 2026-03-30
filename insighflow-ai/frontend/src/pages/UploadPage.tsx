import { useState, useCallback } from 'react';
import { motion } from 'framer-motion';
import { Upload, CheckCircle2, AlertCircle, ArrowRight, Sparkles } from 'lucide-react';
import { useData } from '@/contexts/DataContext';
import { useNavigate } from 'react-router-dom';

const UploadPage = () => {
  const { dataset, uploadCSV, isReady, sessionEmail } = useData();
  const navigate = useNavigate();
  const [isDragging, setIsDragging] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFile = useCallback(async (file: File) => {
    if (!/\.(csv|json|xlsx|xls|parquet)$/i.test(file.name)) {
      setError('Please upload a CSV, JSON, Excel, or Parquet file');
      return;
    }
    setError(null);
    setIsUploading(true);
    try {
      await uploadCSV(file);
      navigate('/');
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to upload and analyze the dataset';
      setError(message);
      console.error('Upload error:', err);
    } finally {
      setIsUploading(false);
    }
  }, [navigate, uploadCSV]);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  }, [handleFile]);

  return (
    <div className="p-6 max-w-3xl mx-auto space-y-6">
      <motion.div initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }}>
        <h1 className="text-2xl font-bold text-foreground">Upload Data</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Upload a dataset and let the FastAPI backend analyze it using PostgreSQL-backed storage.
        </p>
        {sessionEmail && <p className="text-xs text-muted-foreground mt-2 font-mono">Connected session: {sessionEmail}</p>}
      </motion.div>

      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        onDragOver={(e) => {
          e.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={handleDrop}
        className={`relative border-2 border-dashed rounded-2xl p-12 text-center transition-colors cursor-pointer ${
          isDragging ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/50'
        }`}
        onClick={() => document.getElementById('file-input')?.click()}
      >
        <input
          id="file-input"
          type="file"
          accept=".csv,.json,.xlsx,.xls,.parquet"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleFile(file);
          }}
        />
        <div className="flex flex-col items-center gap-4">
          <div className={`w-16 h-16 rounded-2xl flex items-center justify-center transition-colors ${
            isDragging ? 'bg-primary/20' : 'bg-muted'
          }`}>
            <Upload className={`w-8 h-8 ${isDragging ? 'text-primary' : 'text-muted-foreground'}`} />
          </div>
          <div>
            <p className="text-sm font-medium text-foreground">
              {isUploading ? 'Uploading and analyzing...' : 'Drop your dataset here or click to browse'}
            </p>
            <p className="text-xs text-muted-foreground mt-1">Supports CSV, JSON, Excel, and Parquet files up to 100MB</p>
          </div>
        </div>
      </motion.div>

      {!isReady && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-center text-sm text-muted-foreground">
          Initializing backend session...
        </motion.div>
      )}

      {error && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex items-center gap-2 text-destructive text-sm">
          <AlertCircle className="w-4 h-4" />
          {error}
        </motion.div>
      )}

      {dataset && (
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass rounded-xl p-5 space-y-4"
        >
          <div className="flex items-center gap-3">
            <CheckCircle2 className="w-5 h-5 text-success" />
            <div>
              <p className="text-sm font-semibold text-foreground">{dataset.name}</p>
              <p className="text-xs text-muted-foreground font-mono">
                {dataset.rowCount.toLocaleString()} rows · {dataset.columns.length} columns · {dataset.domain ?? 'unclassified'}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            {dataset.columns.map((col) => (
              <span key={col.name} className="px-2 py-1 rounded-md bg-muted text-xs font-mono text-muted-foreground">
                {col.name} <span className="text-primary">({col.type})</span>
              </span>
            ))}
          </div>

          <div className="flex gap-2">
            <button
              onClick={() => navigate('/')}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-secondary text-secondary-foreground text-sm hover:bg-secondary/80 transition-colors"
            >
              View Dashboard <ArrowRight className="w-3 h-3" />
            </button>
            <button
              onClick={() => navigate('/chat')}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm hover:bg-primary/90 transition-colors"
            >
              <Sparkles className="w-4 h-4" />
              Ask AI <ArrowRight className="w-3 h-3" />
            </button>
          </div>
        </motion.div>
      )}
    </div>
  );
};

export default UploadPage;
