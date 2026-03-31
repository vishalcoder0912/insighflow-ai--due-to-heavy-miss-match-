import { useState } from 'react';
import { Database, Globe, Link2, Table, Server, FileJson, Upload, CheckCircle, AlertCircle, Loader2, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { useToast } from '@/components/ui/use-toast';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

type SourceType = 'csv' | 'excel' | 'json' | 'url' | 'rest_api' | 'postgresql';

interface ImportResult {
  success: boolean;
  rows?: number;
  columns?: string[];
  preview?: any[];
  error?: string;
}

const ImportPage = () => {
  const { toast } = useToast();
  const [activeTab, setActiveTab] = useState<SourceType>('csv');
  const [isLoading, setIsLoading] = useState(false);
  const [importProgress, setImportProgress] = useState(0);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  
  // CSV/Excel/JSON upload state
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  
  // URL state
  const [url, setUrl] = useState('');
  
  // REST API state
  const [apiUrl, setApiUrl] = useState('');
  const [apiMethod, setApiMethod] = useState<'GET' | 'POST'>('GET');
  const [apiHeaders, setApiHeaders] = useState('');
  const [jsonPath, setJsonPath] = useState('');
  
  // PostgreSQL state
  const [connectionString, setConnectionString] = useState('');
  const [sqlQuery, setSqlQuery] = useState('');

  const sourceOptions = [
    { id: 'csv' as const, label: 'CSV File', icon: Table, description: 'Upload CSV files' },
    { id: 'excel' as const, label: 'Excel File', icon: Table, description: 'Upload Excel files (.xlsx, .xls)' },
    { id: 'json' as const, label: 'JSON File', icon: FileJson, description: 'Upload JSON files' },
    { id: 'url' as const, label: 'URL Import', icon: Globe, description: 'Import from web URL' },
    { id: 'rest_api' as const, label: 'REST API', icon: Link2, description: 'Connect to REST API' },
    { id: 'postgresql' as const, label: 'PostgreSQL', icon: Server, description: 'Query PostgreSQL database' },
  ];

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setSelectedFile(file);
      setImportResult(null);
    }
  };

  const handleFileImport = async () => {
    if (!selectedFile) return;
    
    setIsLoading(true);
    setImportProgress(0);
    setImportResult(null);
    
    const progressInterval = setInterval(() => {
      setImportProgress(prev => Math.min(prev + 15, 90));
    }, 300);
    
    try {
      const token = localStorage.getItem('insightflow.accessToken');
      const formData = new FormData();
      formData.append('file', selectedFile);
      
      const response = await fetch(`${API_URL}/datasets/upload`, {
        method: 'POST',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        body: formData,
      });
      
      if (!response.ok) throw new Error('Upload failed');
      
      clearInterval(progressInterval);
      setImportProgress(100);
      
      const data = await response.json();
      
      setImportResult({
        success: true,
        rows: data.row_count || 0,
        columns: data.columns || [],
      });
      
      toast({ title: 'File imported successfully!', variant: 'default' });
    } catch (error) {
      clearInterval(progressInterval);
      setImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Import failed',
      });
      toast({ title: 'Import failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsLoading(false);
      setImportProgress(0);
    }
  };

  const handleUrlImport = async () => {
    if (!url) return;
    
    setIsLoading(true);
    setImportProgress(0);
    setImportResult(null);
    
    const progressInterval = setInterval(() => {
      setImportProgress(prev => Math.min(prev + 15, 90));
    }, 300);
    
    try {
      const token = localStorage.getItem('insightflow.accessToken');
      const response = await fetch(`${API_URL}/import/url`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({ url, source_type: 'csv' }),
      });
      
      if (!response.ok) throw new Error('URL import failed');
      
      clearInterval(progressInterval);
      setImportProgress(100);
      
      const data = await response.json();
      
      setImportResult({
        success: true,
        rows: data.rows || 0,
        columns: data.columns || [],
        preview: data.data?.slice(0, 5) || [],
      });
      
      toast({ title: 'URL data imported successfully!' });
    } catch (error) {
      clearInterval(progressInterval);
      setImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Import failed',
      });
      toast({ title: 'Import failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsLoading(false);
      setImportProgress(0);
    }
  };

  const handleApiImport = async () => {
    if (!apiUrl) return;
    
    setIsLoading(true);
    setImportProgress(0);
    setImportResult(null);
    
    const progressInterval = setInterval(() => {
      setImportProgress(prev => Math.min(prev + 15, 90));
    }, 300);
    
    try {
      const token = localStorage.getItem('insightflow.accessToken');
      
      let headers: Record<string, string> = {};
      if (apiHeaders) {
        try {
          headers = JSON.parse(apiHeaders);
        } catch {
          toast({ title: 'Invalid headers JSON', variant: 'destructive' });
          return;
        }
      }
      
      const response = await fetch(`${API_URL}/import/rest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          url: apiUrl,
          method: apiMethod,
          headers,
          json_path: jsonPath,
        }),
      });
      
      if (!response.ok) throw new Error('API import failed');
      
      clearInterval(progressInterval);
      setImportProgress(100);
      
      const data = await response.json();
      
      setImportResult({
        success: true,
        rows: data.rows || 0,
        columns: data.columns || [],
        preview: data.data?.slice(0, 5) || [],
      });
      
      toast({ title: 'API data imported successfully!' });
    } catch (error) {
      clearInterval(progressInterval);
      setImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Import failed',
      });
      toast({ title: 'Import failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsLoading(false);
      setImportProgress(0);
    }
  };

  const handlePostgresImport = async () => {
    if (!connectionString || !sqlQuery) return;
    
    setIsLoading(true);
    setImportProgress(0);
    setImportResult(null);
    
    const progressInterval = setInterval(() => {
      setImportProgress(prev => Math.min(prev + 15, 90));
    }, 300);
    
    try {
      const token = localStorage.getItem('insightflow.accessToken');
      const response = await fetch(`${API_URL}/import/postgresql`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          connection_string: connectionString,
          query: sqlQuery,
        }),
      });
      
      if (!response.ok) throw new Error('Database import failed');
      
      clearInterval(progressInterval);
      setImportProgress(100);
      
      const data = await response.json();
      
      setImportResult({
        success: true,
        rows: data.rows || 0,
        columns: data.columns || [],
        preview: data.data?.slice(0, 5) || [],
      });
      
      toast({ title: 'Database data imported successfully!' });
    } catch (error) {
      clearInterval(progressInterval);
      setImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Import failed',
      });
      toast({ title: 'Import failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsLoading(false);
      setImportProgress(0);
    }
  };

  const renderImportForm = () => {
    switch (activeTab) {
      case 'csv':
      case 'excel':
      case 'json':
        return (
          <div className="space-y-6">
            <div className="border-2 border-dashed border-border rounded-lg p-8 text-center hover:border-primary/50 transition-colors">
              <input
                type="file"
                id="file-upload"
                className="hidden"
                accept={activeTab === 'csv' ? '.csv' : activeTab === 'excel' ? '.xlsx,.xls' : '.json'}
                onChange={handleFileSelect}
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <Upload className="w-12 h-12 mx-auto text-muted-foreground mb-4" />
                <p className="text-sm text-muted-foreground">
                  {selectedFile ? selectedFile.name : `Click to upload ${activeTab.toUpperCase()} file`}
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  Max file size: 100MB
                </p>
              </label>
            </div>
            <Button onClick={handleFileImport} disabled={!selectedFile || isLoading} className="w-full">
              {isLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
              Import File
            </Button>
          </div>
        );

      case 'url':
        return (
          <div className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="url">Data URL</Label>
              <Input
                id="url"
                placeholder="https://example.com/data.csv"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Supports CSV, Excel, and JSON files from public URLs
              </p>
            </div>
            <Button onClick={handleUrlImport} disabled={!url || isLoading} className="w-full">
              {isLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Globe className="w-4 h-4 mr-2" />}
              Import from URL
            </Button>
          </div>
        );

      case 'rest_api':
        return (
          <div className="space-y-6">
            <div className="space-y-2">
              <Label>API Method</Label>
              <Select value={apiMethod} onValueChange={(v) => setApiMethod(v as 'GET' | 'POST')}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="GET">GET</SelectItem>
                  <SelectItem value="POST">POST</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>API URL</Label>
              <Input placeholder="https://api.example.com/data" value={apiUrl} onChange={(e) => setApiUrl(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Headers (JSON)</Label>
              <Input placeholder='{"Authorization": "Bearer token"}' value={apiHeaders} onChange={(e) => setApiHeaders(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>JSON Path (optional)</Label>
              <Input placeholder="data.results" value={jsonPath} onChange={(e) => setJsonPath(e.target.value)} />
              <p className="text-xs text-muted-foreground">Dot notation to extract nested array from response</p>
            </div>
            <Button onClick={handleApiImport} disabled={!apiUrl || isLoading} className="w-full">
              {isLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Link2 className="w-4 h-4 mr-2" />}
              Import from API
            </Button>
          </div>
        );

      case 'postgresql':
        return (
          <div className="space-y-6">
            <div className="space-y-2">
              <Label>Connection String</Label>
              <Input
                placeholder="postgresql://user:pass@localhost:5432/dbname"
                value={connectionString}
                onChange={(e) => setConnectionString(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Format: postgresql://username:password@host:port/database
              </p>
            </div>
            <div className="space-y-2">
              <Label>SQL Query</Label>
              <textarea
                className="w-full h-24 px-3 py-2 bg-muted rounded-lg border border-input text-sm font-mono"
                placeholder="SELECT * FROM table_name LIMIT 10000"
                value={sqlQuery}
                onChange={(e) => setSqlQuery(e.target.value)}
              />
            </div>
            <Button onClick={handlePostgresImport} disabled={!connectionString || !sqlQuery || isLoading} className="w-full">
              {isLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Server className="w-4 h-4 mr-2" />}
              Execute Query
            </Button>
          </div>
        );
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-2rem)] m-4 overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto p-6 space-y-6">
          {/* Header */}
          <div>
            <h1 className="text-2xl font-bold text-foreground flex items-center gap-2">
              <Database className="w-6 h-6 text-primary" />
              Data Import
            </h1>
            <p className="text-sm text-muted-foreground mt-1">
              Import data from various sources - files, URLs, APIs, or databases
            </p>
          </div>

          {/* Source Selection */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Data Source</CardTitle>
              <CardDescription>Select where to import your data from</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                {sourceOptions.map((source) => (
                  <button
                    key={source.id}
                    onClick={() => {
                      setActiveTab(source.id);
                      setImportResult(null);
                    }}
                    className={`p-4 rounded-lg border text-left transition-all ${
                      activeTab === source.id
                        ? 'border-primary bg-primary/5 ring-2 ring-primary/20'
                        : 'border-border hover:border-primary/50 hover:bg-muted/50'
                    }`}
                  >
                    <source.icon className={`w-6 h-6 mb-2 ${activeTab === source.id ? 'text-primary' : 'text-muted-foreground'}`} />
                    <p className="font-medium text-sm">{source.label}</p>
                    <p className="text-xs text-muted-foreground mt-1">{source.description}</p>
                  </button>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* Import Form */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                {sourceOptions.find(s => s.id === activeTab)?.icon && (
                  <>
                    {activeTab === 'csv' && <Table className="w-5 h-5" />}
                    {activeTab === 'excel' && <Table className="w-5 h-5" />}
                    {activeTab === 'json' && <FileJson className="w-5 h-5" />}
                    {activeTab === 'url' && <Globe className="w-5 h-5" />}
                    {activeTab === 'rest_api' && <Link2 className="w-5 h-5" />}
                    {activeTab === 'postgresql' && <Server className="w-5 h-5" />}
                    Import from {sourceOptions.find(s => s.id === activeTab)?.label}
                  </>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {isLoading && (
                <div className="mb-4 space-y-2">
                  <Progress value={importProgress} className="h-2" />
                  <p className="text-xs text-muted-foreground text-center">Importing data...</p>
                </div>
              )}
              {renderImportForm()}
            </CardContent>
          </Card>

          {/* Import Result */}
          {importResult && (
            <Card className={importResult.success ? 'border-green-500/50' : 'border-red-500/50'}>
              <CardHeader className="pb-2">
                <CardTitle className="text-lg flex items-center gap-2">
                  {importResult.success ? (
                    <>
                      <CheckCircle className="w-5 h-5 text-green-500" />
                      Import Successful
                    </>
                  ) : (
                    <>
                      <AlertCircle className="w-5 h-5 text-red-500" />
                      Import Failed
                    </>
                  )}
                </CardTitle>
              </CardHeader>
              <CardContent>
                {importResult.success ? (
                  <div className="space-y-4">
                    <div className="grid grid-cols-3 gap-4">
                      <div className="bg-muted/30 rounded-lg p-3 text-center">
                        <p className="text-2xl font-bold text-primary">{importResult.rows?.toLocaleString()}</p>
                        <p className="text-xs text-muted-foreground">Rows</p>
                      </div>
                      <div className="bg-muted/30 rounded-lg p-3 text-center">
                        <p className="text-2xl font-bold text-primary">{importResult.columns?.length || 0}</p>
                        <p className="text-xs text-muted-foreground">Columns</p>
                      </div>
                      <div className="bg-muted/30 rounded-lg p-3 text-center">
                        <Badge variant="default">Ready</Badge>
                        <p className="text-xs text-muted-foreground mt-1">Status</p>
                      </div>
                    </div>
                    {importResult.preview && importResult.preview.length > 0 && (
                      <div>
                        <p className="text-sm font-medium mb-2">Data Preview (first 5 rows)</p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs border-collapse">
                            <thead>
                              <tr>
                                {importResult.columns?.map((col) => (
                                  <th key={col} className="border border-border p-2 bg-muted/50 text-left font-medium">
                                    {col}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {importResult.preview.map((row, i) => (
                                <tr key={i}>
                                  {importResult.columns?.map((col) => (
                                    <td key={col} className="border border-border p-2">
                                      {String(row[col] || '-').substring(0, 30)}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-red-500">{importResult.error}</p>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default ImportPage;
