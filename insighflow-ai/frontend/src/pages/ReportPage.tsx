import { useState, useEffect } from 'react';
import { useData } from '@/contexts/DataContext';
import { FileText, Download, FileSpreadsheet, BarChart3, TrendingUp, Users, AlertTriangle, CheckCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Link } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

const ReportPage = () => {
  const { dataset } = useData();
  const [reportType, setReportType] = useState<'pdf' | 'excel'>('pdf');
  const [includeSummary, setIncludeSummary] = useState(true);
  const [includeStatistics, setIncludeStatistics] = useState(true);
  const [includeCharts, setIncludeCharts] = useState(true);
  const [includeRecommendations, setIncludeRecommendations] = useState(true);
  const [isGenerating, setIsGenerating] = useState(false);
  const [reportProgress, setReportProgress] = useState(0);
  const [lastGenerated, setLastGenerated] = useState<Date | null>(null);

  const handleGenerateReport = async () => {
    if (!dataset) return;

    setIsGenerating(true);
    setReportProgress(0);

    const progressInterval = setInterval(() => {
      setReportProgress(prev => Math.min(prev + 10, 90));
    }, 200);

    try {
      const token = localStorage.getItem('insightflow.accessToken');
      
      const response = await fetch(`${API_URL}/analytics/reports/${reportType}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          dataset_id: dataset.datasetId,
          include_summary: includeSummary,
          include_statistics: includeStatistics,
          include_charts: includeCharts,
          include_recommendations: includeRecommendations,
        }),
      });

      if (!response.ok) throw new Error('Failed to generate report');

      clearInterval(progressInterval);
      setReportProgress(100);

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${dataset.name}_report.${reportType === 'pdf' ? 'pdf' : 'xlsx'}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);

      setLastGenerated(new Date());
    } catch (error) {
      console.error('Report generation failed:', error);
    } finally {
      clearInterval(progressInterval);
      setIsGenerating(false);
      setReportProgress(0);
    }
  };

  if (!dataset) {
    return (
      <div className="flex flex-col h-[calc(100vh-2rem)] m-4 glass rounded-2xl overflow-hidden items-center justify-center text-center p-6">
        <FileText className="w-16 h-16 text-muted-foreground mb-4" />
        <h2 className="text-lg font-semibold text-foreground mb-2">No dataset loaded</h2>
        <p className="text-sm text-muted-foreground mb-4">Upload a dataset first to generate reports.</p>
        <Link to="/upload">
          <Button>Upload Dataset</Button>
        </Link>
      </div>
    );
  }

  const reportFeatures = [
    {
      icon: CheckCircle,
      title: 'Executive Summary',
      description: 'Key metrics and overview',
      enabled: includeSummary,
      onChange: setIncludeSummary,
    },
    {
      icon: BarChart3,
      title: 'Column Statistics',
      description: 'Data type, null counts, distributions',
      enabled: includeStatistics,
      onChange: setIncludeStatistics,
    },
    {
      icon: TrendingUp,
      title: 'Visual Charts',
      description: 'Auto-generated chart visualizations',
      enabled: includeCharts,
      onChange: setIncludeCharts,
    },
    {
      icon: AlertTriangle,
      title: 'Recommendations',
      description: 'AI-powered insights and suggestions',
      enabled: includeRecommendations,
      onChange: setIncludeRecommendations,
    },
  ];

  return (
    <div className="flex flex-col h-[calc(100vh-2rem)] m-4 overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto p-6 space-y-6">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-foreground flex items-center gap-2">
                <FileText className="w-6 h-6 text-primary" />
                Report Export
              </h1>
              <p className="text-sm text-muted-foreground mt-1">
                Generate professional PDF or Excel reports from your analyzed data
              </p>
            </div>
            {lastGenerated && (
              <Badge variant="outline" className="text-xs">
                Last generated: {lastGenerated.toLocaleTimeString()}
              </Badge>
            )}
          </div>

          {/* Report Type Selection */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Report Format</CardTitle>
              <CardDescription>Choose your preferred export format</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={reportType} onValueChange={(v) => setReportType(v as 'pdf' | 'excel')}>
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="pdf" className="flex items-center gap-2">
                    <FileText className="w-4 h-4" />
                    PDF Report
                  </TabsTrigger>
                  <TabsTrigger value="excel" className="flex items-center gap-2">
                    <FileSpreadsheet className="w-4 h-4" />
                    Excel Report
                  </TabsTrigger>
                </TabsList>
                <TabsContent value="pdf" className="mt-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <h4 className="font-medium mb-2">PDF Report Includes:</h4>
                    <ul className="text-sm text-muted-foreground space-y-1">
                      <li>• Professional formatting suitable for presentations</li>
                      <li>• Charts and visualizations embedded</li>
                      <li>• Executive summary section</li>
                      <li>• Key findings and recommendations</li>
                    </ul>
                  </div>
                </TabsContent>
                <TabsContent value="excel" className="mt-4">
                  <div className="bg-muted/50 rounded-lg p-4">
                    <h4 className="font-medium mb-2">Excel Report Includes:</h4>
                    <ul className="text-sm text-muted-foreground space-y-1">
                      <li>• Multiple worksheets with different sections</li>
                      <li>• Raw data for further analysis</li>
                      <li>• Summary statistics table</li>
                      <li>• Easy to share and edit</li>
                    </ul>
                  </div>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>

          {/* Report Content Options */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Report Content</CardTitle>
              <CardDescription>Select what to include in your report</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {reportFeatures.map((feature) => (
                <div key={feature.title} className="flex items-center justify-between p-4 bg-muted/30 rounded-lg">
                  <div className="flex items-center gap-4">
                    <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                      <feature.icon className="w-5 h-5 text-primary" />
                    </div>
                    <div>
                      <h4 className="font-medium">{feature.title}</h4>
                      <p className="text-xs text-muted-foreground">{feature.description}</p>
                    </div>
                  </div>
                  <Switch checked={feature.enabled} onCheckedChange={feature.onChange} />
                </div>
              ))}
            </CardContent>
          </Card>

          {/* Dataset Info */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Dataset Information</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-muted/30 rounded-lg p-3">
                  <p className="text-xs text-muted-foreground">Dataset</p>
                  <p className="font-medium truncate">{dataset.name}</p>
                </div>
                <div className="bg-muted/30 rounded-lg p-3">
                  <p className="text-xs text-muted-foreground">Rows</p>
                  <p className="font-medium">{dataset.rowCount.toLocaleString()}</p>
                </div>
                <div className="bg-muted/30 rounded-lg p-3">
                  <p className="text-xs text-muted-foreground">Columns</p>
                  <p className="font-medium">{dataset.columns.length}</p>
                </div>
                <div className="bg-muted/30 rounded-lg p-3">
                  <p className="text-xs text-muted-foreground">Domain</p>
                  <p className="font-medium capitalize">{dataset.domain || 'General'}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Generate Button */}
          <div className="space-y-4">
            {isGenerating && (
              <div className="space-y-2">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Generating report...</span>
                  <span className="font-medium">{reportProgress}%</span>
                </div>
                <Progress value={reportProgress} className="h-2" />
              </div>
            )}
            
            <Button 
              onClick={handleGenerateReport} 
              disabled={isGenerating}
              className="w-full h-12 text-base"
              size="lg"
            >
              {isGenerating ? (
                <>Generating...</>
              ) : (
                <>
                  <Download className="w-4 h-4 mr-2" />
                  Generate {reportType === 'pdf' ? 'PDF' : 'Excel'} Report
                </>
              )}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ReportPage;
