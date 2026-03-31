import { useState, useEffect } from 'react';
import { useData } from '@/contexts/DataContext';
import { Brain, TrendingUp, Users, BarChart3, PieChart, Activity, Loader2, AlertCircle, CheckCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { useToast } from '@/components/ui/use-toast';
import { Link } from 'react-router-dom';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

interface AnalysisResult {
  status: string;
  results?: any;
  confidence?: string;
  quality_score?: number;
  warnings?: string[];
}

interface RFMResult extends AnalysisResult {
  segments?: { segment: string; customers: number; avg_monetary: number }[];
  customer_count?: number;
}

interface CohortResult extends AnalysisResult {
  cohort_count?: number;
  average_retention?: number;
  retention_matrix?: { cohort_period: string; values: Record<string, number> }[];
}

const AnalyticsPage = () => {
  const { dataset } = useData();
  const { toast } = useToast();
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analysisProgress, setAnalysisProgress] = useState(0);
  const [activeAnalysis, setActiveAnalysis] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, AnalysisResult>>({});

  // RFM config
  const [rfmCustomerCol, setRfmCustomerCol] = useState('');
  const [rfmDateCol, setRfmDateCol] = useState('');
  const [rfmAmountCol, setRfmAmountCol] = useState('');

  // Cohort config
  const [cohortCohortCol, setCohortCohortCol] = useState('');
  const [cohortTimeCol, setCohortTimeCol] = useState('');
  const [cohortMetricCol, setCohortMetricCol] = useState('');

  const numericColumns = dataset?.columns.filter(c => c.type === 'number') || [];
  const dateColumns = dataset?.columns.filter(c => c.type === 'date') || [];
  const allColumns = dataset?.columns || [];

  useEffect(() => {
    if (numericColumns.length > 0 && !rfmAmountCol) {
      setRfmAmountCol(numericColumns[0].name);
    }
    if (dateColumns.length > 0) {
      if (!rfmDateCol) setRfmDateCol(dateColumns[0].name);
      if (!cohortTimeCol) setCohortTimeCol(dateColumns[0].name);
    }
    if (allColumns.length > 0) {
      if (!rfmCustomerCol) setRfmCustomerCol(allColumns[0].name);
      if (!cohortCohortCol) setCohortCohortCol(allColumns[0].name);
      if (!cohortMetricCol) setCohortMetricCol(numericColumns[0]?.name || allColumns[0].name);
    }
  }, [dataset]);

  const runAnalysis = async (analysisType: string) => {
    if (!dataset?.datasetId) return;

    setIsAnalyzing(true);
    setActiveAnalysis(analysisType);
    setAnalysisProgress(0);

    const progressInterval = setInterval(() => {
      setAnalysisProgress(prev => Math.min(prev + 8, 90));
    }, 400);

    try {
      const token = localStorage.getItem('insightflow.accessToken');
      
      let options = {};
      
      if (analysisType === 'rfm') {
        options = {
          customer_column: rfmCustomerCol,
          date_column: rfmDateCol,
          amount_column: rfmAmountCol,
        };
      } else if (analysisType === 'cohort') {
        options = {
          cohort_column: cohortCohortCol,
          time_column: cohortTimeCol,
          metric_column: cohortMetricCol,
        };
      }
      
      const response = await fetch(`${API_URL}/advanced-analytics/run`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          dataset_id: dataset.datasetId,
          analyses: [analysisType],
          options,
        }),
      });

      if (!response.ok) throw new Error('Analysis failed');

      const data = await response.json();
      clearInterval(progressInterval);
      setAnalysisProgress(100);

      if (data.results && data.results[analysisType]) {
        setResults(prev => ({
          ...prev,
          [analysisType]: data.results[analysisType],
        }));
        toast({ title: `${analysisType.toUpperCase()} analysis completed!`, variant: 'default' });
      } else if (data.failures && data.failures.length > 0) {
        toast({ title: 'Analysis failed', description: data.failures[0].message, variant: 'destructive' });
      }
    } catch (error) {
      clearInterval(progressInterval);
      toast({ title: 'Analysis failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsAnalyzing(false);
      setAnalysisProgress(0);
      setActiveAnalysis(null);
    }
  };

  const runAllAnalyses = async () => {
    if (!dataset?.datasetId) return;

    setIsAnalyzing(true);
    setActiveAnalysis('all');
    setAnalysisProgress(0);

    const progressInterval = setInterval(() => {
      setAnalysisProgress(prev => Math.min(prev + 5, 95));
    }, 500);

    try {
      const token = localStorage.getItem('insightflow.accessToken');
      
      const response = await fetch(`${API_URL}/advanced-analytics/run`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          dataset_id: dataset.datasetId,
          analyses: ['rfm', 'cohort', 'clustering', 'regression'],
        }),
      });

      if (!response.ok) throw new Error('Analysis failed');

      const data = await response.json();
      clearInterval(progressInterval);
      setAnalysisProgress(100);

      if (data.results) {
        setResults(data.results);
        toast({ title: 'All analyses completed!', variant: 'default' });
      }
    } catch (error) {
      clearInterval(progressInterval);
      toast({ title: 'Analysis failed', description: String(error), variant: 'destructive' });
    } finally {
      setIsAnalyzing(false);
      setAnalysisProgress(0);
      setActiveAnalysis(null);
    }
  };

  if (!dataset) {
    return (
      <div className="flex flex-col h-[calc(100vh-2rem)] m-4 glass rounded-2xl overflow-hidden items-center justify-center text-center p-6">
        <Brain className="w-16 h-16 text-muted-foreground mb-4" />
        <h2 className="text-lg font-semibold text-foreground mb-2">No dataset loaded</h2>
        <p className="text-sm text-muted-foreground mb-4">Upload a dataset first to run advanced analytics.</p>
        <Link to="/upload">
          <Button>Upload Dataset</Button>
        </Link>
      </div>
    );
  }

  const renderRFMSection = () => {
    const rfmResult = results.rfm as RFMResult | undefined;
    
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="w-5 h-5 text-primary" />
            RFM Analysis
          </CardTitle>
          <CardDescription>Recency, Frequency, Monetary customer segmentation</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {!rfmResult ? (
            <>
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label>Customer Column</Label>
                  <Select value={rfmCustomerCol} onValueChange={setRfmCustomerCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {allColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Date Column</Label>
                  <Select value={rfmDateCol} onValueChange={setRfmDateCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {dateColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Amount Column</Label>
                  <Select value={rfmAmountCol} onValueChange={setRfmAmountCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {numericColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <Button onClick={() => runAnalysis('rfm')} disabled={isAnalyzing} className="w-full">
                {isAnalyzing && activeAnalysis === 'rfm' ? (
                  <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</>
                ) : (
                  <><Activity className="w-4 h-4 mr-2" />Run RFM Analysis</>
                )}
              </Button>
            </>
          ) : (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <Badge variant="outline" className="text-green-500 border-green-500">
                  <CheckCircle className="w-3 h-3 mr-1" /> Analysis Complete
                </Badge>
                <Button variant="outline" size="sm" onClick={() => runAnalysis('rfm')} disabled={isAnalyzing}>
                  Re-run
                </Button>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-muted/30 rounded-lg p-4 text-center">
                  <p className="text-3xl font-bold text-primary">{rfmResult.results?.customer_count?.toLocaleString() || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Customers</p>
                </div>
                <div className="bg-muted/30 rounded-lg p-4 text-center">
                  <p className="text-3xl font-bold text-primary">{rfmResult.confidence || 'N/A'}</p>
                  <p className="text-xs text-muted-foreground">Confidence</p>
                </div>
              </div>
              {rfmResult.results?.segments && (
                <div>
                  <p className="text-sm font-medium mb-2">Customer Segments</p>
                  <div className="space-y-2">
                    {rfmResult.results.segments.slice(0, 5).map((seg: any, i: number) => (
                      <div key={i} className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
                        <div className="flex items-center gap-2">
                          <div className={`w-3 h-3 rounded-full ${
                            seg.segment === 'champions' ? 'bg-green-500' :
                            seg.segment === 'loyal' ? 'bg-blue-500' :
                            seg.segment === 'at_risk' ? 'bg-orange-500' :
                            seg.segment === 'new_customers' ? 'bg-purple-500' :
                            'bg-gray-500'
                          }`} />
                          <span className="font-medium capitalize">{seg.segment.replace('_', ' ')}</span>
                        </div>
                        <div className="text-right">
                          <p className="font-medium">{seg.customers?.toLocaleString() || 0}</p>
                          <p className="text-xs text-muted-foreground">${seg.avg_monetary?.toFixed(2) || 0} avg</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    );
  };

  const renderCohortSection = () => {
    const cohortResult = results.cohort as CohortResult | undefined;
    
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <PieChart className="w-5 h-5 text-primary" />
            Cohort Analysis
          </CardTitle>
          <CardDescription>Track retention and value over time periods</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {!cohortResult ? (
            <>
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label>Cohort Column</Label>
                  <Select value={cohortCohortCol} onValueChange={setCohortCohortCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {allColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Time Column</Label>
                  <Select value={cohortTimeCol} onValueChange={setCohortTimeCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {dateColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Metric Column</Label>
                  <Select value={cohortMetricCol} onValueChange={setCohortMetricCol}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {numericColumns.map(col => (
                        <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <Button onClick={() => runAnalysis('cohort')} disabled={isAnalyzing} className="w-full">
                {isAnalyzing && activeAnalysis === 'cohort' ? (
                  <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</>
                ) : (
                  <><Activity className="w-4 h-4 mr-2" />Run Cohort Analysis</>
                )}
              </Button>
            </>
          ) : (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <Badge variant="outline" className="text-green-500 border-green-500">
                  <CheckCircle className="w-3 h-3 mr-1" /> Analysis Complete
                </Badge>
                <Button variant="outline" size="sm" onClick={() => runAnalysis('cohort')} disabled={isAnalyzing}>
                  Re-run
                </Button>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-muted/30 rounded-lg p-4 text-center">
                  <p className="text-3xl font-bold text-primary">{cohortResult.results?.cohort_count || 0}</p>
                  <p className="text-xs text-muted-foreground">Total Cohorts</p>
                </div>
                <div className="bg-muted/30 rounded-lg p-4 text-center">
                  <p className="text-3xl font-bold text-primary">{((cohortResult.results?.average_retention || 0) * 100).toFixed(1)}%</p>
                  <p className="text-xs text-muted-foreground">Avg Retention</p>
                </div>
              </div>
              {cohortResult.results?.retention_matrix && (
                <div>
                  <p className="text-sm font-medium mb-2">Retention Matrix</p>
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr>
                          <th className="border border-border p-2 bg-muted/50">Cohort</th>
                          <th className="border border-border p-2 bg-muted/50">Period 0</th>
                          <th className="border border-border p-2 bg-muted/50">Period 1</th>
                          <th className="border border-border p-2 bg-muted/50">Period 2</th>
                        </tr>
                      </thead>
                      <tbody>
                        {cohortResult.results.retention_matrix.slice(0, 5).map((row: any, i: number) => (
                          <tr key={i}>
                            <td className="border border-border p-2">{row.cohort_period}</td>
                            <td className="border border-border p-2">{(row.values['0'] * 100).toFixed(1)}%</td>
                            <td className="border border-border p-2">{(row.values['1'] * 100).toFixed(1)}%</td>
                            <td className="border border-border p-2">{(row.values['2'] * 100).toFixed(1)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    );
  };

  const renderClusteringSection = () => {
    const clusterResult = results.clustering;
    
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="w-5 h-5 text-primary" />
            Clustering
          </CardTitle>
          <CardDescription>K-Means clustering for data segmentation</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {!clusterResult ? (
            <Button onClick={() => runAnalysis('clustering')} disabled={isAnalyzing} className="w-full">
              {isAnalyzing && activeAnalysis === 'clustering' ? (
                <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</>
              ) : (
                <><Activity className="w-4 h-4 mr-2" />Run Clustering</>
              )}
            </Button>
          ) : (
            <div className="space-y-4">
              <Badge variant="outline" className="text-green-500 border-green-500">
                <CheckCircle className="w-3 h-3 mr-1" /> Analysis Complete
              </Badge>
              <p className="text-sm text-muted-foreground">
                Clustering analysis found natural groupings in your data. 
                {clusterResult.results?.cluster_count ? ` ${clusterResult.results.cluster_count} clusters identified.` : ''}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    );
  };

  const renderRegressionSection = () => {
    const regResult = results.regression;
    
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-primary" />
            Regression Analysis
          </CardTitle>
          <CardDescription>Linear regression for prediction and trends</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {!regResult ? (
            <Button onClick={() => runAnalysis('regression')} disabled={isAnalyzing} className="w-full">
              {isAnalyzing && activeAnalysis === 'regression' ? (
                <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</>
              ) : (
                <><Activity className="w-4 h-4 mr-2" />Run Regression</>
              )}
            </Button>
          ) : (
            <div className="space-y-4">
              <Badge variant="outline" className="text-green-500 border-green-500">
                <CheckCircle className="w-3 h-3 mr-1" /> Analysis Complete
              </Badge>
              <p className="text-sm text-muted-foreground">
                Regression analysis completed. 
                {regResult.results?.r_squared ? ` R² score: ${regResult.results.r_squared.toFixed(3)}` : ''}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    );
  };

  return (
    <div className="flex flex-col h-[calc(100vh-2rem)] m-4 overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-5xl mx-auto p-6 space-y-6">
          {/* Header */}
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-foreground flex items-center gap-2">
                <Brain className="w-6 h-6 text-primary" />
                Advanced Analytics
              </h1>
              <p className="text-sm text-muted-foreground mt-1">
                Run ML-powered analysis: RFM, Cohort, Clustering, Regression
              </p>
            </div>
            <Button onClick={runAllAnalyses} disabled={isAnalyzing} size="lg">
              {isAnalyzing ? (
                <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Running...</>
              ) : (
                <><Activity className="w-4 h-4 mr-2" />Run All Analyses</>
              )}
            </Button>
          </div>

          {/* Progress */}
          {isAnalyzing && (
            <Card>
              <CardContent className="pt-6">
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">
                      {activeAnalysis === 'all' ? 'Running all analyses...' : `Running ${activeAnalysis} analysis...`}
                    </span>
                    <span className="font-medium">{analysisProgress}%</span>
                  </div>
                  <Progress value={analysisProgress} className="h-2" />
                </div>
              </CardContent>
            </Card>
          )}

          {/* Dataset Info */}
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Dataset: {dataset.name}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex gap-4 text-sm text-muted-foreground">
                <span>{dataset.rowCount.toLocaleString()} rows</span>
                <span>•</span>
                <span>{dataset.columns.length} columns</span>
                <span>•</span>
                <span>{numericColumns.length} numeric</span>
                <span>•</span>
                <span>{dateColumns.length} temporal</span>
              </div>
            </CardContent>
          </Card>

          {/* Analysis Cards */}
          <Tabs defaultValue="rfm" className="space-y-4">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="rfm">RFM</TabsTrigger>
              <TabsTrigger value="cohort">Cohort</TabsTrigger>
              <TabsTrigger value="clustering">Clustering</TabsTrigger>
              <TabsTrigger value="regression">Regression</TabsTrigger>
            </TabsList>
            
            <TabsContent value="rfm" className="space-y-4">
              {renderRFMSection()}
            </TabsContent>
            
            <TabsContent value="cohort" className="space-y-4">
              {renderCohortSection()}
            </TabsContent>
            
            <TabsContent value="clustering" className="space-y-4">
              {renderClusteringSection()}
            </TabsContent>
            
            <TabsContent value="regression" className="space-y-4">
              {renderRegressionSection()}
            </TabsContent>
          </Tabs>

          {/* Results Summary */}
          {Object.keys(results).length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <CheckCircle className="w-5 h-5 text-green-500" />
                  Analysis Results Summary
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  {Object.entries(results).map(([key, result]: [string, any]) => (
                    <div key={key} className="bg-muted/30 rounded-lg p-4 text-center">
                      <p className="text-lg font-bold capitalize">{key}</p>
                      <Badge variant="outline" className="mt-2">
                        {result.status === 'SUCCESS' ? (
                          <span className="text-green-500">{result.status}</span>
                        ) : (
                          <span className="text-red-500">{result.status}</span>
                        )}
                      </Badge>
                      {result.quality_score && (
                        <p className="text-xs text-muted-foreground mt-2">
                          Quality: {(result.quality_score * 100).toFixed(0)}%
                        </p>
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default AnalyticsPage;
