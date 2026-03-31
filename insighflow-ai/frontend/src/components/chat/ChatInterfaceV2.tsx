import { Suspense, lazy, useEffect, useRef, useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Send, Sparkles, User, Lightbulb, Trash2, Plus, Clock, Database, AlertCircle, CheckCircle, ChevronDown } from 'lucide-react';
import { useData } from '@/contexts/DataContext';
import { ChatMessage as ChatMsg, ChartConfig } from '@/lib/data-store';
import { Link } from 'react-router-dom';
import { sendChatMessage, createChatSession, getChatSessions, getSessionMessages, deleteChatSession, ChatSession as ChatSessionType, ChatMessage } from '@/lib/api';

const AnalyticsChart = lazy(() => import('@/components/charts/AnalyticsChart'));

const ChartFallback = () => <div className="glass h-64 rounded-xl animate-pulse" />;

const chartTypeMap: Record<string, ChartConfig['type']> = {
  bar: 'bar',
  line: 'line',
  pie: 'pie',
  scatter: 'scatter',
  table: 'table',
  kpi: 'kpi',
  area: 'area',
};

interface ChatInterfaceV2Props {
  initialDatasetId?: number;
}

const ChatInterfaceV2: React.FC<ChatInterfaceV2Props> = ({ initialDatasetId }) => {
  const { dataset } = useData();
  const [sessions, setSessions] = useState<ChatSessionType[]>([]);
  const [currentSessionId, setCurrentSessionId] = useState<number | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [sqlExpanded, setSqlExpanded] = useState<number | null>(null);
  
  const bottomRef = useRef<HTMLDivElement>(null);
  const datasetId = dataset?.datasetId || initialDatasetId;

  useEffect(() => {
    loadSessions();
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const loadSessions = async () => {
    if (!datasetId) return;
    try {
      const result = await getChatSessions(datasetId);
      if (result.status === 'success') {
        setSessions(result.sessions);
        if (result.sessions.length > 0 && !currentSessionId) {
          loadSessionMessages(result.sessions[0].id);
        }
      }
    } catch (error) {
      console.error('Failed to load sessions:', error);
    }
  };

  const loadSessionMessages = async (sessionId: number) => {
    try {
      const result = await getSessionMessages(sessionId);
      if (result.status === 'success') {
        setMessages(result.messages);
        setCurrentSessionId(sessionId);
      }
    } catch (error) {
      console.error('Failed to load messages:', error);
    }
  };

  const handleNewSession = async () => {
    if (!datasetId) return;
    try {
      const result = await createChatSession(datasetId, `Chat - ${new Date().toLocaleString()}`);
      if (result.status === 'success') {
        setCurrentSessionId(result.session_id);
        setMessages([]);
        loadSessions();
      }
    } catch (error) {
      console.error('Failed to create session:', error);
    }
  };

  const handleDeleteSession = async (sessionId: number, e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await deleteChatSession(sessionId);
      if (currentSessionId === sessionId) {
        setCurrentSessionId(null);
        setMessages([]);
      }
      loadSessions();
    } catch (error) {
      console.error('Failed to delete session:', error);
    }
  };

  const processQuery = async (query: string) => {
    if (!datasetId || !query.trim()) return;

    const userMsg: ChatMessage = {
      id: Date.now(),
      role: 'user',
      content: query,
      is_error: false,
      is_cached: false,
      created_at: new Date().toISOString(),
    };
    
    setMessages(prev => [...prev, userMsg]);
    setIsProcessing(true);

    try {
      const result = await sendChatMessage({
        session_id: currentSessionId || undefined,
        dataset_id: datasetId,
        message: query,
        use_llm: true,
      });

      if (result.status === 'success') {
        setCurrentSessionId(result.session_id);
        
        const assistantMsg: ChatMessage = {
          ...result.message,
          role: 'assistant',
        };
        
        setMessages(prev => [...prev, assistantMsg]);
        
        if (!currentSessionId) {
          loadSessions();
        }
      } else {
        const errorMsg: ChatMessage = {
          id: Date.now() + 1,
          role: 'assistant',
          content: result.message?.content || 'An error occurred',
          is_error: true,
          is_cached: false,
          created_at: new Date().toISOString(),
        };
        setMessages(prev => [...prev, errorMsg]);
      }
    } catch (error) {
      const errorMsg: ChatMessage = {
        id: Date.now() + 1,
        role: 'assistant',
        content: 'Failed to process your request. Please try again.',
        is_error: true,
        is_cached: false,
        created_at: new Date().toISOString(),
      };
      setMessages(prev => [...prev, errorMsg]);
    } finally {
      setIsProcessing(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isProcessing) return;
    processQuery(input.trim());
    setInput('');
  };

  const handleSuggestionClick = (suggestion: string) => {
    processQuery(suggestion);
  };

  if (!dataset) {
    return (
      <div className="flex flex-col h-[calc(100vh-2rem)] m-4 glass rounded-2xl overflow-hidden items-center justify-center text-center p-6">
        <h2 className="text-lg font-semibold text-foreground mb-2">No dataset loaded</h2>
        <p className="text-sm text-muted-foreground mb-4">Upload a dataset first to start chatting with your data.</p>
        <Link to="/upload" className="px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm hover:bg-primary/90 transition-colors">
          Upload Dataset
        </Link>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-[calc(100vh-2rem)] m-4 glass rounded-2xl overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-border/50 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-foreground">AI Data Analyst</h2>
            <p className="text-xs text-muted-foreground">
              {dataset.name} · {dataset.rowCount.toLocaleString()} rows
            </p>
          </div>
        </div>
        
        <div className="flex items-center gap-2">
          <button
            onClick={handleNewSession}
            className="p-2 rounded-lg hover:bg-secondary transition-colors"
            title="New Chat"
          >
            <Plus className="w-4 h-4 text-muted-foreground" />
          </button>
          <button
            onClick={() => setShowHistory(!showHistory)}
            className="p-2 rounded-lg hover:bg-secondary transition-colors"
            title="Chat History"
          >
            <Clock className="w-4 h-4 text-muted-foreground" />
          </button>
        </div>
      </div>

      <div className="flex flex-1 overflow-hidden">
        {/* History Sidebar */}
        <AnimatePresence>
          {showHistory && (
            <motion.div
              initial={{ width: 0, opacity: 0 }}
              animate={{ width: 250, opacity: 1 }}
              exit={{ width: 0, opacity: 0 }}
              className="border-r border-border/50 overflow-hidden bg-muted/20"
            >
              <div className="p-3 border-b border-border/30">
                <h3 className="text-xs font-semibold text-muted-foreground uppercase">Chat History</h3>
              </div>
              <div className="overflow-y-auto p-2 space-y-1">
                {sessions.map(session => (
                  <div
                    key={session.id}
                    onClick={() => loadSessionMessages(session.id)}
                    className={`group p-2 rounded-lg cursor-pointer text-sm truncate ${
                      currentSessionId === session.id 
                        ? 'bg-primary/10 text-primary' 
                        : 'hover:bg-secondary text-muted-foreground'
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="truncate">{session.title}</span>
                      <button
                        onClick={(e) => handleDeleteSession(session.id, e)}
                        className="opacity-0 group-hover:opacity-100 p-1 hover:text-destructive transition-all"
                      >
                        <Trash2 className="w-3 h-3" />
                      </button>
                    </div>
                    <div className="text-xs text-muted-foreground/60 mt-1">
                      {session.message_count} messages
                    </div>
                  </div>
                ))}
                {sessions.length === 0 && (
                  <p className="text-xs text-muted-foreground p-2">No chat history</p>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Chat Area */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
            {messages.length === 0 && (
              <motion.div 
                initial={{ opacity: 0 }} 
                animate={{ opacity: 1 }} 
                className="flex flex-col items-center justify-center h-full text-center"
              >
                <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center mb-4 glow-primary">
                  <Sparkles className="w-8 h-8 text-primary" />
                </div>
                <h3 className="text-lg font-semibold text-foreground mb-2">Ask about your data</h3>
                <p className="text-sm text-muted-foreground mb-6 max-w-md">
                  I can help you analyze your data using natural language. Try asking questions like:
                </p>
                <div className="flex flex-wrap gap-2 justify-center max-w-lg">
                  {[
                    'Show top 10 by sales',
                    'What is the total revenue?',
                    'Find anomalies in the data',
                    'Show correlations between metrics',
                  ].map((q, i) => (
                    <button
                      key={i}
                      onClick={() => processQuery(q)}
                      className="px-3 py-1.5 text-xs rounded-full bg-secondary text-secondary-foreground hover:bg-primary/20 hover:text-primary transition-colors"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </motion.div>
            )}

            <AnimatePresence>
              {messages.map((msg, index) => (
                <motion.div
                  key={msg.id || index}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}
                >
                  {msg.role === 'assistant' && (
                    <div className="w-7 h-7 rounded-lg bg-primary/10 flex items-center justify-center flex-shrink-0 mt-1">
                      <Sparkles className="w-3.5 h-3.5 text-primary" />
                    </div>
                  )}
                  
                  <div className={`max-w-2xl space-y-2 ${msg.role === 'user' ? 'text-right' : ''}`}>
                    {/* Message Content */}
                    <div className={`inline-block px-4 py-2.5 rounded-xl text-sm ${
                      msg.role === 'user' 
                        ? 'bg-primary text-primary-foreground' 
                        : msg.is_error
                          ? 'bg-destructive/10 text-destructive'
                          : 'bg-secondary text-secondary-foreground'
                    }`}>
                      {msg.content}
                    </div>

                    {/* SQL Query Toggle */}
                    {msg.sql_query && (
                      <div className="bg-muted/50 rounded-lg overflow-hidden">
                        <button
                          onClick={() => setSqlExpanded(sqlExpanded === msg.id ? null : msg.id)}
                          className="flex items-center gap-2 px-3 py-2 text-xs text-muted-foreground hover:text-foreground w-full transition-colors"
                        >
                          <Database className="w-3 h-3" />
                          <span className="flex-1 text-left font-mono truncate">
                            {msg.sql_query.replace(/\n/g, ' ').slice(0, 60)}...
                          </span>
                          <ChevronDown className={`w-3 h-3 transition-transform ${sqlExpanded === msg.id ? 'rotate-180' : ''}`} />
                        </button>
                        <AnimatePresence>
                          {sqlExpanded === msg.id && (
                            <motion.div
                              initial={{ height: 0 }}
                              animate={{ height: 'auto' }}
                              exit={{ height: 0 }}
                              className="px-3 pb-3"
                            >
                              <pre className="text-xs font-mono bg-muted p-2 rounded overflow-x-auto whitespace-pre-wrap text-muted-foreground">
                                {msg.sql_query}
                              </pre>
                            </motion.div>
                          )}
                        </AnimatePresence>
                      </div>
                    )}

                    {/* Execution Info */}
                    {msg.role === 'assistant' && !msg.is_error && (
                      <div className="flex items-center gap-3 text-xs text-muted-foreground">
                        {msg.is_cached && (
                          <span className="flex items-center gap-1">
                            <CheckCircle className="w-3 h-3 text-green-500" />
                            Cached
                          </span>
                        )}
                        {msg.execution_time_ms && (
                          <span>{msg.execution_time_ms}ms</span>
                        )}
                        {msg.row_count !== undefined && (
                          <span>{msg.row_count} rows</span>
                        )}
                      </div>
                    )}

                    {/* Error Message */}
                    {msg.is_error && (
                      <div className="flex items-center gap-2 text-xs text-destructive">
                        <AlertCircle className="w-3 h-3" />
                        <span>An error occurred processing your request</span>
                      </div>
                    )}

                    {/* Chart */}
                    {msg.chart_config && !msg.is_error && (
                      <div className="w-full">
                        <Suspense fallback={<ChartFallback />}>
                          <AnalyticsChart 
                            config={{
                              id: `msg-chart-${msg.id}`,
                              type: chartTypeMap[msg.chart_config.type] || 'bar',
                              title: msg.chart_config.title || 'Chart',
                              xKey: msg.chart_config.options?.xAxisKey || 'name',
                              yKey: msg.chart_config.options?.yAxisKey || 'value',
                              data: msg.chart_config.data || [],
                            }} 
                            index={0} 
                          />
                        </Suspense>
                      </div>
                    )}
                  </div>

                  {msg.role === 'user' && (
                    <div className="w-7 h-7 rounded-lg bg-secondary flex items-center justify-center flex-shrink-0 mt-1">
                      <User className="w-3.5 h-3.5 text-secondary-foreground" />
                    </div>
                  )}
                </motion.div>
              ))}
            </AnimatePresence>

            {/* Processing Indicator */}
            {isProcessing && (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex gap-3">
                <div className="w-7 h-7 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Sparkles className="w-3.5 h-3.5 text-primary animate-pulse" />
                </div>
                <div className="bg-secondary rounded-xl px-4 py-2.5">
                  <div className="flex gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '0ms' }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '150ms' }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </motion.div>
            )}

            <div ref={bottomRef} />
          </div>

          {/* Input Form */}
          <form onSubmit={handleSubmit} className="px-4 py-3 border-t border-border/50">
            <div className="flex gap-3">
              <input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Ask about your data..."
                disabled={isProcessing}
                className="flex-1 bg-muted rounded-xl px-4 py-3 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
              />
              <button
                type="submit"
                disabled={!input.trim() || isProcessing}
                className="w-11 h-11 rounded-xl bg-primary text-primary-foreground flex items-center justify-center hover:bg-primary/90 transition-colors disabled:opacity-50"
              >
                <Send className="w-4 h-4" />
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
};

export default ChatInterfaceV2;
