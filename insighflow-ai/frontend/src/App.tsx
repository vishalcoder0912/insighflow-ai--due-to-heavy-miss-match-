import { Suspense, lazy } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { DataProvider } from "@/contexts/DataContext";

const AppLayout = lazy(() => import("@/components/layout/AppLayout"));
const Index = lazy(() => import("./pages/Index"));
const UploadPage = lazy(() => import("./pages/UploadPage"));
const ChatPage = lazy(() => import("./pages/ChatPage"));
const ReportPage = lazy(() => import("./pages/ReportPage"));
const ImportPage = lazy(() => import("./pages/ImportPage"));
const AnalyticsPage = lazy(() => import("./pages/AnalyticsPage"));
const NotFound = lazy(() => import("./pages/NotFound"));

const queryClient = new QueryClient();

const RouteFallback = () => (
  <div className="min-h-screen bg-background px-6 py-8 text-sm text-muted-foreground">
    Loading InsightFlow...
  </div>
);

const routerFuture = {
  v7_startTransition: true,
  v7_relativeSplatPath: true,
} as const;

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <DataProvider>
        <Suspense fallback={<RouteFallback />}>
          <BrowserRouter future={routerFuture}>
            <Routes>
              <Route element={<AppLayout />}>
                <Route path="/" element={<Index />} />
                <Route path="/upload" element={<UploadPage />} />
                <Route path="/chat" element={<ChatPage />} />
                <Route path="/reports" element={<ReportPage />} />
                <Route path="/import" element={<ImportPage />} />
                <Route path="/analytics" element={<AnalyticsPage />} />
              </Route>
              <Route path="*" element={<NotFound />} />
            </Routes>
          </BrowserRouter>
        </Suspense>
      </DataProvider>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
