import { Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Upload, MessageSquare, Database, Sparkles, Menu, FileText, Brain, Download } from 'lucide-react';
import { motion } from 'framer-motion';
import { Sheet, SheetContent, SheetTrigger } from '@/components/ui/sheet';
import { Button } from '@/components/ui/button';

const navItems = [
  { path: '/', label: 'Dashboard', icon: LayoutDashboard },
  { path: '/upload', label: 'Upload Data', icon: Upload },
  { path: '/chat', label: 'AI Chat', icon: MessageSquare },
  { path: '/analytics', label: 'Advanced Analytics', icon: Brain },
  { path: '/reports', label: 'Export Reports', icon: FileText },
  { path: '/import', label: 'Data Import', icon: Download },
];

const SidebarContent = ({ pathname }: { pathname: string }) => (
  <div className="flex h-full flex-col bg-sidebar">
    <div className="border-b border-sidebar-border p-6">
      <Link to="/" className="flex items-center gap-3">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 glow-primary">
          <Sparkles className="h-5 w-5 text-primary" />
        </div>
        <div>
          <h1 className="text-base font-bold tracking-tight text-sidebar-accent-foreground">InsightFlow</h1>
          <p className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">AI Analytics</p>
        </div>
      </Link>
    </div>

    <nav className="flex-1 space-y-1 p-4">
      {navItems.map((item) => {
        const isActive = pathname === item.path;
        return (
          <Link key={item.path} to={item.path}>
            <motion.div
              className={`relative flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-colors ${
                isActive
                  ? 'bg-primary text-sidebar-primary-foreground'
                  : 'text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground'
              }`}
              whileHover={{ x: 2 }}
              whileTap={{ scale: 0.98 }}
            >
              <item.icon className="h-4 w-4" />
              <span className="font-medium">{item.label}</span>
            </motion.div>
          </Link>
        );
      })}
    </nav>

    <div className="border-t border-sidebar-border p-4">
      <div className="flex items-center gap-2 rounded-lg bg-sidebar-accent px-3 py-2">
        <Database className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-xs text-muted-foreground">Backend session ready</span>
        <span className="ml-auto h-2 w-2 rounded-full bg-success animate-pulse-slow" />
      </div>
    </div>
  </div>
);

const AppSidebar = () => {
  const location = useLocation();

  return (
    <>
      <header className="sticky top-0 z-40 flex items-center justify-between border-b border-sidebar-border bg-sidebar px-4 py-3 md:hidden">
        <Link to="/" className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10 glow-primary">
            <Sparkles className="h-5 w-5 text-primary" />
          </div>
          <div>
            <h1 className="text-sm font-bold tracking-tight text-sidebar-accent-foreground">InsightFlow</h1>
            <p className="text-[10px] font-mono uppercase tracking-widest text-muted-foreground">AI Analytics</p>
          </div>
        </Link>
        <Sheet>
          <SheetTrigger asChild>
            <Button variant="ghost" size="icon" aria-label="Open navigation menu">
              <Menu className="h-5 w-5" />
            </Button>
          </SheetTrigger>
          <SheetContent side="left" className="w-72 border-sidebar-border bg-sidebar p-0">
            <SidebarContent pathname={location.pathname} />
          </SheetContent>
        </Sheet>
      </header>

      <aside className="fixed left-0 top-0 z-50 hidden h-screen w-64 border-r border-sidebar-border bg-sidebar md:flex md:flex-col">
        <SidebarContent pathname={location.pathname} />
      </aside>
    </>
  );
};

export default AppSidebar;
