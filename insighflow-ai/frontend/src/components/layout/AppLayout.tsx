import { Outlet } from 'react-router-dom';
import AppSidebar from './AppSidebar';

const AppLayout = () => {
  return (
    <div className="min-h-screen bg-background">
      <AppSidebar />
      <main className="min-h-[calc(100vh-64px)] md:ml-64 md:min-h-screen">
        <Outlet />
      </main>
    </div>
  );
};

export default AppLayout;
