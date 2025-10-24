import React, { useEffect } from 'react';
import ReportPage from './components/ReportPage';

const clearAuthState = () => {
  console.log('🔄 Clearing auth state on app load');
  try {
    localStorage.removeItem('auth_state');
    localStorage.removeItem('user_info');
    sessionStorage.removeItem('auth_token');
  } catch (error) {
    console.log('No auth state to clear or storage not available');
  }
};

const App: React.FC = () => {
  useEffect(() => {
    clearAuthState();
    const handleBeforeUnload = () => {
      console.log('🧹 Cleaning up before page unload');
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, []);

  return (
    <div className="App">
      <ReportPage />
    </div>
  );
};

export default App;