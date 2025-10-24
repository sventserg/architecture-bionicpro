import React, { useState, useEffect } from 'react';

interface UserInfo {
  sub: string;
  email: string;
  preferred_username: string;
  given_name: string;
  family_name: string;
}

const ReportPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [authLoading, setAuthLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [userInfo, setUserInfo] = useState<UserInfo | null>(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  const checkAuth = async (): Promise<boolean> => {
    try {
      console.log('🔐 Checking authentication with BFF...');
      
      const response = await fetch(`${process.env.REACT_APP_API_URL}/auth/user`, {
        method: 'GET',
        credentials: 'include',
        headers: {
          'Accept': 'application/json',
        },
      });
      
      console.log('🔐 Auth response status:', response.status);
      
      if (response.ok) {
        const user = await response.json();
        console.log('✅ User authenticated:', user);
        setUserInfo(user);
        setIsAuthenticated(true);
        setError(null);
        return true;
      } else if (response.status === 401) {
        console.log('❌ User not authenticated');
        setIsAuthenticated(false);
        setUserInfo(null);
        return false;
      } else {
        console.log('❌ Auth check failed, status:', response.status);
        // При других ошибках не меняем состояние аутентификации
        return isAuthenticated;
      }
    } catch (err) {
      console.error('🔐 Auth check error:', err);
      // При ошибке сети не меняем состояние аутентификации
      setError('Cannot connect to authentication service');
      return isAuthenticated;
    }
  };

  useEffect(() => {
    const initializeAuth = async () => {
      setAuthLoading(true);
      await checkAuth();
      setAuthLoading(false);
    };
    
    initializeAuth();
  }, []);

  useEffect(() => {
    console.log('🔄 Authentication state changed:', isAuthenticated);
  }, [isAuthenticated]);

  const handleLogin = () => {
    console.log('🚀 Initiating login via BFF...');
    window.location.href = `${process.env.REACT_APP_API_URL}/auth/login`;
  };

  const handleLogout = async () => {
    try {
      console.log('🚪 Logging out...');
      setLoading(true);
      setError(null);
      
      const response = await fetch(`${process.env.REACT_APP_API_URL}/auth/logout`, {
        method: 'GET',
        credentials: 'include',
      });

      console.log('🚪 Logout response status:', response.status);

      if (response.ok) {
        const result = await response.json();
        console.log('✅ Logout successful:', result);
        
        setIsAuthenticated(false);
        setUserInfo(null);
        setError(null);
        
        console.log('✅ User state updated to logged out');
        
      } else {
        console.warn('⚠️ Logout API returned non-200 status:', response.status);
        setIsAuthenticated(false);
        setUserInfo(null);
        setError('Logged out with warnings');
      }
      
    } catch (err) {
      console.error('❌ Logout error:', err);
      setIsAuthenticated(false);
      setUserInfo(null);
      setError('Logged out (connection issue)');
    } finally {
      setLoading(false);
    }
  };

  const downloadReport = async () => {
    try {
      setLoading(true);
      setError(null);
      console.log('📊 Downloading report via BFF...');

      const response = await fetch(`${process.env.REACT_APP_API_URL}/api/reports`, {
        method: 'GET',
        credentials: 'include',
      });

      console.log('📊 Report response status:', response.status);

      if (response.status === 404) {
        setError('No report data available for your account. Data may still be processing.');
        return;
      }
      
      if (!response.ok) {
        if (response.status === 401) {
          setIsAuthenticated(false);
          throw new Error('Please login again');
        } else if (response.status === 403) {
          throw new Error('Access denied to report data');
        } else {
          throw new Error(`Failed to download report: ${response.status} ${response.statusText}`);
        }
      }

      const blob = await response.blob();
      console.log('📊 Report blob size:', blob.size);
      
      if (blob.size === 0) {
        throw new Error('Report is empty');
      }

      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      
      const contentDisposition = response.headers.get('content-disposition');
      let filename = 'prosthesis_report.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      console.log('✅ Report downloaded successfully');

    } catch (err) {
      console.error('❌ Download error:', err);
      setError(err instanceof Error ? err.message : 'An error occurred while downloading report');
    } finally {
      setLoading(false);
    }
  };

  console.log('🎯 Render state:', { authLoading, isAuthenticated, userInfo, error, loading });

  if (authLoading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md text-center">
          <h1 className="text-2xl font-bold mb-4">Prosthesis Usage Reports</h1>
          <div className="flex items-center justify-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            <span className="ml-3 text-gray-600">Checking authentication...</span>
          </div>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md text-center">
          <h1 className="text-2xl font-bold mb-6">Prosthesis Usage Reports</h1>
          <p className="mb-4 text-gray-600">Please login to access your reports</p>
          <button
            onClick={handleLogin}
            className="px-6 py-3 bg-blue-500 text-white rounded-lg hover:bg-blue-600 transition-colors font-medium"
          >
            Login with Keycloak
          </button>
          {error && (
            <div className="mt-4 p-3 bg-red-100 text-red-700 rounded text-sm">
              {error}
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-md">
        {/* <div className="mb-6">
          <h1 className="text-2xl font-bold mb-2 text-gray-800">Usage Reports</h1>
          {userInfo && (
            <div className="text-sm text-gray-600 bg-gray-50 p-3 rounded">
              <p className="font-semibold">
                Welcome, {userInfo.given_name} {userInfo.family_name}
              </p>
              <p className="text-xs mt-1">{userInfo.email}</p>
            </div>
          )}
        </div> */}
        
        <button
          onClick={downloadReport}
          disabled={loading}
          className={`w-full px-4 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 mb-4 transition-colors font-medium ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? (
            <div className="flex items-center justify-center">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
              Generating Report...
            </div>
          ) : (
            '📥 Download My Report'
          )}
        </button>

        <button
          onClick={handleLogout}
          disabled={loading}
          className={`w-full px-4 py-2 bg-gray-500 text-white rounded hover:bg-gray-600 transition-colors ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Logging out...' : 'Logout'}
        </button>

        {error && (
          <div className="mt-4 p-3 bg-red-100 text-red-700 rounded text-sm">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;