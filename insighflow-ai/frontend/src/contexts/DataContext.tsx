import React, { createContext, useCallback, useContext, useEffect, useState } from 'react';
import { ChatMessage, Dataset } from '@/lib/data-store';
import { ensureSession, loadPersistedDataset, uploadAndAnalyzeDataset } from '@/lib/api';

interface DataContextType {
  dataset: Dataset | null;
  setDataset: (d: Dataset | null) => void;
  chatMessages: ChatMessage[];
  addChatMessage: (msg: ChatMessage) => void;
  isProcessing: boolean;
  setIsProcessing: (v: boolean) => void;
  uploadCSV: (file: File) => Promise<void>;
  loadDemo: () => Promise<void>;
  isReady: boolean;
  sessionEmail: string | null;
}

const DataContext = createContext<DataContextType | null>(null);

export const useData = () => {
  const ctx = useContext(DataContext);
  if (!ctx) throw new Error('useData must be used within DataProvider');
  return ctx;
};

export const DataProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);
  const [isReady, setIsReady] = useState(false);
  const [sessionEmail, setSessionEmail] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const initialize = async () => {
      try {
        await ensureSession();
        if (!active) return;
        setSessionEmail(localStorage.getItem('insightflow.email'));
        const storedDataset = await loadPersistedDataset();
        if (storedDataset && active) {
          setDataset(storedDataset);
        }
      } finally {
        if (active) setIsReady(true);
      }
    };
    void initialize();
    return () => {
      active = false;
    };
  }, []);

  const addChatMessage = useCallback((msg: ChatMessage) => {
    setChatMessages((prev) => [...prev, msg]);
  }, []);

  const uploadCSV = useCallback(async (file: File) => {
    const nextDataset = await uploadAndAnalyzeDataset(file);
    setDataset(nextDataset);
    setChatMessages([]);
  }, []);

  const loadDemo = useCallback(async () => {
    return;
  }, []);

  return (
    <DataContext.Provider
      value={{
        dataset,
        setDataset,
        chatMessages,
        addChatMessage,
        isProcessing,
        setIsProcessing,
        uploadCSV,
        loadDemo,
        isReady,
        sessionEmail,
      }}
    >
      {children}
    </DataContext.Provider>
  );
};
