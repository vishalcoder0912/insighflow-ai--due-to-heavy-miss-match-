import { useData } from '@/contexts/DataContext';
import ChatInterface from '@/components/chat/ChatInterface';

const ChatPage = () => {
  useData();

  return <ChatInterface />;
};

export default ChatPage;
