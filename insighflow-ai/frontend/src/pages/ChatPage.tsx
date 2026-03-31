import { useData } from '@/contexts/DataContext';
import ChatInterfaceV2 from '@/components/chat/ChatInterfaceV2';

const ChatPage = () => {
  useData();

  return <ChatInterfaceV2 />;
};

export default ChatPage;
