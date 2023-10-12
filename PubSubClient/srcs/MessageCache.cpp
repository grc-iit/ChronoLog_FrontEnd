#include "MessageCache.h"

bool message_cache::add_message(std::string &msg)
{
   char ts_c[8];
   for(int i=0;i<8;i++)
     ts_c[i] = msg[i];

   uint64_t ts = *(uint64_t*)(ts_c);

   m.lock();
   if(messages.size()==num_messages)
   {
	messages.erase(messages.begin());
   }
   messages.push_back(msg);

   m.unlock();
   return true;





}
