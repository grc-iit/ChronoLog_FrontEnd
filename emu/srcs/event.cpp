#include "event.h"
#include <string>


std::string pack_event(struct event* e,int length)
{
   std::string s = std::to_string(e->ts);
   s += std::string(e->data,length);
   return s;
}

