#include "query_parser.h"



bool query_parser::sort_by_attr(std::string &s,std::pair<uint64_t,uint64_t> & range,std::string &a_name)
{
   std::vector<struct event> r_events;
   np->get_events_in_range(s,range,r_events);
   
   event_metadata em;
   bool b = get_event_header(s,em);   
 
   int offset; 
   b = em.get_offset(s,offset);
   int asize,vsize;
   b = em.get_attr(a_name,asize,vsize);  

   auto compare = [&keysize=asize,&key=offset](struct event &e1, struct event &e2)
	          {
		       for(int i=0;i<keysize;i++)
		       {
			  if(e1.data+key+i < e2.data+key+i) return true;
		       } 
		       return false;
		  };

   std::sort(r_events.begin(),r_events.end(),compare);
   return true;
}
