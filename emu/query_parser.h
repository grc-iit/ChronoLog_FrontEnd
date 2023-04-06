#ifndef __EVENT_PARSER_H_
#define __EVENT_PARSER_H_

#include "event_metadata.h"
#include <unordered_map>
#include "event.h"
#include "rw.h"
#include <algorithm>

class query_parser
{

    private:
           std::unordered_map<std::string,event_metadata> event_headers;
           read_write_process *np;            
    public:
	   query_parser()
	   {
	   }
	   ~query_parser()
	   {
	   }
	   void set_rw_pointer(read_write_process *p)
	   {
		   np = p;
	   }
	   void add_event_header(std::string s,int nattrs,std::vector<std::string> &attr_names,std::vector<int> &vsizes,std::vector<bool> &sign,std::vector<bool> &end)
	   {
		auto r = event_headers.find(s);
		if(r == event_headers.end())
		{
		   event_metadata em;
		   assert(nattrs == attr_names.size());
		   assert(nattrs == vsizes.size());
		   assert(nattrs == sign.size());
		   assert(nattrs == end.size());
		   em.set_numattrs(nattrs);
		   for(int i=0;i<nattrs;i++)
		   {
			bool sign_v = sign[i];
			bool end_v = end[i];
			em.add_attr(attr_names[i],vsizes[i],sign_v,end_v);
		   }
		   std::pair<std::string,event_metadata> p(s,em);
		   event_headers.insert(p);
		}
	   }
           bool get_event_header(std::string &s,event_metadata &em)
	   {
              auto r = event_headers.find(s);
	      if(r != event_headers.end())
	      {
		em = r->second;
		return true;
	       }
	       return false;
           }
	   bool sort_by_attr(std::string &s,std::vector<struct event> &,std::string &a,event_metadata &); 
};


#endif
