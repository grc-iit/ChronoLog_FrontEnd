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
	   void add_event_header(std::string s,int nattrs,std::vector<std::string> &attr_names,std::vector<int> &asizes,std::vector<int> &vsizes)
	   {
		auto r = event_headers.find(s);
		if(r == event_headers.end())
		{
		   event_metadata em;
		   assert(nattrs == attr_names.size());
		   assert(nattrs == asizes.size());
		   assert(nattrs == vsizes.size());
		   em.set_numattrs(nattrs);
		   for(int i=0;i<nattrs;i++)
		   {
			em.add_attr(attr_names[i],asizes[i],vsizes[i]);
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
	   bool sort_by_attr(std::string &s,std::pair<uint64_t,uint64_t>&r,std::string &a); 
};


#endif
