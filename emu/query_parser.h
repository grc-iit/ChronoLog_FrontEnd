#ifndef __EVENT_PARSER_H_
#define __EVENT_PARSER_H_

#include "event_metadata.h"
#include <unordered_map>
#include "event.h"
#include "rw.h"
#include <algorithm>

typedef struct view_
{
   int offset;
   int length;
   struct event e;
}view;

class query_parser
{

    private:
           std::unordered_map<std::string,event_metadata> event_headers;
           read_write_process *np;      
           std::unordered_map<std::string,std::vector<view>*> cached_views;
	   std::unordered_map<std::string,std::unordered_map<uint64_t,int>> lookup_tables;


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
	   template<typename T,class EqualFcn=std::equal_to<T>>
	   bool select_by_attr(std::string &,std::string &,std::vector<struct event>&,event_metadata &,T &,std::vector<view> &);
	   bool sort_events_by_attr(std::string &s,std::vector<struct event> &,std::string &a,event_metadata &,std::vector<view>&);
	   //bool select_by_key(std::string &,std::vector<struct event>&,event_metadata &,uint64_t &,std::vector<view>&);
	   bool create_view_from_events(std::string&,std::vector<struct event>&,std::vector<view> &);
	   bool sort_view_by_attr(std::vector<view> &);
	   bool add_view_to_cache(std::string&,std::string&,std::vector<view>&);
	   template<typename T,class EqualFcn=std::equal_to<T>>
	   bool select_unique_by_attr(std::string &,std::string &,std::vector<struct event>&,event_metadata&,T&,std::vector<view>&);
};


#endif
