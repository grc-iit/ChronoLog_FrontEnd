#include "query_parser.h"

template<typename T,int offset=0>
bool compare_events(struct event &e1,struct event &e2)
{
	T *v1 = (T*)(&(e1.data[offset]));
	T *v2 = (T*)(&(e2.data[offset]));
	if(*v1 < *v2) return true;
	else return false;
}

bool query_parser::sort_by_attr(std::string &s, std::vector<struct event> &r_events, std::string &a_name,event_metadata &em)
{
   bool b; 
   int offset; 
   b = em.get_offset(a_name,offset);
   int vsize;
   bool sign_v,end_v;
   b = em.get_attr(a_name,vsize,sign_v,end_v);  

   std::cout <<" offset = "<<offset<<std::endl;

   for(int i=0;i<r_events.size();i++)
   {
       int r = (random()%INT_MAX);
       memcpy(&(r_events[i].data[offset]),&r,sizeof(int));
       int *v = (int*)(&(r_events[i].data[offset]));
   }

   std::sort(r_events.begin(),r_events.end(),compare_events<int,8>);

   return true;
}
