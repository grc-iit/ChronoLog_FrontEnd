#ifndef __DISTRIBUTED_SORT_H_
#define __DISTRIBUTED_SORT_H_

#include "event.h"
#include <mpi.h>
#include <vector>
#include <iostream>
#include <unordered_map>

class dsort
{

   private: 
	   std::vector<std::vector<struct event>*> events;
	   std::unordered_map<std::string,int> sort_names;
	   int numprocs;
	   int myrank;
 
   public:
	   dsort(int n,int p) : numprocs(n), myrank(p)
	   {
	   }

	   ~dsort()
	   {
		
	   }
	   void create_sort_buffer(std::string &s)
	   {
		auto r = sort_names.find(s);
		if(r == sort_names.end())
		{
		   std::vector<struct event> *ev = nullptr;
		   events.push_back(ev);
		   std::pair<std::string,int> p(s,events.size()-1);
		   sort_names.insert(p);
		}

	   }
	   void get_unsorted_data(std::vector<struct event> *inp,std::string &s)
	   {
	      auto r = sort_names.find(s);
	      if(r == sort_names.end())
	      {
		events.push_back(inp);
		std::pair<std::string,int> p(s,events.size()-1);
		sort_names.insert(p);
	      }
	      else 
	      {
		 int index = r->second;
		 events[index] = inp;
	      }
	   }

	   void sort_data(std::string &s,uint64_t&,uint64_t&);

	   std::vector<struct event> * get_sorted_data(std::string &s)
	   {
	     auto r = sort_names.find(s);
	     if(r != sort_names.end())
	     {
	       int index = r->second;
	       return events[index];
	     }
	     else return nullptr;
	   }

};

#endif
