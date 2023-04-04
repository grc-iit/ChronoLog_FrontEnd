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
	   int numprocs;
	   int myrank;
 
   public:
	   dsort(int n,int p) : numprocs(n), myrank(p)
	   {
	   }

	   ~dsort()
	   {
		
	   }
	   void create_sort_buffer()
	   {
		std::vector<struct event> *ev = nullptr;
		events.push_back(ev);
	   }
	   void get_unsorted_data(std::vector<struct event> *inp,int index)
	   {
	       events[index] = inp;
	   }

	   void sort_data(int,uint64_t&,uint64_t&);

	   std::vector<struct event> * get_sorted_data(int index)
	   {
	      return events[index];
	   }

};

#endif
