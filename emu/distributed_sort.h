#ifndef __DISTRIBUTED_SORT_H_
#define __DISTRIBUTED_SORT_H_

#include "event.h"
#include <mpi.h>
#include <vector>
#include <iostream>

class dsort
{

   private: 
	   std::vector<struct event> events;
	   int numprocs;
	   int myrank;
 
   public:
	   dsort(int n,int p) : numprocs(n), myrank(p)
	   {
	   }

	   ~dsort()
	   {
		
	   }
	   void get_unsorted_data(std::vector<struct event> &inp)
	   {
		events.assign(inp.begin(),inp.end());
	   }

	   void sort_data();

	   void get_sorted_data(std::vector<struct event> &oup)
	   {
		   oup.assign(events.begin(),events.end());
		   events.clear();
	   }

};

#endif
