#ifndef __DISTRIBUTED_SORT_H_
#define __DISTRIBUTED_SORT_H_

#include "event.h"
#include <mpi.h>
#include <vector>
#include <iostream>

class dsort
{

   private: 
	   std::vector<struct event>* events;
	   int numprocs;
	   int myrank;
 
   public:
	   dsort(int n,int p) : numprocs(n), myrank(p)
	   {
	   }

	   ~dsort()
	   {
		
	   }
	   void get_unsorted_data(std::vector<struct event> *inp)
	   {
		events = inp;
	   }

	   void sort_data();

	   std::vector<struct event> * get_sorted_data()
	   {
	       return events;
	   }

};

#endif
