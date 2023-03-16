#ifndef __PROCESS_H_
#define __PROCESS_H_

#include <abt.h>
#include <mpi.h>
#include "ClockSync.h"
#include "/home/aparna/Desktop/hdf5-1.12.2/hdf5/include/hdf5.h"
#include <boost/container_hash/hash.hpp>
#include "data_buffer.h"
#include "distributed_sort.h"
#include "mds.h"

class read_write_process
{

private:
      ClockSynchronization<ClocksourceCPPStyle> *CM;
      int myrank;
      int numprocs;
      boost::hash<uint64_t> hasher;
      uint64_t seed = 1;
      int nbits;
      databuffer *dm;
      std::vector<struct event> myevents;
      dsort *ds;
public:
	read_write_process(int r,int np) : myrank(r), numprocs(np)
	{
           H5open();
           std::string unit = "nanosecond";
	   CM = new ClockSynchronization<ClocksourceCPPStyle> (myrank,numprocs,unit);
	   dm = new databuffer(numprocs,myrank,CM);
	   ds = new dsort(numprocs,myrank);
	   nbits = 0;

	}
	~read_write_process()
	{
	   delete CM;
	   delete dm;
	   delete ds;
	   H5close();

	}

	int nearest_power_two(int n)
	{
	    int nn = 0;
	    int v = 0;

	    while(n > v)
	    {
		v = pow(2,nn);
		nn++;
	    }
	    return v;
	}
	int numbits(int n)
	{
	  int pn = nearest_power_two(n);
	  int c = 0;
   	  int nn = pn;

	  nbits = 1;

   	  while(nn > 1)
          {
      		nn = nn/2;
      		int rem = nn%2;
      		c++;
   	  }
	  if(c > 0) nbits = c;
   	  return c;
	}

	int writer_id(uint64_t ts)
	{
	   uint64_t key = seed;
    	   boost::hash_combine(key,ts);
	   uint64_t mask = UINT64_MAX;
	   mask = mask << (64-nbits);
	   uint64_t key1 = key & mask;
	   key1 = key1 >> (64-nbits);
	   int id = key1%numprocs;
	   return id;
	}

	void synchronize()
	{
	   CM->SynchronizeClocks();
	   CM->ComputeErrorInterval();

	}
	void get_events_from_map()
	{
	   myevents.clear();
	   bool b = dm->get_buffer(myevents);
	}
	std::vector<struct event> & get_events()
	{
		return myevents;
	}
	void sort_events()
	{
	    get_events_from_map();
	    ds->get_unsorted_data(myevents);
	    ds->sort_data(); 
	    ds->get_sorted_data(myevents); 
	}
	void create_events(int num_events);
	//void total_order_events();
        void pwrite(const char *);
	void pread();
};

#endif
