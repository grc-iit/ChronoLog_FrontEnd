#ifndef __RW_H_
#define __RW_H_

#include <abt.h>
#include <mpi.h>
#include "ClockSync.h"
#include "hdf5.h"
#include <boost/container_hash/hash.hpp>
#include "data_buffer.h"
#include "distributed_sort.h"

class read_write_process
{

private:
      ClockSynchronization<ClocksourceCPPStyle> *CM;
      int myrank;
      int numprocs;
      boost::hash<uint64_t> hasher;
      uint64_t seed = 1;
      databuffer *dm;
      std::vector<struct event> myevents;
      dsort *ds;
public:
	read_write_process(int r,int np,ClockSynchronization<ClocksourceCPPStyle> *C) : myrank(r), numprocs(np)
	{
           H5open();
           std::string unit = "microsecond";
	   CM = C;
	   dm = new databuffer(numprocs,myrank,CM);
	   ds = new dsort(numprocs,myrank);
	}
	~read_write_process()
	{
	   delete dm;
	   delete ds;
	   H5close();

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

	int num_events()
	{
		return myevents.size();
	}
	int dropped_events()
	{
	    return dm->num_dropped_events();
	}
	void create_events(int num_events);
	//void total_order_events();
        void pwrite(const char *);
	void pread(const char*);
};

#endif
