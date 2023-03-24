#ifndef __DATABUFFER_H_
#define __DATABUFFER_H_

#include "distributed_map.h"
#include "event.h"

class databuffer
{

  private:
     int event_count;
     distributed_hashmap<uint64_t,int> *dmap;
     std::vector<struct event> equeue;
     ClockSynchronization<ClocksourceCPPStyle> *CM;
     double max_t;
     double max_time;
     int myrank;
  public:
     databuffer(int numprocs,int rank,int numcores,ClockSynchronization<ClocksourceCPPStyle> *C) 
     {
	event_count = 0;
	dmap = new distributed_hashmap<uint64_t,int> ();
	int total_size = 65536*2;
	dmap->initialize_tables(total_size,numprocs,numcores,rank,UINT64_MAX);
	CM = C;
	dmap->setClock(CM);
	max_t = 0;
	max_time = 0;
	myrank = rank;
     }
     ~databuffer() 
     {
	 delete dmap;
     }

  int num_events()
  {
	  return event_count;
  }

  int num_dropped_events()
  {
	  return dmap->num_dropped();
  }

  void add_event(event &e)
  {
      uint64_t key = e.ts;
      int v = 1;

      auto t1 = std::chrono::high_resolution_clock::now();

      bool b = dmap->Insert(key,v);   

      auto t2 = std::chrono::high_resolution_clock::now();
      double t = std::chrono::duration<double> (t2-t1).count();
      if(max_t < t) max_t = t;
      

      if(b) 
      {
	      equeue.push_back(e);
	      event_count++; 
      }
  }

  std::vector<struct event> & get_buffer()
  {
	  bool b = dmap->LocalClearMap();
	  return equeue;
  }
  void clear_buffer()
  {
  }

};

#endif
