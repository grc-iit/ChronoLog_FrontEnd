#ifndef __DATABUFFER_H_
#define __DATABUFFER_H_

#include "distributed_map.h"
#include "event.h"

class databuffer
{

  private:
     int event_count;
     distributed_hashmap<uint64_t,struct event> *dmap;
      ClockSynchronization<ClocksourceCPPStyle> *CM;
  public:
     databuffer(int numprocs,int myrank,ClockSynchronization<ClocksourceCPPStyle> *C) 
     {
	event_count = 0;
	dmap = new distributed_hashmap<uint64_t,struct event> ();
	int total_size = 8192;
	dmap->initialize_tables(total_size,numprocs,myrank,UINT64_MAX);
	CM = C;
     }
     ~databuffer() 
     {
	 delete dmap;
     }

  int num_events()
  {
	  return event_count;
  }

  void add_event(event &e)
  {
      uint64_t key = e.ts;
      bool b = CM->NearTime(key);
      if(b)
      {
        b = dmap->Insert(key,e);   
        if(b) event_count++; 
      }
  }

  bool get_buffer(std::vector<struct event> &events)
  {
	  bool b = dmap->LocalGetMap(events);
	  b = dmap->LocalClearMap();
	  return b;
  }
  void clear_buffer()
  {
  }

};

#endif
