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
	int total_size = 65536*2;
	dmap->initialize_tables(total_size,numprocs,myrank,UINT64_MAX);
	CM = C;
	dmap->setClock(CM);
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
      bool b = dmap->Insert(key,e);   
        if(b) event_count++; 
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
