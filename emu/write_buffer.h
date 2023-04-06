#ifndef __DATABUFFER_H_
#define __DATABUFFER_H_

#include <thallium.hpp>
#include <thallium/serialization/proc_input_archive.hpp>
#include <thallium/serialization/proc_output_archive.hpp>
#include <thallium/serialization/serialize.hpp>
#include <thallium/serialization/stl/array.hpp>
#include <thallium/serialization/stl/complex.hpp>
#include <thallium/serialization/stl/deque.hpp>
#include <thallium/serialization/stl/forward_list.hpp>
#include <thallium/serialization/stl/list.hpp>
#include <thallium/serialization/stl/map.hpp>
#include <thallium/serialization/stl/multimap.hpp>
#include <thallium/serialization/stl/multiset.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/set.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/tuple.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/unordered_multimap.hpp>
#include <thallium/serialization/stl/unordered_multiset.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <mpi.h>

#include "distributed_map.h"
#include "event.h"

namespace tl=thallium;

class databuffers
{

  private:
     int event_count;
     distributed_hashmap<uint64_t,int> *dmap;
     std::vector<std::vector<struct event>*> equeues;
     ClockSynchronization<ClocksourceCPPStyle> *CM;
     double max_t;
     double max_time;
     int myrank;
  public:
     databuffers(int numprocs,int rank,int numcores,ClockSynchronization<ClocksourceCPPStyle> *C) 
     {
	event_count = 0;
	dmap = new distributed_hashmap<uint64_t,int> (numprocs,numcores,rank);
	CM = C;
	dmap->setClock(CM);
	max_t = 0;
	max_time = 0;
	myrank = rank;
     }
     ~databuffers() 
     {
	 delete dmap;
	 for(int i=0;i<equeues.size();i++)
		 delete equeues[i];
     }

    void  server_client_addrs(tl::engine *t_server,tl::engine *t_client,tl::engine *t_server_shm, tl::engine *t_client_shm,std::vector<std::string> &ips,std::vector<std::string> &shm_addrs,std::vector<tl::endpoint> &serveraddrs)
    {

	dmap->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ips,shm_addrs,serveraddrs);
	dmap->bind_functions();
	MPI_Barrier(MPI_COMM_WORLD);
    }

  int num_events()
  {
	  return event_count;
  }

  int num_dropped_events()
  {
	  return dmap->num_dropped();
  }
  void create_write_buffer()
  {
     int total_size = 65536*2;
     uint64_t maxkey = UINT64_MAX;
     dmap->create_table(total_size,maxkey);
     std::vector<struct event> *q = new std::vector<struct event> ();
     equeues.push_back(q);
  }
  void clear_write_buffer(int index)
  {
	dmap->LocalClearMap(index);
	equeues[index]->clear();
  }

  void add_event(event &e,int index)
  {
      uint64_t key = e.ts;
      int v = 1;

      auto t1 = std::chrono::high_resolution_clock::now();

      bool b = dmap->Insert(key,v,index);   

      auto t2 = std::chrono::high_resolution_clock::now();
      double t = std::chrono::duration<double> (t2-t1).count();
      if(max_t < t) max_t = t;
      

      if(b) 
      {
	      std::fill(e.data.begin(),e.data.end(),0);
	      equeues[index]->push_back(e);
	      event_count++; 
      }
  }

  std::vector<struct event> * get_write_buffer(int index)
  {
	  return equeues[index];
  }

};

#endif