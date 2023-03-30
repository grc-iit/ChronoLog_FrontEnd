#ifndef __RW_H_
#define __RW_H_

#include <abt.h>
#include <mpi.h>
#include "ClockSync.h"
#include "hdf5.h"
#include <boost/container_hash/hash.hpp>
#include "write_buffer.h"
#include "distributed_sort.h"
#include "data_server_client.h"

class read_write_process
{

private:
      ClockSynchronization<ClocksourceCPPStyle> *CM;
      int myrank;
      int numprocs;
      int numcores;
      boost::hash<uint64_t> hasher;
      uint64_t seed = 1;
      databuffers *dm;
      std::unordered_map<std::string,int> write_names;
      std::unordered_map<std::string,int> read_names;
      std::vector<std::vector<struct event>*> myevents;
      std::vector<std::vector<struct event>> readevents;
      dsort *ds;
      data_server_client *dsc;
public:
	read_write_process(int r,int np,ClockSynchronization<ClocksourceCPPStyle> *C,int n) : myrank(r), numprocs(np), numcores(n)
	{
           H5open();
           std::string unit = "microsecond";
	   CM = C;
	   dsc = new data_server_client(numprocs,myrank);
	   tl::engine *t_server = dsc->get_thallium_server();
	   tl::engine *t_server_shm = dsc->get_thallium_shm_server();
	   tl::engine *t_client = dsc->get_thallium_client();
	   tl::engine *t_client_shm = dsc->get_thallium_shm_client();
	   std::vector<tl::endpoint> server_addrs = dsc->get_serveraddrs();
	   std::vector<std::string> ipaddrs = dsc->get_ipaddrs();
	   std::vector<std::string> shmaddrs = dsc->get_shm_addrs();
	   dm = new databuffers(numprocs,myrank,numcores,CM);
	   dm->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ipaddrs,shmaddrs,server_addrs);
	   ds = new dsort(numprocs,myrank);
	}
	~read_write_process()
	{
	   delete dm;
	   delete ds;
	   delete dsc;
	   H5close();

	}

	void create_write_buffer(std::string &s)
	{
          
	    dm->create_write_buffer(s);
	    ds->create_sort_buffer(s);
	    if(write_names.find(s)==write_names.end())
	    {
	      std::vector<struct event> *ev = nullptr;
	      myevents.push_back(ev);
	      std::pair<std::string,int> p(s,myevents.size()-1);
	      write_names.insert(p);
	    }
	}	
	void create_read_buffer(std::string &s)
	{
	    auto r = read_names.find(s);;
	    if(r==read_names.end())
	    {
	        std::vector<struct event> ev;	    
		readevents.push_back(ev);
		std::pair<std::string,int> p(s,readevents.size()-1);
		read_names.insert(p);
	    }	

	}
	void get_events_from_map(std::string &s)
	{
           auto r = write_names.find(s);
	   int index = r->second;
	   myevents[index] = dm->get_write_buffer(s);
	}
	
	void sort_events(std::string &s)
	{
	    auto r = write_names.find(s);
	    int index = r->second;
	    get_events_from_map(s);
	    ds->get_unsorted_data(myevents[index],s);
	    ds->sort_data(s); 
	}

	int num_write_events(std::string &s)
	{
		auto r = write_names.find(s);
		int index = r->second;
		return myevents[index]->size();
	}
	int dropped_events()
	{
	    return dm->num_dropped_events();
	}
	void create_events(int num_events,std::string &s);
	void clear_events(std::string &s);
        void pwrite(const char *,std::string &s);
	void pread(const char*,std::string &s);
};

#endif
