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
#include "event_metadata.h"
#include <boost/thread/shared_mutex.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>

using namespace boost;

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
      boost::mutex m1;
      boost::mutex m2;
      std::set<std::string> file_names;
      std::unordered_map<std::string,std::pair<int,event_metadata>> write_names;
      std::unordered_map<std::string,std::pair<int,event_metadata>> read_names;
      std::unordered_map<std::string,std::pair<uint64_t,uint64_t>> write_interval;
      std::unordered_map<std::string,std::pair<uint64_t,uint64_t>> read_interval;
      std::unordered_map<std::string,std::pair<uint64_t,uint64_t>> file_minmax;
      std::vector<struct atomic_buffer*> myevents;
      std::vector<struct atomic_buffer*> readevents;
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
	   for(int i=0;i<readevents.size();i++)
	   {
		delete readevents[i]->buffer;
		delete readevents[i];
	   }
	   H5close();

	}

	void create_write_buffer(std::string &s,event_metadata &em)
	{
            m1.lock(); 
	    if(write_names.find(s)==write_names.end())
	    {
	      struct atomic_buffer *ev = nullptr;
	      myevents.push_back(ev);
	      std::pair<int,event_metadata> p1(myevents.size()-1,em);
	      std::pair<std::string,std::pair<int,event_metadata>> p2(s,p1);
	      write_names.insert(p2);
	      dm->create_write_buffer();
	      ds->create_sort_buffer();
	    }
	    m1.unlock();
	}	
	void create_read_buffer(std::string &s,event_metadata &em)
	{
	    m2.lock();
	    auto r = read_names.find(s);;
	    if(r==read_names.end())
	    {
	        struct atomic_buffer *ev = new struct atomic_buffer ();
    	        ev->buffer = new std::vector<struct event> ();		
		readevents.push_back(ev);
		std::pair<int,event_metadata> p1(readevents.size()-1,em);
		std::pair<std::string,std::pair<int,event_metadata>> p2(s,p1);
		read_names.insert(p2);
	    }	
	    m2.unlock();
	}
	void get_events_from_map(std::string &s)
	{
	   m1.lock();
           auto r = write_names.find(s);
	   int index = (r->second).first;
	   m1.unlock();
	   myevents[index] = dm->get_atomic_buffer(index);
	}
	
	void sort_events(std::string &s)
	{
	    m1.lock();
	    auto r = write_names.find(s);
	    int index = (r->second).first;
	    m1.unlock();
	    get_events_from_map(s);
	    boost::upgrade_lock<boost::shared_mutex> lk(myevents[index]->m);
	    ds->get_unsorted_data(myevents[index]->buffer,index);
	    uint64_t min_v,max_v;
	    ds->sort_data(index,min_v,max_v);
	    m1.lock();
	    auto r1 = write_interval.find(s);
	    if(r1 == write_interval.end())
	    {
		std::pair<uint64_t,uint64_t> p(min_v,max_v);
		std::pair<std::string,std::pair<uint64_t,uint64_t>> q(s,p);
		write_interval.insert(q);
	    }
	    else 
	    {
		(r1->second).first = min_v;
		(r1->second).second = max_v;
	    }
	    m1.unlock();
	}
	event_metadata & get_metadata(std::string &s)
	{
	        m1.lock(); 
		auto r = write_names.find(s);
		event_metadata em = (r->second).second;
		m1.unlock();
		return em;
	}
	bool get_range_in_file(std::string &s, uint64_t &min_v,uint64_t &max_v)
	{
	   min_v = UINT64_MAX; max_v = 0;
	   bool err = false;

	   auto r = std::find(file_names.begin(),file_names.end(),s);
	   if(r != file_names.end())
	   {
	      preadfileattr(s.c_str());
	   }

	}

        bool get_range_in_read_buffers(std::string &s,uint64_t &min_v,uint64_t &max_v)
	{
	    min_v = UINT64_MAX; max_v = 0;
	    bool err = false;
	    m2.lock();
	    auto r = read_interval.find(s);
	    if(r != read_interval.end())
	    {
		min_v = (min_v < (r->second).first) ? min_v : (r->second).first;
		max_v = (max_v > (r->second).second) ? max_v : (r->second).second;
		err = true;
	    }
	    m2.unlock();
	    return err;
	}

	bool get_range_in_write_buffers(std::string &s,uint64_t &min_v,uint64_t &max_v)
	{
	    min_v = UINT64_MAX; max_v = 0;
	    bool err = false;
	    m1.lock();
	    auto r = write_interval.find(s);
	    if(r != write_interval.end())
	    {
		min_v = (min_v < (r->second).first) ? min_v : (r->second).first;
		max_v = (max_v > (r->second).second) ? max_v : (r->second).second;
		err = true;
	    }
	    m1.unlock();
	    return err;
	}

	int num_write_events(std::string &s)
	{
	        m1.lock();
		auto r = write_names.find(s);
		int index = (r->second).first;
		m1.unlock();
		boost::shared_lock<boost::shared_mutex> lk(myevents[index]->m);
		int size = myevents[index]->buffer->size();
		return size;
	}
	int dropped_events()
	{
	    return dm->num_dropped_events();
	}
        bool get_events_in_range_from_read_buffers(std::string &s,std::pair<uint64_t,uint64_t> &range,std::vector<struct event> &oup)
	{
	     uint64_t min = range.first; uint64_t max = range.second;
	     bool err = false;
	     m2.lock();
	     auto r = read_interval.find(s);
	     int index = -1;
	     uint64_t min_s, max_s;
	     if(r != read_interval.end())
	     {
		min_s = (r->second).first;
		max_s = (r->second).second;
		if(!((max < min_s) && (min > max_s)))
		{
		   min_s = std::max(min_s,min);
		   max_s = std::min(max_s,max);
		   
		   auto r1 = read_names.find(s);
		   index = (r1->second).first;
		}
	     }
	     m2.unlock();

	     if(index != -1)
	     {
	   	   boost::shared_lock<boost::shared_mutex> lk(readevents[index]->m);	   
		   for(int i=0;i<readevents[index]->buffer->size();i++)
		   {
			uint64_t ts = (*readevents[index]->buffer)[i].ts;
	     	        if(ts >= min_s && ts <= max_s) oup.push_back((*readevents[index]->buffer)[i]);		
		   }	  
		   err = true; 
	     }
	    
	     return err;
	}

	bool get_events_in_range_from_write_buffers(std::string &s,std::pair<uint64_t,uint64_t> &range,std::vector<struct event> &oup)
	{
	     bool err = false;
	     uint64_t min = range.first; uint64_t max = range.second;
	     m1.lock();
	     auto r = write_interval.find(s);
	     uint64_t min_s,max_s;
	     int index = -1;
	     if(r != write_interval.end())
	     {
		min_s = (r->second).first;
		max_s = (r->second).second;
		if(!((min > max_s) && (max < min_s)))
		{	
		   min_s = std::max(min,min_s);
		   max_s = std::min(max,max_s);
	           auto r1 = write_names.find(s);
		   index = (r1->second).first;
		}
	     }
	     m1.unlock();

	     if(index != -1)
	     {
		   boost::shared_lock<boost::shared_mutex> lk(myevents[index]->m);
		   for(int i=0;i<myevents[index]->buffer->size();i++)
		   {
		     uint64_t ts = (*(myevents[index]->buffer))[i].ts;
		     if(ts >= min && ts <= max) oup.push_back((*(myevents[index]->buffer))[i]);
		   }
	           err = true;	   
	     }
	     return err;
	}
	void create_events(int num_events,std::string &s);
	void clear_events(std::string &s);
	void get_range(std::string &s);
	void pwrite(const char *,std::string &s);
        void pwrite_new(const char *,std::string &s);
	void pwrite_extend(const char*,std::string &s);
	void preaddata(const char*,std::string &s);
	void preadfileattr(const char*);
};

#endif
