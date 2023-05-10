#ifndef __RW_H_
#define __RW_H_

#include <abt.h>
#include <mpi.h>
#include "ClockSync.h"
#include <hdf5.h>
#include <boost/container_hash/hash.hpp>
#include "write_buffer.h"
#include "distributed_sort.h"
#include "data_server_client.h"
#include "event_metadata.h"
#include "nvme_buffer.h"
#include <boost/lockfree/queue.hpp>
#include "h5_async_lib.h"
#include <thread>

using namespace boost;

struct thread_arg_w
{
  int tid;
  int num_events;
  std::string name;
  std::pair<uint64_t,uint64_t> range;
  std::vector<hsize_t> total_records;
  std::vector<hsize_t> offsets;
  std::vector<hsize_t> numrecords;
};


struct io_request
{
   std::string name;
   bool from_nvme;
   hsize_t total_records;
};


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
      nvme_buffers *nm;
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
      std::vector<struct thread_arg_w> t_args;
      std::vector<std::thread> workers;    
      boost::lockfree::queue<struct io_request*> *io_queue_async;
      boost::lockfree::queue<struct io_request*> *io_queue_sync;
      std::atomic<int> end_of_session;
      std::atomic<int> num_streams;
      int num_io_threads;
      std::vector<struct thread_arg_w> t_args_io;
      std::vector<std::thread> io_threads;

public:
	read_write_process(int r,int np,ClockSynchronization<ClocksourceCPPStyle> *C,int n) : myrank(r), numprocs(np), numcores(n)
	{
           H5open();
	   H5VLis_connector_registered_by_name("async");
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
	   nm = new nvme_buffers(numprocs,myrank);
	   io_queue_async = new boost::lockfree::queue<struct io_request*> (128);
	   io_queue_sync = new boost::lockfree::queue<struct io_request*> (128);
	   end_of_session.store(0);
	   num_streams.store(0);
	   num_io_threads = 2;
	   std::function<void(struct thread_arg_w *)> IOFunc(
           std::bind(&read_write_process::io_polling,this, std::placeholders::_1));

	   std::function<void(struct thread_arg_w*)> IOFuncSeq(
	   std::bind(&read_write_process::io_polling_seq,this,std::placeholders::_1));

	   t_args_io.resize(num_io_threads);
	   io_threads.resize(num_io_threads);
	   t_args_io[0].tid = 0;
	   std::thread iothread0{IOFunc,&t_args_io[0]};
	   io_threads[0] = std::move(iothread0);
	   t_args_io[1].tid = 1;
	   std::thread iothread1{IOFuncSeq,&t_args_io[1]};
	   io_threads[1] = std::move(iothread1);

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
	   delete nm;
	   delete io_queue_async;
	   delete io_queue_sync;
	   H5close();

	}

	void end_sessions()
	{
		end_of_session.store(1);

		for(int i=0;i<num_io_threads;i++) io_threads[i].join();

	}
	void sync_queue_push(struct io_request *r)
	{
		io_queue_sync->push(r);
	}
	void spawn_write_streams(std::vector<std::string> &snames,std::vector<int> &total_events,int nbatches)
	{

		int num_threads = snames.size();
		t_args.resize(num_threads);
		workers.resize(num_threads);

		for(int i=0;i<snames.size();i++)
		{
	   	   t_args[i].tid = i;
		   int numevents = total_events[i];
		   int events_per_proc = numevents/numprocs;
		   int rem = numevents%numprocs;
		   if(myrank < rem)  
		   t_args[i].num_events = events_per_proc+1;
		   else t_args[i].num_events = events_per_proc;
		   t_args[i].name = snames[i];
		}
		

		std::function<void(struct thread_arg_w *)> DataFunc(
		std::bind(&read_write_process::data_stream,this,std::placeholders::_1));
		
		for(int i=0;i<nbatches;i++)
		{
			for(int j=0;j<num_threads;j++)
  			{
        			std::thread t{DataFunc,&t_args[j]};
        			workers[j] = std::move(t);
  			}

  			for(int j=0;j<num_threads;j++) workers[j].join();
	
			
			for(int j=0;j<num_threads;j++)
			{
				 struct io_request *r = new struct io_request();
         			 r->name = t_args[j].name;
         		         r->from_nvme = true;
         			 io_queue_async->push(r);
			}
			
			/*struct io_request *r = new struct io_request();
			r->name = "table"+std::to_string(0);
		        r->from_nvme = false;
			io_queue_sync->push(r);*/

			num_streams.store(num_threads);
			while(num_streams.load()!=0);
		
		}			
		
	}

	void create_write_buffer(std::string &s,event_metadata &em,int maxsize)
	{
            m1.lock(); 
	    if(write_names.find(s)==write_names.end())
	    {
	      struct atomic_buffer *ev = nullptr;
	      myevents.push_back(ev);
	      std::pair<int,event_metadata> p1(myevents.size()-1,em);
	      std::pair<std::string,std::pair<int,event_metadata>> p2(s,p1);
	      write_names.insert(p2);
	      dm->create_write_buffer(maxsize);
	      ds->create_sort_buffer();
	      nm->create_nvme_buffer(s,em);
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
	    ds->sort_data(index,myevents[index]->buffer_size.load(),min_v,max_v);
	    myevents[index]->buffer_size.store(myevents[index]->buffer->size());
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

	void buffer_in_nvme(std::string &s)
	{
	   m1.lock();
	   auto r = write_names.find(s);
	   int index = (r->second).first;
	   m1.unlock();

	   boost::shared_lock<boost::shared_mutex> lk(myevents[index]->m);
	
	   nm->copy_to_nvme(s,myevents[index]->buffer,myevents[index]->buffer_size.load());
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
		int size = myevents[index]->buffer_size.load();
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
		   for(int i=0;i<myevents[index]->buffer_size.load();i++)
		   {
		     uint64_t ts = (*(myevents[index]->buffer))[i].ts;
		     if(ts >= min && ts <= max) oup.push_back((*(myevents[index]->buffer))[i]);
		   }
	           err = true;	   
	     }
	     return err;
	}
	
	void create_events(int num_events,std::string &s,double);
	void clear_events(std::string &s);
	void get_range(std::string &s);
	void pwrite_extend_files(std::vector<std::string>&,std::vector<hsize_t>&,std::vector<hsize_t>&,std::vector<std::vector<struct event>*>&,std::vector<uint64_t>&,std::vector<uint64_t>&);
	void pwrite(std::vector<std::string>&,std::vector<hsize_t>&,std::vector<hsize_t>&,std::vector<std::vector<struct event>*>&,std::vector<uint64_t>&,std::vector<uint64_t>&);
	void pwrite_files(std::vector<std::string> &,std::vector<hsize_t> &,std::vector<hsize_t>&,std::vector<std::vector<struct event>*>&,std::vector<uint64_t>&,std::vector<uint64_t>&);
	void preaddata(const char*,std::string &s);
	void preadfileattr(const char*);
	std::vector<struct event>* create_data_spaces(std::string &,hsize_t&,hsize_t&,uint64_t&,uint64_t&,bool);
	void io_polling(struct thread_arg_w*);
	void io_polling_seq(struct thread_arg_w*);
	void data_stream(struct thread_arg_w*);
};

#endif
