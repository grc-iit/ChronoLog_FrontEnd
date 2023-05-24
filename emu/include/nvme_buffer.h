#ifndef __NVME_BUFFER_H_
#define __NVME_BUFFER_H_

#include <boost/thread/shared_mutex.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>

#include "event.h"
#include "event_metadata.h"

#define MAXFILESIZE 32768*VALUESIZE

using namespace boost::interprocess;

#if defined(BOOST_INTERPROCESS_MAPPED_FILES)

typedef allocator<struct event,managed_mapped_file::segment_manager> allocator_event_t;
typedef boost::interprocess::vector<struct event,allocator_event_t> MyEventVect;


class nvme_buffers
{

  private:
	int numprocs;
	int myrank;
	std::unordered_map<std::string,std::pair<int,event_metadata>> nvme_fnames;
	std::vector<std::string> buffer_names;
	std::vector<std::string> file_names;
	std::vector<MyEventVect*> nvme_ebufs;
	std::vector<managed_mapped_file*> nvme_files;
	std::vector<boost::shared_mutex*> file_locks;
        std::string prefix;
	std::vector<std::atomic<int>*> buffer_state;
  public:
	nvme_buffers(int np,int rank) : numprocs(np), myrank(rank)
	{
	   prefix = "/mnt/nvme/asasidharan/rank"+std::to_string(myrank);
	}
	~nvme_buffers()
	{
	   for(int i=0;i<nvme_files.size();i++)
	   {
	      	nvme_files[i]->destroy<MyEventVect>(buffer_names[i].c_str());		
		nvme_files[i]->flush();
		delete file_locks[i];
	   }
	   for(auto t = nvme_fnames.begin(); t != nvme_fnames.end(); ++t)
	   {
		std::string fn = t->first;
		file_mapping::remove(fn.c_str());
	   }
	   for(int i=0;i<buffer_state.size();i++) std::free(buffer_state[i]);
	}

	void create_nvme_buffer(std::string &s,event_metadata &em);
	void copy_to_nvme(std::string &s,std::vector<struct event> *inp,int numevents);
	boost::shared_mutex *get_mutex(std::string &);
	void get_buffer(int,int,int);
	void erase_from_nvme(std::string &s, int numevents);
	void fetch_buffer(std::vector<struct event> *,std::string &s,int &,int &);

};

#endif

#endif
