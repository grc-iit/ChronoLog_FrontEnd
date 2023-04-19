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
	std::vector<interprocess_sharable_mutex*> file_locks;
        std::string prefix;
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

	}

	void create_nvme_buffer(std::string &s,event_metadata &em)
	{
	   std::string fname = prefix+s;
	   auto r = nvme_fnames.find(fname);
	
	   if(r == nvme_fnames.end())
	   {
	      file_mapping::remove(fname.c_str());
	      managed_mapped_file *mf = new managed_mapped_file(create_only,fname.c_str(),MAXFILESIZE);
	      const allocator_event_t allocator_e(mf->get_segment_manager());
	      std::string vecname = fname+"MyEventVector";
	      MyEventVect *ev = mf->construct<MyEventVect> (vecname.c_str()) (allocator_e); 
	      interprocess_sharable_mutex *m = new interprocess_sharable_mutex();
	      file_locks.push_back(m);
              nvme_ebufs.push_back(ev);
	      nvme_files.push_back(mf);
	      file_names.push_back(fname);
	      buffer_names.push_back(vecname);
	      std::pair<int,event_metadata> p1(file_names.size()-1,em);	   
	      std::pair<std::string,std::pair<int,event_metadata>> p2(fname,p1);   
	      nvme_fnames.insert(p2);
	   }
	}

	void copy_to_nvme(std::string &s,std::vector<struct event> *inp)
	{
	   std::string fname = prefix+s;
	   auto r = nvme_fnames.find(fname);
	   
	   if(r == nvme_fnames.end()) return;

	   int index = r->second.first;

	   boost::interprocess::scoped_lock<interprocess_sharable_mutex> lk(*file_locks[index]); 

	   MyEventVect *ev = nvme_ebufs[index];

	   for(int i=0;i<inp->size();i++)
	     ev->push_back((*inp)[i]);
	   nvme_files[index]->flush();
	}
	
	std::vector<struct event> *fetch_buffer(std::string &s)
	{
	
	  std::string fname = prefix+s;
	  auto r = nvme_fnames.find(fname);

	  if(r==nvme_fnames.end()) return nullptr;

	  int index = r->second.first;

	  boost::interprocess::scoped_lock<interprocess_sharable_mutex> lk(*file_locks[index]);

	  std::vector<struct event> *data_array = new std::vector<struct event> ();

	  MyEventVect *ev = nvme_ebufs[index];

	  for(int i=0;i<ev->size();i++)
	  {	
		data_array->push_back((*ev)[i]);
	  }

	  nvme_ebufs[index]->clear();

	  return data_array;
	}
	

};

#endif

#endif
