#include "write_buffer.h"
#include <boost/container_hash/hash.hpp>


atomic_buffer* databuffers::create_write_buffer(int maxsize)
{
     int total_size = 65536*2;
     uint64_t maxkey = UINT64_MAX;
     dmap->create_table(total_size,maxkey);
     struct atomic_buffer *a = new struct atomic_buffer();
     a->buffer_size.store(0);
     a->buffer = new std::vector<struct event> (maxsize);
     m1.lock();
     atomicbuffers.push_back(a);
     m1.unlock();
     return a;
}

void databuffers::clear_write_buffer(int index)
{
        boost::upgrade_lock<boost::shared_mutex> lk(atomicbuffers[index]->m);
        dmap->LocalClearMap(index);
        atomicbuffers[index]->buffer_size.store(0);
        atomicbuffers[index]->buffer->clear();
}
  
void databuffers::clear_write_buffer_no_lock(int index)
{
        dmap->LocalClearMap(index);
        atomicbuffers[index]->buffer_size.store(0);
        atomicbuffers[index]->buffer->clear();
}

void databuffers::set_valid_range(int index,uint64_t &n1,uint64_t &n2)
{
        dmap->set_valid_range(index,n1,n2);
}

bool databuffers::add_event(event &e,int index)
{
      uint64_t key = e.ts;
      int v = myrank;

      auto t1 = std::chrono::high_resolution_clock::now();

      bool b = dmap->Insert(key,v,index);

      auto t2 = std::chrono::high_resolution_clock::now();
      double t = std::chrono::duration<double> (t2-t1).count();
      if(max_t < t) max_t = t;


      if(b)
      {
	      int bc = 0;
	      int numuints = std::ceil(VALUESIZE/sizeof(uint64_t));
	      for(int j=0;j<numuints;j++)
	      {
		std::size_t seed=0;     
	        uint64_t key1 = key+j;	
		boost::hash_combine(seed,key1);
		uint64_t mask = 127;
		bool end = false;
		for(int k=0;k<sizeof(uint64_t);k++)
		{
		   uint64_t v = mask & seed;
		   char c = (char)v;
		   seed = seed >> 8;
		   e.data[bc] = c;
		   bc++;
		   if(bc==VALUESIZE) 
		   {
			end = true; break;
		   }
		}
		if(end) break;
	      }
	     
              int ps = atomicbuffers[index]->buffer_size.load();
              (*atomicbuffers[index]->buffer)[ps] = e;
              atomicbuffers[index]->buffer_size.fetch_add(1);
              event_count++;
      }
      return b;
}

std::vector<struct event> * databuffers::get_write_buffer(int index)
{
          return atomicbuffers[index]->buffer;
}

atomic_buffer* databuffers::get_atomic_buffer(int index)
{
        return atomicbuffers[index];
}



