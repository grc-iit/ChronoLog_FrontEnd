#include "write_buffer.h"


void databuffers::create_write_buffer(int maxsize)
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

void databuffers::add_event(event &e,int index)
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
              std::memset(e.data,0,VALUESIZE);
              int ps = atomicbuffers[index]->buffer_size.load();
              (*atomicbuffers[index]->buffer)[ps] = e;
              atomicbuffers[index]->buffer_size.fetch_add(1);
              event_count++;
      }
}

std::vector<struct event> * databuffers::get_write_buffer(int index)
{
          return atomicbuffers[index]->buffer;
}

atomic_buffer* databuffers::get_atomic_buffer(int index)
{
        return atomicbuffers[index];
}



