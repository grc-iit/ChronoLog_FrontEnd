#include "query_engine.h"

void query_engine::send_query(std::string &s)
{
       struct query_req r;
       r.name = s;
       r.minkey = 0;
       r.maxkey = UINT64_MAX;
       r.collective = false;
       if(myrank==0)
       {
          std::cout <<" send_query = "<<s<<std::endl;
          Q->PutAll(r);
        }

}


void query_engine::service_query(struct thread_arg_q* t) 
{
           while(true)
           {
              while(!Q->Empty())
             {
              struct query_req *r=nullptr;
              r = Q->Get();

	      if(r==nullptr) break;

	      uint64_t min_key1,max_key1;

	      bool b = rwp->get_range_in_write_buffers(r->name,min_key1,max_key1);

	      //if(b) std::cout <<" rank = "<<myrank<<" min_key = "<<min_key1<<" max_key = "<<max_key1<<std::endl;

	      uint64_t min_key2, max_key2;

	      b = rwp->get_range_in_read_buffers(r->name,min_key2,max_key2);
	     
	      if(!(r->maxkey < min_key1 ||
		 r->minkey > max_key1))
	      { 
                 atomic_buffer *au = nullptr;

                 au = rwp->get_write_buffer(r->name);

                 int size1 = 0;
                 if(au != nullptr)
                 {

                  boost::shared_lock<boost::shared_mutex> lk(au->m);
                  {
                        size1 = au->buffer_size.load();
                  }
                }

                std::vector<struct event> *buf = rwp->get_nvme_buffer(r->name);

                int size2 = 0;
                size2 = (buf==nullptr)? 0 : buf->size();
                //std::cout <<" rank = "<<myrank<<" size1 = "<<size1<<" size2 = "<<size2<<std::endl;
	      }
              
	      std::string filename = "file";
              filename += r->name+".h5";

	      uint64_t min_key3, max_key3;
	      b = rwp->get_range_in_file(filename,min_key3,max_key3);

	      if(!(r->minkey > max_key3 ||
	         r->maxkey < min_key3))
	      rwp->preaddata(filename.c_str(),r->name);

              delete r;
             }

             if(end_of_session.load()==1) break;
           }
}

