#include "query_engine.h"

void query_engine::send_query(std::string &s)
{
       struct query_req r;
       r.name = s;
       r.minkey = 0;
       r.maxkey = UINT64_MAX;
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

              std::string filename = "file";
              filename += r->name+".h5";

	      rwp->preaddata(filename.c_str(),r->name);

              delete r;
             }

             if(end_of_session.load()==1) break;
           }
}

