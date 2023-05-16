#include "query_engine.h"

void query_engine::send_query(std::string &s)
{
       int count = query_number.fetch_add(1);
       struct query_req r;
       r.name = s;
       r.minkey = 0;
       r.maxkey = UINT64_MAX;
       r.collective = false;
       r.id = count;
       r.output_file = false;
       if(myrank==0)
       {
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

	      uint64_t min_key2, max_key2;

	      b = rwp->get_range_in_read_buffers(r->name,min_key2,max_key2);
	    
	      std::vector<struct event> *buf1 = nullptr;
	      std::vector<struct event> *buf2 = nullptr;
	      std::vector<struct event> *buf3 = nullptr;

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
			if(size1 > 0) 
			{
			    buf1 = new std::vector<struct event> ();
			    buf1->resize(size1);
			    for(int i=0;i<size1;i++)
			    {
				(*buf1)[i] = (*(au->buffer))[i];
			    }
			}
                  }
                }

                buf2 = rwp->get_nvme_buffer(r->name);

                int size2 = 0;
                size2 = (buf2==nullptr)? 0 : buf2->size();
	      }
              
	      std::string filename = "file";
              filename += r->name+".h5";

	      uint64_t min_key3, max_key3;
	      b = rwp->get_range_in_file(filename,min_key3,max_key3);

	      if(!(r->minkey > max_key3 ||
	         r->maxkey < min_key3))
	      {
	         rwp->preaddata(filename.c_str(),r->name);
		 atomic_buffer *ru = nullptr;
	         ru = rwp->get_read_buffer(r->name);
		 if(ru != nullptr)
		 {
		    boost::shared_lock<boost::shared_mutex> lk(ru->m);
		    {
			buf3 = new std::vector<struct event> ();
			buf3->resize(ru->buffer->size());

			for(int i=0;i<ru->buffer->size();i++)
			   (*buf3)[i] = (*(ru->buffer))[i];
		    }

		 }
	      }

	      if(buf1 != nullptr) delete buf1; 
	      if(buf2 != nullptr) delete buf2;
	      if(buf3 != nullptr) delete buf3;
              delete r;
             }

             if(end_of_session.load()==1) break;
           }
}

