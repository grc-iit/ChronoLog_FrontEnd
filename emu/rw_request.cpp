#include "rw_request.h"

void create_events_total_order(struct thread_arg *t)
{

      t->np->create_events(t->num_events,t->name,1);

}

void sort_events(struct thread_arg *t)
{
	t->np->sort_events(t->name);	
}

void write_events(struct thread_arg *t)
{
	std::string filename = "file"+t->name+".h5";
 	t->np->pwrite(filename.c_str(),t->name);
}

void get_events_range(struct thread_arg *t)
{
   uint64_t min_v_r,max_v_r;
   bool b1 = t->np->get_range_in_read_buffers(t->name,min_v_r,max_v_r);
   if(b1) std::cout <<" min_v_r = "<<min_v_r<<" max_v_r = "<<max_v_r<<std::endl;
 

   uint64_t min_v_w,max_v_w;
   b1 = t->np->get_range_in_write_buffers(t->name,min_v_w,max_v_w);
   if(b1) 
   {
	t->range.first = min_v_w;
	t->range.second = max_v_w;
   }


}

void search_events(struct thread_arg *t)
{

	uint64_t min = t->range.first;
	uint64_t max = t->range.second;
	std::vector<struct event> range_events;
	t->np->get_events_in_range_from_write_buffers(t->name,t->range,range_events);

	std::string aname = "attr"+std::to_string(1);

	event_metadata em = t->np->get_metadata(t->name);
	std::vector<view> resp;
        t->q->sort_events_by_attr(t->name,range_events,aname,em,resp);
}

void open_write_stream(struct thread_arg *t)
{
   boost::lockfree::queue<struct io_request*> *io_queue = t->np->get_io_queue();
   int niter = 1;
   std::string filename = "file"+t->name+".h5";
   for(int i=0;i<niter;i++)
   {
	t->np->create_events(t->num_events,t->name,1);
	//t->np->sort_events(t->name);
	//t->np->buffer_in_nvme(t->name);
	//t->np->clear_events(t->name);
	struct io_request *r = new struct io_request();
	r->name = t->name;
	r->from_nvme = true;
	//if(i%2==0)
	io_queue->push(r);

	//std::cout <<" num dropped_events = "<<t->np->dropped_events()<<std::endl;
	//t->np->pwrite_new_from_file(filename.c_str(),t->name);
   }
}

void close_write_stream(struct thread_arg *t)
{
   int niter = 1;
   std::string filename = "file"+t->name+".h5";
   for(int i=0;i<niter;i++)
   {
	t->np->pwrite_new_from_file(filename.c_str(),t->name);
   }
}

void io_polling(struct thread_arg *t)
{

  boost::lockfree::queue<struct io_request*> *io_queue = t->np->get_io_queue();

  while(true)
  {
     while(!io_queue->empty())
     {
       struct io_request *r;

       io_queue->pop(r);

       if(r->from_nvme)
       {
	 t->np->sort_events(r->name);
         std::string filename = "file"+r->name+".h5";
         t->np->pwrite_from_file(filename.c_str(),r->name);
       }
       else
       {
        std::string filename = "file"+r->name+".h5";
        t->np->pwrite(filename.c_str(),r->name);	
       }
  
       delete r;  
    }
    if(io_queue->empty()) break;
    //if(t->np->get_end_of_session()==1 && io_queue->empty()) break;

 }


  MPI_Barrier(MPI_COMM_WORLD);


}
