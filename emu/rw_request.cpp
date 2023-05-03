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
   int niter = 4;
   std::string filename = "file"+t->name+".h5";
   for(int i=0;i<niter;i++)
   {
	t->np->create_events(t->num_events,t->name,1);
	t->np->sort_events(t->name);
	t->np->buffer_in_nvme(t->name);
	t->np->clear_events(t->name);
	//struct io_request *r = new struct io_request();
	//r->name = t->name;
	//r->from_nvme = true;
	//if(i%2==0)
	//io_queue->push(r);

	//std::cout <<" num dropped_events = "<<t->np->dropped_events()<<std::endl;
	//t->np->pwrite_new_from_file(filename.c_str(),t->name);
   }
}

void close_write_stream(struct thread_arg *t)
{
}

void io_polling(struct thread_arg *t)
{

  boost::lockfree::queue<struct io_request*> *io_queue = t->np->get_io_queue();

  std::vector<std::string> snames;
  std::vector<std::vector<struct event>*> data;

  int nreq = 0;
  while(true)
  {

     std::atomic_thread_fence(std::memory_order_seq_cst);

     while(t->np->get_num_streams()==0 && t->np->get_end_of_session()==0);

     if(t->np->get_end_of_session()==1) break;

     int nstreams = t->np->get_num_streams();

     snames.clear(); data.clear();

     for(int i=0;i<nstreams;i++)
     {
       struct io_request *r;

       io_queue->pop(r);

       if(r->from_nvme)
       {
	   hsize_t total_records, offset, numrecords;
	   std::vector<struct event> *data_r = nullptr;
           data_r = t->np->create_data_spaces(r->name,offset,total_records,true);	
	   snames.push_back(r->name);
	   t->total_records.push_back(total_records);
	   t->offsets.push_back(offset); 
	   //t->numrecords.push_back(numrecords);
	   data.push_back(data_r);
       }
       else
       {
        std::string filename = "file"+r->name+".h5";
       }
  
       delete r;  
    }

    t->np->set_num_streams(0);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    t->np->pwrite(snames,t->total_records,t->offsets,data);

    //t->np->set_num_streams(0);
     t->total_records.clear();
     t->offsets.clear();
     t->numrecords.clear();
    }
 

  MPI_Barrier(MPI_COMM_WORLD);

}
