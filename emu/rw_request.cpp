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
   int niter = 4;
   std::string filename = "file"+t->name+".h5";
   for(int i=0;i<niter;i++)
   {
	t->np->create_events(t->num_events,t->name,1);
	t->np->sort_events(t->name);
	/*hid_t s1, s2,s3,s4;
	hsize_t total_records = t->np->create_data_spaces_from_memory(t->name,s1,s2,s3,s4);
	   
	t->spaces.push_back(s1);
	t->filespaces.push_back(s2);
	t->memspaces.push_back(s3);
	t->datasetpl.push_back(s4);
	t->total_records.push_back(total_records);*/
	//std::vector<std::string> sname;
	//sname.push_back(t->name);
	//t->np->pwrite_files(sname);
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
   int niter = 1;
   std::string filename = "file"+t->name+".h5";
   for(int i=0;i<niter;i++)
   {
	 std::vector<std::string> name;
	 name.push_back(t->name);
	//t->np->pwrite(filename.c_str(),t->name);
	/*hid_t meta, meta_e, dtag;
	t->np->pwrite_new_from_file(filename.c_str(),t->name,meta,meta_e,dtag);
	t->meta_events.push_back(meta);
	t->meta_end_events.push_back(meta_e);
	t->data_events.push_back(dtag);*/
	/*MPI_Barrier(MPI_COMM_WORLD);*/
   }
}

void io_polling(struct thread_arg *t)
{

  boost::lockfree::queue<struct io_request*> *io_queue = t->np->get_io_queue();

  std::vector<std::string> snames;

  int nreq = 0;
  while(true)
  {

     while(!io_queue->empty())
     {
       struct io_request *r;

       io_queue->pop(r);

       if(r->from_nvme)
       {
	 snames.push_back(r->name);
       }
       else
       {
        std::string filename = "file"+r->name+".h5";
        t->np->pwrite(filename.c_str(),r->name);	
       }
  
       delete r;  
    }

    if(io_queue->empty()) break;

 }
    
  t->np->pwrite_files_from_memory(snames,t->spaces,t->filespaces,t->memspaces,t->datasetpl,t->total_records);

  t->spaces.clear();
  t->filespaces.clear();
  t->memspaces.clear();
  t->datasetpl.clear();
  t->total_records.clear();

  MPI_Barrier(MPI_COMM_WORLD);

}
