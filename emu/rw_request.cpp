#include "rw_request.h"

void create_events_total_order(struct thread_arg *t)
{

      t->np->create_events(t->num_events,t->name);

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

void get_event_range(struct thread_arg *t)
{
   uint64_t min_v,max_v;
   t->np->get_range(t->name,min_v,max_v);

}

void search_events(struct thread_arg *t)
{

}
