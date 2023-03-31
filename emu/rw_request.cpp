#include "rw_request.h"

void create_events_total_order(struct thread_arg *t)
{

      t->np->create_events(t->num_events,t->name);

}

void sort_events(struct thread_arg *t)
{
	t->np->sort_events(t->name);	
}
