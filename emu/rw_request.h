#ifndef __RW_REQUEST_H_
#define __RW_REQUEST_H_

#include <thread>
#include "rw.h"

struct thread_arg
{
  int tid;
  read_write_process *np;
  int num_events;
  std::string name;
};


void create_events_total_order(struct thread_arg *);
void sort_events(struct thread_arg *);
void write_events(struct thread_arg *t);




#endif
