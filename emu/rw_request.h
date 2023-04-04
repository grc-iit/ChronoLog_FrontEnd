#ifndef __RW_REQUEST_H_
#define __RW_REQUEST_H_

#include <thread>
#include "rw.h"
#include "query_parser.h"

struct thread_arg
{
  int tid;
  query_parser *q;
  read_write_process *np;
  int num_events;
  std::string name;
  std::pair<uint64_t,uint64_t> range;

};


void create_events_total_order(struct thread_arg *);
void sort_events(struct thread_arg *);
void write_events(struct thread_arg *t);
void search_events(struct thread_arg *t);
void get_events_range(struct thread_arg *t);


#endif
