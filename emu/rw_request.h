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
  hid_t async_fapl;
  hid_t async_dxpl;
  std::pair<uint64_t,uint64_t> range;
  std::vector<hsize_t> total_records;
  std::vector<hsize_t> offsets;
  std::vector<hsize_t> numrecords;
};

void create_events_total_order(struct thread_arg *);
void sort_events(struct thread_arg *);
void write_events(struct thread_arg *t);
void search_events(struct thread_arg *t);
void get_events_range(struct thread_arg *t);
void open_write_stream(struct thread_arg *t);
void close_write_stream(struct thread_arg *t);
void io_polling(struct thread_arg *t);

#endif
