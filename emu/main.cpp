#include "process.h"
#include <cassert>
#include <thallium.hpp>
#include <chrono>
#include <sched.h>
#include <thread>
#include "rw_request.h"
#include "event_metadata.h"

namespace tl=thallium;
using namespace boost::interprocess;

int main(int argc,char **argv)
{

  int p;

  MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&p);

  assert(p == MPI_THREAD_MULTIPLE);

  int size,rank;
  MPI_Comm_size(MPI_COMM_WORLD,&size);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

  cpu_set_t cpu;
  pthread_t self = pthread_self();

  pthread_getaffinity_np(self, sizeof(cpu_set_t), &cpu);

  int num_cores = 0;
  for(int i=0;i<CPU_SETSIZE;i++)
  {
	if(CPU_ISSET(i,&cpu)) num_cores++;
  }

  //std::cout <<" rank = "<<rank<<" num_cores = "<<num_cores<<std::endl;

  auto t1 = std::chrono::high_resolution_clock::now();

  emu_process *np = new emu_process(size,rank,num_cores);

  np->synchronize();

  auto t2 = std::chrono::high_resolution_clock::now();

  double t = std::chrono::duration<double>(t2-t1).count();

  double total_time = 0;
  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" numprocs = "<<size<<" sync time = "<<total_time<<std::endl;

  /*metadata_client *CC = np->getclientobj();


  if(rank != 0)
  {

  std::string client_id = "client";
  client_id += std::to_string(rank);
  CC->Connect(client_id);

  std::string chronicle_name = "record";
  CC->CreateChronicle(client_id,chronicle_name);

  CC->AcquireChronicle(client_id,chronicle_name);

  CC->ReleaseChronicle(client_id,chronicle_name);

  CC->DestroyChronicle(client_id,chronicle_name);

  }*/

  int numstories = 8;
  std::vector<std::string> story_names;
  std::vector<int> total_events;

  event_metadata em;
  em.set_numattrs(625);
  for(int i=0;i<625;i++)
  {
    std::string a = "attr"+std::to_string(i);
    int vsize = sizeof(double);
    bool is_signed = false;
    bool is_big_endian = true; 
    em.add_attr(a,vsize,is_signed,is_big_endian);
  }

  for(int i=0;i<numstories;i++)
  {
	std::string name = "table"+std::to_string(i);
	story_names.push_back(name);
	total_events.push_back(8192);
	np->prepare_service(name,em);
  }


  MPI_Barrier(MPI_COMM_WORLD);

  int num_threads = 8;

  t1 = std::chrono::high_resolution_clock::now();

  std::vector<struct thread_arg> t_args(num_threads+1);
  std::vector<std::thread> workers(num_threads+1);

  for(int i=0;i<num_threads+1;i++)
  {
      int s_id = (i==num_threads) ? 0 : i;
      int events_per_proc = total_events[s_id]/size;
      int rem = total_events[s_id]%size;
      if(rank < rem) events_per_proc++;
      t_args[i].tid = i; 
      t_args[i].np = np->get_rw_object();
      t_args[i].q = np->get_query_parser_obj();
      t_args[i].num_events = events_per_proc;
      t_args[i].name = story_names[s_id];
  }

  /*for(int i=0;i<1;i++)
  {
      std::thread t{get_events_range,&t_args[i]};
      workers[i] = std::move(t);
      workers[i].join();
  }

  MPI_Barrier(MPI_COMM_WORLD);

  if(rank==0)
  for(int i=0;i<1;i++)
  {
      std::thread t{search_events,&t_args[i]};
      workers[i] = std::move(t);
      workers[i].join();
  }

  MPI_Barrier(MPI_COMM_WORLD);*/

  //std::thread iot{io_polling,&t_args[2]};

  std::thread iot{io_polling,&t_args[num_threads]};
  int nbatches = 8;

  t1 = std::chrono::high_resolution_clock::now();

  for(int n=0;n<nbatches;n++)
  {

  for(int i=0;i<num_threads;i++)
  {
	std::thread t{open_write_stream,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();

  t2 = std::chrono::high_resolution_clock::now();

  std::atomic_thread_fence(std::memory_order_seq_cst);

  boost::lockfree::queue<struct io_request*> *io_queue = t_args[0].np->get_io_queue();

  std::vector<std::string> snames;
  std::vector<std::vector<struct event>*> data;
  std::vector<hsize_t> total_records, offsets;

  for(int i=0;i<num_threads;i++)
  {
           
      /*std::vector<struct event> *data_r = nullptr;
      hsize_t offset, trecords;
      data_r = t_args[0].np->create_data_spaces(t_args[i].name,offset,trecords,true);
      snames.push_back(t_args[i].name);
      data.push_back(data_r);
      total_records.push_back(trecords);
      offsets.push_back(offset);*/

         struct io_request *r = new struct io_request();
         r->name = t_args[i].name;
         r->from_nvme = true;
         io_queue->push(r);
  }

  //t_args[0].np->pwrite(snames,total_records,offsets,data);

  t_args[0].np->set_num_streams(num_threads);
  std::atomic_thread_fence(std::memory_order_seq_cst); 
  while(t_args[0].np->get_num_streams()!=0);

  }

  t_args[0].np->mark_end_of_session();

  iot.join();

  t2 = std::chrono::high_resolution_clock::now();

  t = std::chrono::duration<double> (t2-t1).count();


  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" Total time = "<<total_time<<std::endl;
  /*
  int numevents = t_args[0].np->num_write_events(t_args[0].name);

  int totalevents = 0;
  //MPI_Allreduce(&numevents,&totalevents,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);*/

  /*std::vector<std::string> names_s;

  for(int i=0;i<num_threads;i++)
	  names_s.push_back(t_args[i].name);

  t_args[0].np->pwrite_files(names_s);*/

  /*MPI_Barrier(MPI_COMM_WORLD);

  t2 = std::chrono::high_resolution_clock::now();

  t = std::chrono::duration<double>(t2-t1).count();

  double total_w_time=0;
  MPI_Allreduce(&t,&total_w_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" total_write_time = "<<total_w_time<<std::endl;
*/
  //io_polling(&t_args[2]);

  /*t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	std::thread t{close_write_stream,&t_args[i]};
	workers[i] = std::move(t);
	workers[i].join();
        MPI_Barrier(MPI_COMM_WORLD);
  }
  
  t2 = std::chrono::high_resolution_clock::now();

  t = std::chrono::duration<double>(t2-t1).count();
  total_time = 0;
  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  if(rank==0) std::cout <<" writing time = "<<total_time<<std::endl;
*/
 /* std::string fname = "file"+t_args[0].name+".h5";
  np->read_events(fname.c_str(),t_args[0].name);*/

  /*np->clear_events(name);*/

  MPI_Barrier(MPI_COMM_WORLD);

  delete np;
  MPI_Finalize();

}
