#include "process.h"
#include <cassert>
#include <thallium.hpp>
#include <chrono>
#include <sched.h>
#include <thread>
#include "rw_request.h"
#include "event_metadata.h"

namespace tl=thallium;


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

  int numstories = 1;
  std::vector<std::string> story_names;
  std::vector<int> total_events;

  event_metadata em;
  em.set_numattrs(5);
  for(int i=0;i<5;i++)
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
	total_events.push_back(65536);
	np->prepare_service(name,em);
  }


  MPI_Barrier(MPI_COMM_WORLD);

  int num_threads = 1;

  t1 = std::chrono::high_resolution_clock::now();

  std::vector<struct thread_arg> t_args(num_threads);
  std::vector<std::thread> workers(num_threads);

  for(int i=0;i<num_threads;i++)
  {
      int events_per_proc = total_events[i]/size;
      int rem = total_events[i]%size;
      if(rank < rem) events_per_proc++;
      t_args[i].tid = i; 
      t_args[i].np = np->get_rw_object();
      t_args[i].q = np->get_query_parser_obj();
      t_args[i].num_events = events_per_proc;
      t_args[i].name = story_names[i];
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

  
  t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	std::thread t{open_write_stream,&t_args[i]};
	workers[i] = std::move(t);
	workers[i].join();
  }

  t2 = std::chrono::high_resolution_clock::now();
  t = std::chrono::duration<double>(t2-t1).count();
  total_time = 0;
  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  if(rank==0) std::cout <<" writing time = "<<total_time<<std::endl;

 /* std::string fname = "file"+t_args[0].name+".h5";
  np->read_events(fname.c_str(),t_args[0].name);*/

  /*np->clear_events(name);*/

  MPI_Barrier(MPI_COMM_WORLD);

  delete np;
  MPI_Finalize();

}
