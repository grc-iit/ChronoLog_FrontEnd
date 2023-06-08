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

  int numstories = 4;
  std::vector<std::string> story_names;
  std::vector<int> total_events;

  int numattrs = (int)VALUESIZE/sizeof(double);

  event_metadata em;
  em.set_numattrs(numattrs);
  for(int i=0;i<numattrs;i++)
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
	total_events.push_back(2048);
	np->prepare_service(name,em,2048);
  }


  MPI_Barrier(MPI_COMM_WORLD);

  int num_writer_threads = 4;

  int nbatches = 16;

  t1 = std::chrono::high_resolution_clock::now();

  np->spawn_post_processing();

  MPI_Barrier(MPI_COMM_WORLD);

  np->data_streams_s(story_names,total_events,nbatches);

  np->generate_queries(story_names);

  np->end_sessions();

  t2 = std::chrono::high_resolution_clock::now();
  t = std::chrono::duration<double> (t2-t1).count();

  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" Total time = "<<total_time<<std::endl;

  delete np;
  MPI_Finalize();

}
