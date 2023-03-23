#include "process.h"
#include <cassert>
#include <thallium.hpp>
#include <chrono>

namespace tl=thallium;


int main(int argc,char **argv)
{

  int p;

  MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&p);

  assert(p == MPI_THREAD_MULTIPLE);

  int size,rank;
  MPI_Comm_size(MPI_COMM_WORLD,&size);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

  auto t1 = std::chrono::high_resolution_clock::now();

  emu_process *np = new emu_process(size,rank);

  np->synchronize();

  auto t2 = std::chrono::high_resolution_clock::now();

  double t = std::chrono::duration<double>(t2-t1).count();

  double total_time = 0;
  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" numprocs = "<<size<<" sync time = "<<total_time<<std::endl;

  t1 = std::chrono::high_resolution_clock::now();

  int total_events = 65536;

  int events_per_proc = total_events/size;
  int rem = total_events%size;

  if(rank < rem) events_per_proc++;

  np->create_events(events_per_proc);

  MPI_Barrier(MPI_COMM_WORLD);

  const char *filename = "file1.h5";
  np->write_events(filename);

  np->read_events(filename);

  MPI_Barrier(MPI_COMM_WORLD);

  delete np;
  MPI_Finalize();

}
