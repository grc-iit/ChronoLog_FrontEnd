#include "process.h"
#include <cassert>
#include <thallium.hpp>
#include "distributed_map.h"
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

  read_write_process *rw = new read_write_process(rank,size); 

  rw->synchronize();

  auto t2 = std::chrono::high_resolution_clock::now();

  double t = std::chrono::duration<double>(t2-t1).count();

  double total_time = 0;
  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" numprocs = "<<size<<" sync time = "<<total_time<<std::endl;

  t1 = std::chrono::high_resolution_clock::now();

  int total_events = 65536*2*2*2;

  int events_per_proc = total_events/size;
  int rem = total_events%size;

  if(rank < rem) events_per_proc++;

  rw->create_events(events_per_proc);

  MPI_Barrier(MPI_COMM_WORLD);

  int de = rw->dropped_events();
  rw->sort_events();

  t2 = std::chrono::high_resolution_clock::now();

  t = std::chrono::duration<double> (t2-t1).count();

  total_time = 0;

  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
  int total_de = 0;
  MPI_Allreduce(&de,&total_de,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

  int num_events = rw->num_events();

  int num_events_t = 0;
  MPI_Allreduce(&num_events,&num_events_t,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" total_events = "<<total_events<<" num_events = "<<num_events_t<<std::endl;
  if(rank==0) std::cout <<" total_order_time = "<<total_time<<std::endl;
  if(rank==0) std::cout <<" dropped events = "<<total_de<<std::endl;

  t1 = std::chrono::high_resolution_clock::now();

  const char *filename = "file1.h5";
  rw->pwrite(filename);

  t2 = std::chrono::high_resolution_clock::now();

  t = std::chrono::duration<double> (t2-t1).count();

  MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

  if(rank==0) std::cout <<" hdf5 parallel write time = "<<total_time<<std::endl;

  rw->pread(filename);

  MPI_Barrier(MPI_COMM_WORLD);

  delete rw;
  MPI_Finalize();

}
