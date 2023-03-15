#include "process.h"
#include <cassert>
#include <thallium.hpp>
#include "distributed_map.h"

namespace tl=thallium;


void sum(const tl::request& req, int x, int y) {
    std::cout << "Computing " << x << "+" << y << std::endl;
    req.respond(x+y);
}

int main(int argc,char **argv)
{

  int p;

  MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&p);

  assert(p == MPI_THREAD_MULTIPLE);

  int size,rank;
  MPI_Comm_size(MPI_COMM_WORLD,&size);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);

  read_write_process *rw = new read_write_process(rank,size); 

  rw->synchronize();

  rw->create_events(128);
  //const char *filename = "file1.h5";
  //rw->pwrite(filename);

  //rw->pread();

  MPI_Barrier(MPI_COMM_WORLD);

  /*std::vector<struct event> myevents = rw->get_events();

  std::cout <<" num_events = "<<myevents.size()<<std::endl;*/

  MPI_Barrier(MPI_COMM_WORLD);
  delete rw;
  MPI_Finalize();

}
