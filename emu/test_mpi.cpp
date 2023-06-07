#include <mpi.h>
#include <iostream>
#include <vector>
#include <cmath>
#include <cfloat>
#include <climits>
#include <thread>


struct thread_arg
{
   int tid;
   MPI_Comm *comm;
   int rank;
   int numprocs;
};

void mpi_functions(struct thread_arg *t)
{

  int sreq = t->tid;
  int recv_req = 0;


  MPI_Allreduce(&sreq,&recv_req,1,MPI_INT,MPI_SUM,*(t->comm));

  MPI_Barrier(*(t->comm));

  if(t->rank==0) std::cout <<" id = "<<t->tid<<"sum = "<<recv_req<<std::endl;

}

int main(int argc,char **argv)
{



	int req;
	MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&req);

	int rank,numprocs;

	MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);

	MPI_Comm comms[8];

	for(int i=0;i<8;i++)
	{
	   MPI_Comm_dup(MPI_COMM_WORLD,&comms[i]);
	}

	int num_threads = 8;

	std::vector<struct thread_arg> t_args(num_threads);
	std::vector<std::thread> workers(num_threads);

	for(int i=0;i<num_threads;i++)
	{
		t_args[i].tid = i;
		t_args[i].comm = &(comms[i]);
		t_args[i].rank = rank;
		t_args[i].numprocs = numprocs;
	}

	for(int i=0;i<num_threads;i++)
	{
	   std::thread t{mpi_functions,&t_args[i]};
	   workers[i] = std::move(t);
	}

	for(int i=0;i<num_threads;i++)
		workers[i].join();

	for(int i=0;i<8;i++)
		MPI_Comm_free(&comms[i]);
	MPI_Finalize();

}
