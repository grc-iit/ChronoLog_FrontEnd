#ifndef __INVERTED_LIST_H_
#define __INVERTED_LIST_H_

#include "rw.h"

class hdf5_invlist
{

   private:
	   int numprocs;
	   int myrank;
	   MPI_Comm inv_comm; 
	


   public:
	   hdf5_invlist(int n,int p) : numprocs(n), myrank(p)
	   {
		MPI_Comm_dup(MPI_COMM_WORLD,&inv_comm);

	   }

	   ~hdf5_invlist()
	   {
		MPI_Comm_free(&inv_comm);
	   }



};

#endif
