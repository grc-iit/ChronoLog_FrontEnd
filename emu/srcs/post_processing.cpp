#include "post_processing.h"



int main(int argc,char **argv)
{
   int numprocs,rank;

   int prov;

   MPI_Comm parent;

   MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&prov);

   MPI_Comm_get_parent(&parent);

   int psize = 0;
   MPI_Comm_remote_size(parent,&psize);


   MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
   MPI_Comm_rank(MPI_COMM_WORLD,&rank);

   file_post_processing *fp = new file_post_processing(numprocs,rank);
  
   fp->sort_on_secondary_key(); 

   delete fp;

   MPI_Finalize();

}
