#include "PSClient.h"


int main(int argc,char **argv)
{
   int prov;

   MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&prov);
   assert (prov == MPI_THREAD_MULTIPLE);

   int size,rank;
   MPI_Comm_size(MPI_COMM_WORLD,&size);
   MPI_Comm_rank(MPI_COMM_WORLD,&rank);

   pubsubclient *p = new pubsubclient(size,rank);

   std::string sname = "table1";
   int n = 3;
   std::vector<std::string> types;
   std::vector<std::string> names;
   std::vector<int> lens;
   types.push_back("int");
   names.push_back("value1");
   lens.push_back(sizeof(int));
   types.push_back("int");
   names.push_back("value2");
   lens.push_back(sizeof(int));
   types.push_back("char");
   names.push_back("value3");
   lens.push_back(200);
   int len = sizeof(int)*2+200;
   KeyValueStoreMetadata m(sname,n,types,names,lens,len);

   int total_size = 8*65536;
   int size_per_proc = total_size/size;
   int rate = 20000;
   int pub_rate = 100000;
   bool bcast = true;

   auto t1 = std::chrono::high_resolution_clock::now();

   p->CreatePubSubWorkload(sname,names[0],m,size_per_proc,rate,pub_rate,bcast);


   delete p;

   auto t2 = std::chrono::high_resolution_clock::now();
   double time_taken = std::chrono::duration<double>(t2-t1).count();
   int tag = 12300;

   MPI_Request *reqs = new MPI_Request[2*size];
   int nreq = 0;

   std::vector<double> recvv(size);
   std::fill(recvv.begin(),recvv.end(),0);

   for(int i=0;i<size;i++)
   {
	MPI_Isend(&time_taken,1,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
	MPI_Irecv(&recvv[i],1,MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;

   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   time_taken = DBL_MIN;
   for(int i=0;i<size;i++)
	  if(recvv[i] > time_taken) time_taken = recvv[i];

   if(rank==0) std::cout <<" Time taken = "<<time_taken<<" seconds"<<std::endl;
   delete reqs;

  MPI_Finalize();

}
