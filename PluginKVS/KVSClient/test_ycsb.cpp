#include <iostream>
#include "KeyValueStore.h"


int main(int argc,char **argv)
{

  int prov;

   MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&prov);

   int size,rank;

   MPI_Comm_size(MPI_COMM_WORLD,&size);
   MPI_Comm_rank(MPI_COMM_WORLD,&rank);

   KeyValueStore *k = new KeyValueStore(size,rank);

   std::string sname = "loada";
   int n = 2;
   std::vector<std::string> types;
   std::vector<std::string> names;
   std::vector<int> lens;
   types.push_back("unsignedlong");
   names.push_back("value1");
   lens.push_back(sizeof(uint64_t));
   types.push_back("char");
   names.push_back("value2");
   lens.push_back(sizeof(char)*200);
   int len = sizeof(uint64_t)+200*sizeof(char);
   KeyValueStoreMetadata m(sname,n,types,names,lens,len);
   
   std::vector<uint64_t> keys;
   std::vector<std::string> values;
   std::vector<int> op;
   std::string filename = sname+".log"; 
   k->get_ycsb_test(filename,keys,values,op);

   std::vector<uint64_t> keys_n;
   std::vector<std::string> values_n;
   std::vector<int> op_n;

   auto t1 = std::chrono::high_resolution_clock::now();

   int nloops = 1;
   int nticks = 50;
   int ifreq = 200;
   int s = k->start_session(sname,names[0],m,32768,nloops,nticks,ifreq);

   k->create_keyvalues<unsigned_long_invlist,uint64_t>(s,keys,values,op,20000);

   MPI_Request *reqs = new MPI_Request[2*size];
   int nreq = 0;

   int send_v = 1;
   std::vector<int> recvv(size);
   std::fill(recvv.begin(),recvv.end(),0);

   for(int i=0;i<size;i++)
   {
	MPI_Isend(&send_v,1,MPI_INT,i,12345,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
	MPI_Irecv(&recvv[i],1,MPI_INT,i,12345,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }
   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   sname = "loadrun";

   filename = sname + ".log";
   keys.clear();
   values.clear();
   op.clear();
   k->get_ycsb_test(filename,keys,values,op);

   k->create_keyvalues<unsigned_long_invlist,uint64_t>(s,keys,values,op,20000);

   delete reqs;

   k->close_sessions();

   auto t2 = std::chrono::high_resolution_clock::now();

   double t_t = std::chrono::duration<double> (t2-t1).count();

   delete k;

   double time_taken = 0;
   MPI_Allreduce(&t_t,&time_taken,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
   if(rank == 0) std::cout <<" Time taken = "<<time_taken<<" number of puts-gets = "<<keys.size()<<std::endl;

   MPI_Finalize();

}
