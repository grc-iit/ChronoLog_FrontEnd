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

   std::string sname = "loadb";
   int n = 2;
   std::vector<std::string> types;
   std::vector<std::string> names;
   std::vector<int> lens;
   types.push_back("unsignedlong");
   names.push_back("value1");
   lens.push_back(sizeof(uint64_t));
   types.push_back("char");
   names.push_back("value2");
   lens.push_back(sizeof(char)*1000);
   int len = sizeof(uint64_t)+1000*sizeof(char);
   KeyValueStoreMetadata m(sname,n,types,names,lens,len);
   
   std::vector<uint64_t> keys;
   std::vector<std::string> values;
   std::vector<int> op;
   std::string filename = sname+".log"; 
   k->get_ycsb_test(filename,keys,values,op);

   int s = k->start_session(sname,names[0],m,32768);

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

   sname = "loadbrun";

   filename = sname + ".log";
   keys.clear();
   values.clear();
   op.clear();
   k->get_ycsb_test(filename,keys,values,op);

   k->create_keyvalues_ordered<unsigned_long_invlist,uint64_t>(s,keys,values,op,20000);
  
   delete reqs;

   k->close_sessions();

   delete k;
   MPI_Finalize();

}
