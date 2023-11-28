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

   std::string sname = "table2";
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
   lens.push_back(400);
   int len = sizeof(int)*2+400;
   KeyValueStoreMetadata m(sname,n,types,names,lens,len);

   int tdw = 200000;
   int td = tdw/size;

   int nloops = 4;
   int nticks = 50;
   int ifreq = 100;
   /*nticks = freq for backup to nvme
    * nloops*nticks = freq for backup to disk*/
     /*ifreq is frequency for index backups*/
   auto t1 = std::chrono::high_resolution_clock::now();
    
   int s1 = k->start_session(sname,names[0],m,32768,nloops,nticks,ifreq);

   auto t1_v = std::chrono::high_resolution_clock::now();

   int numgets = k->create_keyvalues<integer_invlist,int>(s1,td,400000);

   auto t2_v = std::chrono::high_resolution_clock::now();
   double t_v = std::chrono::duration<double> (t2_v-t1_v).count();

   k->close_sessions();

   delete k;
   MPI_Barrier(MPI_COMM_WORLD);

   auto t2 = std::chrono::high_resolution_clock::now();

   double t = std::chrono::duration<double>(t2-t1).count();

   double total_time = 0;
   MPI_Allreduce(&t,&total_time,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
   double total_time_v = 0;
   MPI_Allreduce(&t_v,&total_time_v,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);
   int total_gets = 0;
   MPI_Allreduce(&numgets,&total_gets,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
   if(rank==0) 
   {
	std::cout <<" num-gets = "<<total_gets<<std::endl;
	std::cout <<" num put-gets = "<<tdw<<std::endl;
	std::cout <<" total_time put = "<<total_time_v<<std::endl;
	std::cout <<" total time = "<<total_time<<std::endl;
   }
   MPI_Finalize();

}
