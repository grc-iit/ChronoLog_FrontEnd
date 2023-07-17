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

   std::string sname = "table0";
   int n = 4;
   std::vector<std::string> types;
   types.push_back("int");
   types.push_back("int");
   types.push_back("int");
   types.push_back("int");
   std::vector<std::string> names;
   names.push_back("name1");
   names.push_back("name2");
   names.push_back("name3");
   names.push_back("name4"); 
   std::vector<int> lens;
   lens.push_back(sizeof(int));
   lens.push_back(sizeof(int));
   lens.push_back(sizeof(int));
   lens.push_back(sizeof(int));
   int len = names.size()*sizeof(int);
   KeyValueStoreMetadata m(sname,n,types,names,lens,len);

   k->createKeyValueStoreEntry(sname,m);

   k->addKeyValueStoreInvList(sname,names[0]);  
   k->addKeyValueStoreInvList(sname,names[1]);
   k->addKeyValueStoreInvList(sname,names[2]);
   k->addKeyValueStoreInvList(sname,names[3]);

   std::vector<int> keys1;
   std::vector<uint64_t> ts1;

   k->get_testworkload(keys1,ts1,0);

   std::vector<int> keys2;
   std::vector<uint64_t> ts2;
   k->get_testworkload(keys2,ts2,4);

   std::vector<int> keys3;
   std::vector<uint64_t> ts3;
   k->get_testworkload(keys3,ts3,8);

   std::vector<int> keys4;
   std::vector<uint64_t> ts4;
   k->get_testworkload(keys4,ts4,12);

   k->spawn_kvstream<integer_invlist,int>(sname,names[0],keys1,ts1);
   k->spawn_kvstream<integer_invlist,int>(sname,names[1],keys2,ts2);
   k->spawn_kvstream<integer_invlist,int>(sname,names[2],keys3,ts3);
   k->spawn_kvstream<integer_invlist,int>(sname,names[3],keys4,ts4);

   k->end_io_session();


   MPI_Barrier(MPI_COMM_WORLD);

   delete k;

   MPI_Finalize();



}
