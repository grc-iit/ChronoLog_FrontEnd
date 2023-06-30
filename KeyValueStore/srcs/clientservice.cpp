#include <iostream>
#include "KeyValueStore.h"


int main(int argc,char **argv)
{
   int prov;

   MPI_Init_thread(&argc,&argv,MPI_THREAD_SINGLE,&prov);

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

   k->create_keyvalues(sname,names[0],100000);


   MPI_Barrier(MPI_COMM_WORLD);
   delete k;

   MPI_Finalize();



}
