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

   std::vector<int> pubs,subs;

   for(int i=0;i<size;i++)
   {
	if(i%2==0) pubs.push_back(i);
	else subs.push_back(i);
   }

   p->create_pub_sub_service(sname,pubs,subs);
   int datasize = m.value_size();
   int recordlen = sizeof(uint64_t)+datasize;
   p->add_message_cache(sname,100,recordlen);

   KeyValueStore *k = p->getkvs();

   int s1 = k->start_session(sname,names[0],m,32768);








  delete p;


  MPI_Finalize();

}
