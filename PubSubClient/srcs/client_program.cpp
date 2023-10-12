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

   int recordlen = sizeof(uint64_t)+len;
   p->create_pub_sub_service(sname,pubs,subs,100,recordlen);
   p->bind_functions();


   KeyValueStore *k = p->getkvs();

   int s1 = k->start_session(sname,names[0],m,32768);
   
   KeyValueStoreAccessor *ka = nullptr;
   int id = 0;
   k->get_keyvaluestorestructs(sname,names[0],ka,id);
   int datasize = m.value_size();
   std::string data;
   data.resize(datasize);
   int key = random()%RAND_MAX;
   char *key_c = (char *)(&key);
   for(int j=0;j<sizeof(int);j++)
      data[j] = key_c[j];
   for(int j=0;j<datasize-sizeof(int);j++)
       data[sizeof(int)+j] = 0;

    std::string st = sname;

    uint64_t ts = ka->Put_ts<integer_invlist,int,std::string>(id,st,key,data);
    char *ts_c = (char*)(&ts);
    std::string msg;
    msg.resize(datasize+sizeof(uint64_t));
    for(int i=0;i<sizeof(uint64_t);i++)
      msg[i] = ts_c[i];

    for(int i=0;i<datasize;i++)
	msg[i+sizeof(uint64_t)] = data[i];


    if(rank==0)
    bool b = p->publish_message(st,msg); 


  delete p;


  MPI_Finalize();

}
