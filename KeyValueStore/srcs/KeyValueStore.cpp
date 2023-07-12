#include "invertedlist.h"
#include "KeyValueStore.h"



void KeyValueStore::createKeyValueStoreEntry(std::string &s, KeyValueStoreMetadata &m)
{
   std::vector<std::string> pk;
   m.packmetadata(pk);
   bool b = mds->Insert(s,pk);

   tables->add_accessor(s,m);
}

bool KeyValueStore::findKeyValueStoreEntry(std::string &s,KeyValueStoreMetadata &m)
{
   bool ret = false;
   std::vector<std::string> it;
   it = mds->Get(s);
   if(it.size()>0)
   {
	m.unpackmetadata(it);
   }
   return ret;
}

void KeyValueStore::create_keyvalues(std::string &s,std::string &attr_name,int numreq)
{
    KeyValueStoreAccessor* ka = tables->get_accessor(s);

    if(ka==nullptr)
    {
	KeyValueStoreMetadata m;
	if(!findKeyValueStoreEntry(s,m)) return;
	if(!tables->add_accessor(s,m)) return;
	ka = tables->get_accessor(s);
    }
    int pos = ka->get_inverted_list_index(attr_name);

    
    if(pos==-1) 
    {
      tables->create_invertedlist(s,attr_name,io_count);
      io_count++;
    }

    pos = ka->get_inverted_list_index(attr_name);

    std::vector<int> keys;
    std::vector<uint64_t> ts;
    create_integertestinput(numprocs,myrank,0,keys,ts);

    ka->cache_invertedtable<integer_invlist>(attr_name);
    /*srandom(myrank);*/

    auto t1 = std::chrono::high_resolution_clock::now();

    for(int i=0;i<keys.size();i++)
    {
	//int key = (int)random()%RAND_MAX;

	 int key = keys[i];
	 uint64_t ts_k = ts[i];

	//uint64_t ts = std::chrono::high_resolution_clock::now().time_since_epoch().count();

	//usleep(20000);

	ka->insert_entry<integer_invlist,int>(pos,key,ts_k);
    }

    auto t2 = std::chrono::high_resolution_clock::now();

    double time1 = std::chrono::duration<double>(t2-t1).count();

    double max_time1 = 0;

    //MPI_Allreduce(&time1,&max_time1,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

    t1 = std::chrono::high_resolution_clock::now();

    for(int i=0;i<keys.size();i++)
    {

	std::vector<uint64_t> values = ka->get_entry<integer_invlist,int>(pos,keys[i]);
    }

    t2 = std::chrono::high_resolution_clock::now();

    double time2 = std::chrono::duration<double>(t2-t1).count();

    double max_time2 = 0;

    //MPI_Allreduce(&time2,&max_time2,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

    t1 = std::chrono::high_resolution_clock::now();

    ka->flush_invertedlist<integer_invlist>(attr_name);
  
    t2 = std::chrono::high_resolution_clock::now();

    double time3 = std::chrono::duration<double>(t2-t1).count();

    int total_keys = 0;
    int mykeys = keys.size();
    //MPI_Allreduce(&mykeys,&total_keys,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

    double max_time3;

    //MPI_Allreduce(&time3,&max_time3,1,MPI_DOUBLE,MPI_MAX,MPI_COMM_WORLD);

    if(myrank==0) std::cout <<" put : "<<max_time1<<" seconds"<<" get : "<<max_time2<<" seconds"<<" flush : "<<max_time3<<" seconds"<<" get throughput = "<<total_keys/max_time2<<" reqs/sec"<<std::endl;

    ka->delete_inverted_list<integer_invlist>(pos);
}

void KeyValueStore::addKeyValueStoreInvList(std::string &s,std::string &attr_name)
{
      if(!tables->find_accessor(s))
      {
	   KeyValueStoreMetadata m;
	   if(!findKeyValueStoreEntry(s,m)) return;

	   if(!tables->add_accessor(s,m)) return;
      }

      tables->create_invertedlist(s,attr_name,io_count);
      io_count++;
}


bool KeyValueStore::findKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}

void KeyValueStore::removeKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}
