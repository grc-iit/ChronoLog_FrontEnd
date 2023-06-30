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

    if(pos==-1) tables->create_invertedlist(s,attr_name);

    pos = ka->get_inverted_list_index(attr_name);

    std::vector<int> keys;

    srandom(myrank);
    for(int i=0;i<numreq;i++)
    {
	int key = (int)random()%RAND_MAX;

	uint64_t ts = std::chrono::high_resolution_clock::now().time_since_epoch().count();

	//usleep(20000);

	ka->insert_entry<integer_invlist,int>(pos,key,ts);
	keys.push_back(key);
    }
    for(int i=0;i<keys.size();i++)
    {
	std::vector<uint64_t> values = ka->get_entry<integer_invlist,int>(pos,keys[i]);	
    }

    MPI_Barrier(MPI_COMM_WORLD);

    ka->flush_invertedlist<integer_invlist>(attr_name);
  

}

void KeyValueStore::addKeyValueStoreInvList(std::string &s,std::string &attr_name)
{
      if(!tables->find_accessor(s))
      {
	   KeyValueStoreMetadata m;
	   if(!findKeyValueStoreEntry(s,m)) return;

	   if(!tables->add_accessor(s,m)) return;
      }

      tables->create_invertedlist(s,attr_name);
}


bool KeyValueStore::findKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}

void KeyValueStore::removeKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}
