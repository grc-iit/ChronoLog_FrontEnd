#include "KeyValueStore.h"



void KeyValueStore::createKeyValueStoreEntry(std::string &s, KeyValueStoreMetadata &m)
{
   std::vector<std::string> pk;
   m.packmetadata(pk);
   bool b = mds->Insert(s,pk);

   KeyValueStoreAccessor *ka = new KeyValueStoreAccessor(numprocs,myrank,m,io_layer);
   if(accessor_maps->insert(s,ka)!=INSERTED) delete ka;
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

void KeyValueStore::addKeyValueStoreInvList(std::string &s,std::string &attr_name)
{
    int pos = accessor_maps->find(s);
    if(pos==NOT_IN_TABLE) 
    {
	KeyValueStoreMetadata m;
	std::vector<std::string> it = mds->Get(s);
	if(it.size()>0)
	{
	  m.unpackmetadata(it);
	  KeyValueStoreAccessor *ka = new KeyValueStoreAccessor(numprocs,myrank,m,io_layer);
	  if(accessor_maps->insert(s,ka)!=INSERTED) delete ka;
	}
	else return;
    }

    KeyValueStoreAccessor *ka;
    bool b = accessor_maps->get(s,&ka);

    if(b)
    {
	
	KeyValueStoreMetadata md = ka->get_metadata();


    }


}


bool KeyValueStore::findKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}

void KeyValueStore::removeKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}
