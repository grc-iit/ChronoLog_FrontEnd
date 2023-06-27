#include "KeyValueStore.h"



void KeyValueStore::createKeyValueStoreEntry(std::string &s, KeyValueStoreMetadata &m)
{
   std::vector<std::string> pk;
   m.packmetadata(pk);
   bool b = mds->Insert(s,pk);
}

void KeyValueStore::findKeyValueStoreEntry(std::string &s)
{








}

void KeyValueStore::removeKeyValueStoreEntry(std::string &s)
{








}

void KeyValueStore::addKeyValueStoreInvList(std::string &s,std::string &attr_name)
{






}


bool KeyValueStore::findKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}

void KeyValueStore::removeKeyValueStoreInvList(std::string &s,std::string &attr_name)
{





}
