#include "KeyValueStore.h"



void KeyValueStore::createKeyValueStoreEntry(std::string &s, KeyValueStoreMetadata &m)
{

	struct keyvaluestoremetadata k;
	 k.name = m.db_name();
         k.num_attributes = m.num_attributes();
	 std::vector<std::string> names = m.attribute_names();
         k.attribute_names.assign(names.begin(),names.end());
	 std::vector<std::string> types = m.attribute_types();
         k.attribute_types.assign(types.begin(),types.end());
	 std::vector<int> lengths = m.attribute_lengths();
         k.attribute_lengths.assign(lengths.begin(),lengths.end());
         k.value_size = m.value_size();

	 if(myrank==1)
	 {
           int i = 0;
	   mds->Insert(s,i);
	 }
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
