#ifndef __KeyValueStore_H_
#define __KeyValueStore_H_

#include "KeyValueStoreMetadata.h"
#include "KeyValueStoreAccessor.h"
#include "KeyValueStoreMDS.h"
#include "data_server_client.h"
#include "stringfunctions.h"
#include <hdf5.h>
#include "h5_async_lib.h"

class KeyValueStore
{
    private:
	    int numprocs;
	    int myrank;
	    KeyValueStoreMDS *mds;
	    data_server_client *ds; 
	    BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *accessor_maps;
	    memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *t_pool; 
    public:
	    KeyValueStore(int np,int r) : numprocs(np), myrank(r)
	   {
		H5open();
		int base_port = 1000;
		ds = new data_server_client(numprocs,myrank,base_port);
		mds = new KeyValueStoreMDS(numprocs,myrank);
		t_pool = new memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> (100);
		std::string emptyKey = "";
		accessor_maps = new BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal>(128,t_pool,emptyKey);

	   }
	   void createKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &m);
	   void findKeyValueStoreEntry(std::string &);
	   void removeKeyValueStoreEntry(std::string &s);
	   void addKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   bool findKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   void removeKeyValueStoreInvList(std::string &s,std::string &attr_name);
	     

	   ~KeyValueStore()
	   {

		H5close();
		delete t_pool;
		delete accessor_maps;
		delete mds;
		delete ds;

	   }




};



#endif
