#ifndef __KeyValueStore_H_
#define __KeyValueStore_H_

#include "KeyValueStoreMetadata.h"
#include "KeyValueStoreAccessor.h"
#include "KeyValueStoreMDS.h"
#include "KeyValueStoreIO.h"
#include "data_server_client.h"
#include "util.h"
#include <hdf5.h>
#include "h5_async_lib.h"

class KeyValueStore
{
    private:
	    int numprocs;
	    int myrank;
	    KeyValueStoreMDS *mds;
	    data_server_client *ds; 
	    KeyValueStoreIO *io_layer;
	    BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *accessor_maps;
	    memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *t_pool; 
    public:
	    KeyValueStore(int np,int r) : numprocs(np), myrank(r)
	   {
		H5open();
		int base_port = 2000;
		ds = new data_server_client(numprocs,myrank,base_port);
		io_layer = new KeyValueStoreIO(numprocs,myrank);
		mds = new KeyValueStoreMDS(numprocs,myrank);
		tl::engine *t_server = ds->get_thallium_server();
           	tl::engine *t_server_shm = ds->get_thallium_shm_server();
           	tl::engine *t_client = ds->get_thallium_client();
           	tl::engine *t_client_shm = ds->get_thallium_shm_client();
           	std::vector<tl::endpoint> server_addrs = ds->get_serveraddrs();
           	std::vector<std::string> ipaddrs = ds->get_ipaddrs();
           	std::vector<std::string> shmaddrs = ds->get_shm_addrs();
           	mds->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ipaddrs,shmaddrs,server_addrs);
		mds->bind_functions();
		t_pool = new memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> (100);
		std::string emptyKey = "";
		accessor_maps = new BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal>(128,t_pool,emptyKey);
		
		MPI_Barrier(MPI_COMM_WORLD);
	   }
	   void createKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &m);
	   void findKeyValueStoreEntry(std::string &);
	   void removeKeyValueStoreEntry(std::string &s);
	   void addKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   bool findKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   void removeKeyValueStoreInvList(std::string &s,std::string &attr_name);

	   void KeyValueStoreT()
	   {
		std::string name = "table0";
   		int num_attributes = 4;
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
		KeyValueStoreMetadata *k = new KeyValueStoreMetadata(name,num_attributes,types,names,lens,len);
		std::vector<std::string> ks;
	       	k->packmetadata(ks);
		mds->Insert(name,ks);
		delete k;
		MPI_Barrier(MPI_COMM_WORLD);
		if(myrank==0)
		{
		  std::vector<std::string> ns  = mds->Get(name);
		}	
		MPI_Barrier(MPI_COMM_WORLD);
	   }

	   ~KeyValueStore()
	   {

		H5close();
		delete t_pool;
		delete accessor_maps;
		delete mds;
		delete io_layer;
		delete ds;

	   }




};



#endif
