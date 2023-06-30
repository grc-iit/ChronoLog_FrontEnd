#ifndef __KeyValueStore_H_
#define __KeyValueStore_H_

#include "KeyValueStoreMetadata.h"
#include "KeyValueStoreAccessorRepository.h"
#include "KeyValueStoreMDS.h"
#include "KeyValueStoreIO.h"
#include "data_server_client.h"
#include "util_t.h"
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
	    KeyValueStoreAccessorRepository *tables;
    public:
	    KeyValueStore(int np,int r) : numprocs(np), myrank(r)
	   {
		H5open();
		int base_port = 2000;
		ds = new data_server_client(numprocs,myrank,base_port);
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
		io_layer = new KeyValueStoreIO(numprocs,myrank);
		io_layer->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ipaddrs,shmaddrs,server_addrs);
		io_layer->bind_functions();
		tables = new KeyValueStoreAccessorRepository(numprocs,myrank,io_layer,ds);
		MPI_Barrier(MPI_COMM_WORLD);
	   }
	   void createKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &);
	   bool findKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &);
	   void removeKeyValueStoreEntry(std::string &s);
	   void addKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   bool findKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   void removeKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   void create_keyvalues(std::string &,std::string &,int);

	   ~KeyValueStore()
	   {

		io_layer->end_io();
		delete tables;
		delete io_layer;
		delete mds;
		delete ds;
		H5close();

	   }




};



#endif
