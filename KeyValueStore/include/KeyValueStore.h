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
#include <thread>
#include <mutex>

template<typename N>
struct kstream_args
{
  std::string tname;
  std::string attr_name;
  std::vector<N> keys;
  std::vector<uint64_t> ts;
  int tid;
};

class KeyValueStore
{
    private:
	    int numprocs;
	    int myrank;
	    KeyValueStoreMDS *mds;
	    data_server_client *ds;
	    KeyValueStoreIO *io_layer; 
	    KeyValueStoreAccessorRepository *tables;
	    int io_count;
	    int tag;
	    std::vector<struct kstream_args<int>> k_args;
	    std::vector<std::thread> kstreams;
	    std::atomic<int> nstreams;

    public:
	    KeyValueStore(int np,int r) : numprocs(np), myrank(r)
	   {
		H5open();
   	        H5VLis_connector_registered_by_name("async");
		io_count=0;
		int base_port = 2000;
		tag = 20000;
		nstreams.store(0);
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
		MPI_Barrier(MPI_COMM_WORLD);
		io_layer = new KeyValueStoreIO(numprocs,myrank);
		io_layer->server_client_addrs(t_server,t_client,t_server_shm,t_client_shm,ipaddrs,shmaddrs,server_addrs);
		io_layer->bind_functions();
		tables = new KeyValueStoreAccessorRepository(numprocs,myrank,io_layer,ds);
		k_args.resize(MAXSTREAMS);
	        kstreams.resize(MAXSTREAMS);
	   }
	   void createKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &);
	   bool findKeyValueStoreEntry(std::string &,KeyValueStoreMetadata &);
	   void removeKeyValueStoreEntry(std::string &s);
	   void addKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   bool findKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   void removeKeyValueStoreInvList(std::string &s,std::string &attr_name);
	   template<typename T,typename N>
           void RunKeyValueStoreFunctions(KeyValueStoreAccessor* ka,struct kstream_args<N> *k)
	   {
		ka->cache_invertedtable<T>(k->attr_name);

   		int pos = ka->get_inverted_list_index(k->attr_name);
    		for(int i=0;i<k->keys.size();i++)
    		{
        		N key = k->keys[i];
        		uint64_t ts_k = k->ts[i];
        		ka->insert_entry<T,N>(pos,key,ts_k);
    		}

		 for(int i=0;i<k->keys.size();i++)
    		 {
      		   std::vector<uint64_t> values = ka->get_entry<T,N>(pos,k->keys[i]);
    		 }

		 ka->flush_invertedlist<T>(k->attr_name);
	   }
	   template<typename T,typename N>
	   void create_keyvalues(struct kstream_args<N> *k)
	   {
		std::string s = k->tname;
   		std::string attr_name = k->attr_name;
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
                  pos = ka->get_inverted_list_index(attr_name);
               }

               std::string type = ka->get_attribute_type(attr_name);

   	       RunKeyValueStoreFunctions<T,N>(ka,k);
	   }

           void get_testworkload(std::vector<int>&,std::vector<uint64_t>&);

	   template<typename T,typename N>
	   void spawn_kvstream(std::string &s,std::string &a,std::vector<N> &keys,std::vector<uint64_t> &ts)
	   {

		int prev = nstreams.load();
		k_args[prev].tname = s;
		k_args[prev].attr_name = a;
		k_args[prev].tid = prev;
		k_args[prev].keys.assign(keys.begin(),keys.end());
		k_args[prev].ts.assign(ts.begin(),ts.end());

		keys.clear(); ts.clear();

		std::function<void(struct kstream_args<N> *)> 
		KVStream(std::bind(&KeyValueStore::create_keyvalues<T,N>,this, std::placeholders::_1));

		nstreams.fetch_add(1);

	 	std::thread t{KVStream,&k_args[prev]};	
		kstreams[prev] = std::move(t);

	   }
	   void end_io_session()
	   {
		for(int i=0;i<nstreams.load();i++) kstreams[i].join();

		io_layer->end_io();

		MPI_Request *reqs = (MPI_Request *)std::malloc(2*numprocs*sizeof(MPI_Request));
	        int nreq = 0;
		int tag = 1000;
		
		int send_v = 1;
		std::vector<int> recv_v(numprocs);
		std::fill(recv_v.begin(),recv_v.end(),0);

		for(int i=0;i<numprocs;i++)
		{
		   MPI_Isend(&send_v,1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
		   nreq++;
		   MPI_Irecv(&recv_v[i],1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
		   nreq++;
		}

		MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

		std::free(reqs);
	   }
	   ~KeyValueStore()
	   {

		delete tables;
		delete io_layer;
		delete mds;
		delete ds;
		H5close();

	   }




};



#endif
