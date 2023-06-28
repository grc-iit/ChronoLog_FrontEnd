#ifndef __INVERTED_LIST_H_
#define __INVERTED_LIST_H_

#include "blockmap.h"
#include <mpi.h>
#include <hdf5.h>
#include "h5_async_lib.h"
#include "event.h"
#include "data_server_client.h"

namespace tl=thallium;

template<class KeyT,class ValueT, class HashFcn=std::hash<KeyT>,class EqualFcn=std::equal_to<KeyT>>
struct invnode
{
   BlockMap<KeyT,ValueT,HashFcn,EqualFcn> *bm;
   memory_pool<KeyT,ValueT,HashFcn,EqualFcn> *ml; 
};

template <class KeyT>
struct KeyIndex
{
  KeyT key;
  uint64_t index;
};

template<class KeyT>
bool compareIndex(struct KeyIndex<KeyT> &k1, struct KeyIndex<KeyT> &k2)
{
    if(k1.index < k2.index) return true;
    else return false;
}

template<class KeyT,class ValueT,class hashfcn=std::hash<KeyT>,class equalfcn=std::equal_to<KeyT>>
class hdf5_invlist
{

   private:
	   int numprocs;
	   int myrank;
	   struct invnode<KeyT,ValueT,hashfcn,equalfcn>* invlist;
	   int tag;
	   int maxsize;
	   KeyT emptyKey;
	   hid_t kv1;
	   std::string rpc_prefix;
	   data_server_client *d;
	   tl::engine *thallium_server;
           tl::engine *thallium_shm_server;
           tl::engine *thallium_client;
           tl::engine *thallium_shm_client;
           std::vector<tl::endpoint> serveraddrs;
           std::vector<std::string> ipaddrs;
           std::vector<std::string> shmaddrs;
           std::string myipaddr;
           std::string myhostname;
           int nservers;
           int serverid;

   public:
	   hdf5_invlist(int n,int p,int size,KeyT emptykey,std::string &pre,data_server_client *ds) : numprocs(n), myrank(p)
	   {
	     tag = 20000;
	     maxsize = size;
	     emptyKey = emptykey;
	     rpc_prefix = pre;
	     d = ds;
	     tl::engine *t_server = d->get_thallium_server();
             tl::engine *t_server_shm = d->get_thallium_shm_server();
             tl::engine *t_client = d->get_thallium_client();
             tl::engine *t_client_shm = d->get_thallium_shm_client();
             std::vector<tl::endpoint> saddrs = d->get_serveraddrs();
             std::vector<std::string> ips = d->get_ipaddrs();
             std::vector<std::string> shm_addrs = d->get_shm_addrs();
	     nservers = numprocs;
	     serverid = myrank;
	     thallium_server = t_server;
             thallium_shm_server = t_server_shm;
             thallium_client = t_client;
             thallium_shm_client = t_client_shm;
             ipaddrs.assign(ips.begin(),ips.end());
             shmaddrs.assign(shm_addrs.begin(),shm_addrs.end());
             myipaddr = ipaddrs[serverid];
             serveraddrs.assign(saddrs.begin(),saddrs.end());
	     invlist = new struct invnode<KeyT,ValueT,hashfcn,equalfcn> ();
	     invlist->ml = new memory_pool<KeyT,ValueT,hashfcn,equalfcn> (100);
	     invlist->bm = new BlockMap<KeyT,ValueT,hashfcn,equalfcn>(size,invlist->ml,emptykey);
	     kv1 = H5Tcreate(H5T_COMPOUND,sizeof(struct KeyIndex<KeyT>));
    	     H5Tinsert(kv1,"key",HOFFSET(struct KeyIndex<KeyT>,key),H5T_NATIVE_INT);
    	     H5Tinsert(kv1,"index",HOFFSET(struct KeyIndex<KeyT>,index),H5T_NATIVE_UINT64);
	   }

	   void bind_functions()
	   {



		MPI_Barrier(MPI_COMM_WORLD);
	   }

	   ~hdf5_invlist()
	   {
	        if(invlist != nullptr) 
	        {
	          delete invlist->bm;
		  delete invlist->ml;
		  delete invlist;
	         }
		H5Tclose(kv1);

	   }

	   inline int nearest_power_two(int n)
	   {
		int c = 1;

		while(c < n)
		{
		   c = 2*c;
		}
		return c;
	   }

	   bool put_entry(KeyT&,uint64_t&);
	   bool get_entry(KeyT&,std::vector<uint64_t>&values);
	   void fill_invlist_from_file(std::string&,int);
	   void flush_table_file(std::string &,int);
	   int partition_no(KeyT &k);		  
	   void add_entries_to_tables(std::string&,std::vector<struct event>*,uint64_t,int); 
	   void get_entries_from_tables(std::string &,std::vector<std::vector<KeyT>> *,std::vector<std::vector<ValueT>>*,int&,int&);
};

#include "../srcs/invertedlist.cpp"

#endif
