#ifndef __INVERTED_LIST_H_
#define __INVERTED_LIST_H_

#include "blockmap.h"
#include <mpi.h>
#include <hdf5.h>
#include "h5_async_lib.h"
#include "event.h"
#include "data_server_client.h"
#include "KeyValueStoreIO.h"
#include "util_t.h"

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
	   int totalsize;
	   int maxsize;
	   KeyT emptyKey;
	   hid_t kv1;
	   std::string filename;
	   std::string attributename;
	   std::string rpc_prefix;
	   bool file_exists;
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
	   KeyValueStoreIO *io_t;
           int nbits;
	   int nbits_p;
   public:
	   hdf5_invlist(int n,int p,int tsize,KeyT emptykey,std::string &table,std::string &attr,data_server_client *ds,KeyValueStoreIO *io) : numprocs(n), myrank(p)
	   {
	     tag = 20000;
	     totalsize = tsize;
	     int size = nearest_power_two(totalsize);
	     nbits = log2(size);
	     int nprocs = nearest_power_two(numprocs);
	     nbits_p = log2(nprocs);
	     int nbits_r = nbits-nbits_p; 
	     maxsize = pow(2,nbits_r);
	     if(myrank==0) std::cout <<" nbits = "<<nbits<<" nbits_p = "<<nbits_p<<" maxsize = "<<maxsize<<std::endl;
	     emptyKey = emptykey;
	     filename = table;
	     attributename = attr;
	     rpc_prefix = filename+attributename;
	     d = ds;
	     io_t = io;
	     file_exists = true;
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
	       std::function<void(const tl::request &,KeyT &,ValueT&)> putEntryFunc(
               std::bind(&hdf5_invlist<KeyT,ValueT,hashfcn,equalfcn>::ThalliumLocalPutEntry,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

	       std::function<void(const tl::request &,KeyT&)> getEntryFunc(
	       std::bind(&hdf5_invlist<KeyT,ValueT,hashfcn,equalfcn>::ThalliumLocalGetEntry,this,std::placeholders::_1,std::placeholders::_2));		       

	       std::string fcnname1 = rpc_prefix+"RemotePutEntry";
               thallium_server->define(fcnname1.c_str(),putEntryFunc);
               thallium_shm_server->define(fcnname1.c_str(),putEntryFunc);

	       std::string fcnname2 = rpc_prefix+"RemoteGetEntry";
	       thallium_server->define(fcnname2.c_str(),getEntryFunc);
	       thallium_shm_server->define(fcnname2.c_str(),getEntryFunc);

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

	   bool LocalPutEntry(KeyT &k,ValueT& v)
	   {
		bool b = false;
		int ret = invlist->bm->insert(k,v);
		if(ret==INSERTED) b = true;
		return b;
	   }

	   std::vector<ValueT> LocalGetEntry(KeyT &k)
	   {
		bool b;
		std::vector<ValueT> values;
		b = invlist->bm->getvalues(k,values);
		return values;
	   }

	   void ThalliumLocalPutEntry(const tl::request &req,KeyT &k,ValueT &v)
           {
                req.respond(LocalPutEntry(k,v));
           }

	   void ThalliumLocalGetEntry(const tl::request &req,KeyT &k)
	   {
		req.respond(LocalGetEntry(k));
	   }

	   bool PutEntry(KeyT &k,ValueT &v,int destid)
	   {
              if(ipaddrs[destid].compare(myipaddr)==0)
              {
                    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
		    std::string fcnname = rpc_prefix+"RemotePutEntry";
                    tl::remote_procedure rp = thallium_shm_client->define(fcnname.c_str());
                    return rp.on(ep)(k,v);
              }
              else
              {
		    std::string fcnname = rpc_prefix+"RemotePutEntry";
                    tl::remote_procedure rp = thallium_client->define(fcnname.c_str());
                    return rp.on(serveraddrs[destid])(k,v);
              }
	   }

	   std::vector<ValueT> GetEntry(KeyT &k,int destid)
	   {
		if(ipaddrs[destid].compare(myipaddr)==0)
		{
		    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
		    std::string fcnname = rpc_prefix+"RemoteGetEntry";
		    tl::remote_procedure rp = thallium_shm_client->define(fcnname.c_str());
		    return rp.on(ep)(k);
		}
		else
		{
		   std::string fcnname = rpc_prefix+"RemoteGetEntry";
		   tl::remote_procedure rp = thallium_client->define(fcnname.c_str());
		   return rp.on(serveraddrs[destid])(k);
		}
	   }

	   std::vector<struct event> get_events(KeyT&,std::vector<ValueT> &);
	   void create_async_io_request(KeyT &,std::vector<ValueT>&);
	   void create_sync_io_request();
	   bool put_entry(KeyT&,ValueT&);
	   int get_entry(KeyT&,std::vector<ValueT>&);
	   void fill_invlist_from_file(std::string&,int);
	   void flush_table_file(int);
	   int partition_no(KeyT &k);		  
	   void add_entries_to_tables(std::string&,std::vector<struct event>*,uint64_t,int); 
	   void get_entries_from_tables(std::vector<struct KeyIndex<KeyT>> &,int&,int&);
	   std::vector<struct KeyIndex<KeyT>> merge_keyoffsets(std::vector<struct KeyIndex<KeyT>>&,std::vector<struct KeyIndex<KeyT>>&,std::vector<int>&);
};

#include "../srcs/invertedlist.cpp"

#endif
