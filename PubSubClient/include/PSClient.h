#ifndef __PSCLIENT_H_
#define __PSCLIENT_H_

#include "KeyValueStore.h"
#include "MessageCache.h"
#include <mpi.h>
#include <thallium.hpp>
#include <thallium/serialization/proc_input_archive.hpp>
#include <thallium/serialization/proc_output_archive.hpp>
#include <thallium/serialization/serialize.hpp>
#include <thallium/serialization/stl/array.hpp>
#include <thallium/serialization/stl/complex.hpp>
#include <thallium/serialization/stl/deque.hpp>
#include <thallium/serialization/stl/forward_list.hpp>
#include <thallium/serialization/stl/list.hpp>
#include <thallium/serialization/stl/map.hpp>
#include <thallium/serialization/stl/multimap.hpp>
#include <thallium/serialization/stl/multiset.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/set.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/tuple.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/unordered_multimap.hpp>
#include <thallium/serialization/stl/unordered_multiset.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cassert>
#include <thread>

class pubsubclient
{
   private :
	   int numprocs;
	   int myrank;
	   std::vector<std::vector<int>> subscribers;
	   std::vector<std::vector<int>> publishers;
	   std::unordered_map<std::string,int> table_roles;
  	   KeyValueStore *ks; 
	   data_server_client *ds;
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
	   std::unordered_map<std::string,int> client_role;
	   std::unordered_map<std::string,int> mcnum;
	   std::vector<message_cache *> mcs;
	   std::mutex m;
	   int tag;
   public :

	   pubsubclient(int n,int p) : numprocs(n),myrank(p)
	   {  
		ks = new KeyValueStore(numprocs,myrank);
		ds = ks->get_rpc_client();
		tl::engine *t_server = ds->get_thallium_server();
                tl::engine *t_server_shm = ds->get_thallium_shm_server();
                tl::engine *t_client = ds->get_thallium_client();
                tl::engine *t_client_shm = ds->get_thallium_shm_client();
                std::vector<tl::endpoint> saddrs = ds->get_serveraddrs();
                std::vector<std::string> ips = ds->get_ipaddrs();
                std::vector<std::string> shm_addrs = ds->get_shm_addrs();
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
		tag = 80000;
	   }

	   void bind_functions()
	   {
	       std::function<void(const tl::request &,std::string &,std::string &)> bcastMesg(
               std::bind(&pubsubclient::ThalliumBroadcastMessage,this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

               std::string fcnname1 = "BroadcastMessage";
               thallium_server->define(fcnname1.c_str(),bcastMesg);
               thallium_shm_server->define(fcnname1.c_str(),bcastMesg);

		barrier();
	   }

	   void create_pub_sub_service(std::string&,std::vector<int> &,std::vector<int> &,int,int);
	   void add_pubs(std::string &,std::vector<int>&);
	   void add_subs(std::string &,std::vector<int>&);
	   void remove_pubs(std::string &,std::vector<int>&);
	   void remove_subs(std::string &,std::vector<int>&);
	   void add_message_cache(std::string &,int,int);
	   bool broadcast_message(std::string&,std::string&);
	   bool publish_message(std::string &s,std::string &);
	   void barrier();

	   void ThalliumBroadcastMessage(const tl::request &req,std::string &s,std::string &msg)
           {
                req.respond(broadcast_message(s,msg));
           }

	   KeyValueStore *getkvs()
	   {
		return ks;
	   }

	   ~pubsubclient()
	   {
		ks->close_sessions();
		delete ks;
		for(int i=0;i<mcs.size();i++)
		  delete mcs[i];
	   }
};


#endif
