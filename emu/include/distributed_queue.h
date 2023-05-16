#ifndef __QUEUE_H_
#define __QUEUE_H_

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
#include "query_request.h"
#include <boost/lockfree/queue.hpp>

namespace tl=thallium;


template<typename A>
void serialize(A &ar,struct query_req &e)
{
        ar & e.name;
        ar & e.minkey;
	ar & e.maxkey;
}


class distributed_queue
{

   private:
	   boost::lockfree::queue<struct query_req*>  *local_queue;
	   tl::engine *thallium_server;
           tl::engine *thallium_shm_server;
           tl::engine *thallium_client;
           tl::engine *thallium_shm_client;
           std::vector<tl::endpoint> serveraddrs;
           std::vector<std::string> ipaddrs;
           std::vector<std::string> shmaddrs;
           std::string myipaddr;
           std::string myhostname;
	   uint32_t nservers;
           uint32_t serverid;

   public:
	   distributed_queue(int np,int r) : nservers(np), serverid(r)
	   {
		local_queue = new boost::lockfree::queue<struct query_req*> (128);
	   }
	   void server_client_addrs(tl::engine *t_server,tl::engine *t_client,tl::engine *t_server_shm, tl::engine *t_client_shm,std::vector<std::string> &ips,std::vector<std::string> &shm_addrs,std::vector<tl::endpoint> &saddrs)
           {
               thallium_server = t_server;
               thallium_shm_server = t_server_shm;
               thallium_client = t_client;
               thallium_shm_client = t_client_shm;
               ipaddrs.assign(ips.begin(),ips.end());
               shmaddrs.assign(shm_addrs.begin(),shm_addrs.end());
               myipaddr = ipaddrs[serverid];
               serveraddrs.assign(saddrs.begin(),saddrs.end());
          }

	   void bind_functions()
	   {
	     std::function<void(const tl::request &, struct query_req &r)> PutFunc(
             std::bind(&distributed_queue::ThalliumLocalPut,this, std::placeholders::_1, std::placeholders::_2));
	     thallium_server->define("RemotePut",PutFunc);
	     thallium_shm_server->define("RemotePut",PutFunc);
	   }

	   bool LocalPut(struct query_req &r)
	   {
		struct query_req *r_n = new struct query_req ();
		r_n->name = r.name;
		r_n->minkey = r.minkey;
		r_n->maxkey = r.maxkey;
		return local_queue->push(r_n);
	   }

	   struct query_req* Get()
	   {

		struct query_req *r = nullptr;
		if(local_queue->pop(r))
		return r;
		else return nullptr;
	   }

	   bool Empty()
	   {
		return local_queue->empty();
	   }

	   ~distributed_queue()
	   {
		delete local_queue;
	   }

	   void ThalliumLocalPut(const tl::request &req, struct query_req &r)
  	   {
        	req.respond(LocalPut(r));
  	   }

	   bool PutAll(struct query_req &r)
  	   {
	       bool b = false;
	       for(int i=0;i<nservers;i++)
	       {
		  int destid = i;
                  if(ipaddrs[destid].compare(myipaddr)==0)
                  {
                     tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
                     tl::remote_procedure rp = thallium_shm_client->define("RemotePut");
                     b = rp.on(ep)(r);
                  }
                  else
                  {
                     tl::remote_procedure rp = thallium_client->define("RemotePut");
                     b = rp.on(serveraddrs[destid])(r);
                  }
               }

	       return b;
	   }

};



#endif
