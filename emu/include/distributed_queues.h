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
#include "query_response.h"
#include <boost/lockfree/queue.hpp>

namespace tl=thallium;


template<typename A>
void serialize(A &ar,struct query_req &e)
{
        ar & e.name;
        ar & e.minkey;
	ar & e.maxkey;
	ar & e.id;
	ar & e.sender;
	ar & e.from_nvme;
	ar & e.sorted;
	ar & e.collective;
	ar & e.single_point;
	ar & e.output_file;
	ar & e.op;
}

template<typename A>
void serialize(A &ar,struct query_resp &e)
{
   ar & e.id;
   ar & e.response_id;
   ar & e.minkey;
   ar & e.maxkey;
   ar & e.sender;
   ar & e.response.ts;
   ar & e.response.data;
   ar & e.output_file;
   ar & e.complete;
}

class distributed_queues
{

   private:
	   boost::lockfree::queue<struct query_req*>  *local_req_queue;
	   boost::lockfree::queue<struct query_resp*> *local_resp_queue;
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
	   distributed_queues(int np,int r) : nservers(np), serverid(r)
	   {
		local_req_queue = new boost::lockfree::queue<struct query_req*> (128);
		local_resp_queue = new boost::lockfree::queue<struct query_resp*> (128);
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
	     std::function<void(const tl::request &, struct query_req &r)> PutRequestFunc(
             std::bind(&distributed_queues::ThalliumLocalPutRequest,this, std::placeholders::_1, std::placeholders::_2));

	     std::function<void(const tl::request &,struct query_resp &r)> PutResponseFunc(
	     std::bind(&distributed_queues::ThalliumLocalPutResponse,this,std::placeholders::_1, std::placeholders::_2));

	     thallium_server->define("RemotePutRequest",PutRequestFunc);
	     thallium_shm_server->define("RemotePutRequest",PutRequestFunc);

	     thallium_server->define("RemotePutResponse",PutResponseFunc);
	     thallium_shm_server->define("RemotePutResponse",PutResponseFunc);
	   }

	   bool LocalPutRequest(struct query_req &r)
	   {
		struct query_req *r_n = new struct query_req ();
		r_n->name = r.name;
		r_n->minkey = r.minkey;
		r_n->maxkey = r.maxkey;
		r_n->id = r.id;
		r_n->from_nvme = r.from_nvme;
		r_n->collective = r.collective;
		r_n->sorted = r.sorted;
		r_n->output_file = r.output_file;
		r_n->single_point = r.single_point;
		r_n->op = r.op;
		r_n->sender = r.sender;
		return local_req_queue->push(r_n);
	   }

	   bool LocalPutResponse(struct query_resp &r)
	   {
		struct query_resp *r_n = new struct query_resp();
		r_n->id = r.id;
   		r_n->response_id = r.response_id;
   		r_n->minkey = r.minkey;
   		r_n->maxkey = r.maxkey;
   		r_n->sender = r.sender;
   		r_n->response = r.response;
   		r_n->output_file = r.output_file;
   		r_n->complete = r.complete;
		return local_resp_queue->push(r_n);
	   }

	   struct query_req* GetRequest()
	   {

		struct query_req *r = nullptr;
		if(local_req_queue->pop(r))
		return r;
		else return nullptr;
	   }

	   struct query_resp *GetResponse()
	   {
		struct query_resp *r = nullptr;
		if(local_resp_queue->pop(r))
	 	return r;
		else return nullptr;
	   }

	   bool EmptyRequestQueue()
	   {
		return local_req_queue->empty();
	   }

	   bool EmptyResponseQueue()
	   {
		return local_resp_queue->empty();
	   }

	   ~distributed_queues()
	   {
		while(!local_req_queue->empty())
		{
		   struct query_req *r = nullptr;
		   local_req_queue->pop(r);
		   delete r;
		}
		delete local_req_queue;

		int i = 0;
		while(!local_resp_queue->empty())
		{
		   struct query_resp *r = nullptr;
		   local_resp_queue->pop(r);
		   delete r;
		   i++;
		}
		delete local_resp_queue;
	   }

	   void ThalliumLocalPutRequest(const tl::request &req, struct query_req &r)
  	   {
        	req.respond(LocalPutRequest(r));
  	   }

	   void ThalliumLocalPutResponse(const tl::request &req, struct query_resp &r)
	   {
		req.respond(LocalPutResponse(r));
	   }

	   bool PutRequestAll(struct query_req &r)
  	   {
	       bool b = false;
	       for(int i=0;i<nservers;i++)
	       {
		  int destid = i;
                  if(ipaddrs[destid].compare(myipaddr)==0)
                  {
                     tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
                     tl::remote_procedure rp = thallium_shm_client->define("RemotePutRequest");
                     b = rp.on(ep)(r);
                  }
                  else
                  {
                     tl::remote_procedure rp = thallium_client->define("RemotePutRequest");
                     b = rp.on(serveraddrs[destid])(r);
                  }
               }

	       return b;
	   }
	   bool PutRequest(struct query_req &r,int server_id)
	   {
		bool b = false;
		if(ipaddrs[server_id].compare(myipaddr)==0)
		{
		  tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[server_id]);
		  tl::remote_procedure rp = thallium_shm_client->define("RemotePutRequest");
		  b = rp.on(ep)(r);
		}
		else
		{
		   tl::remote_procedure rp = thallium_client->define("RemotePutRequest");
		   b = rp.on(serveraddrs[server_id])(r);
		}
		return b;
	   }
	   bool PutResponse(struct query_resp &r,int server_id)
	   {
		bool b = false;
		if(ipaddrs[server_id].compare(myipaddr)==0)
		{
		   tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[server_id]);
		   tl::remote_procedure rp = thallium_shm_client->define("RemotePutResponse");
		   b = rp.on(ep)(r);
		}
		else
		{
		   tl::remote_procedure rp = thallium_client->define("RemotePutResponse");
		   b = rp.on(serveraddrs[server_id])(r);
		}
		return b;
	   }

};



#endif
