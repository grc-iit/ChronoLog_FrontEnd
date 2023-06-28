#ifndef __KeyValueStoreIO_H_
#define __KeyValueStoreIO_H_

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
#include <boost/lockfree/queue.hpp>
#include "event.h"
#include <thread>

struct request
{
  std::string name;
  int id;
  int keytype;
  int intkey;
  float floatkey;
  double doublekey;
  int sender;
  bool flush;
};

struct response
{
  std::string name;
  int id;
  int response_id;
  struct event e;
  int sender;
  bool complete;
};

namespace tl=thallium;

template<typename A>
void serialize(A &ar,struct request &e)
{
	ar & e.name;
        ar & e.id;
        ar & e.keytype;
	ar & e.intkey;
	ar & e.floatkey;
	ar & e.doublekey;
	ar & e.sender;
	ar & e.flush;
}	

template<typename A>
void serialize(A &ar,struct response &e)
{
   ar & e.name;
   ar & e.id;
   ar & e.response_id;
   ar & e.e.ts;
   ar & e.e.data;   
   ar & e.sender;
   ar & e.complete;
}


class KeyValueStoreIO
{

   private: 
	   int nservers;
	   int serverid;
	   boost::lockfree::queue<struct request*> *req_queue;
	   boost::lockfree::queue<struct response*> *resp_queue;
	   boost::lockfree::queue<struct request*> *sync_queue;
	   tl::engine *thallium_server;
           tl::engine *thallium_shm_server;
           tl::engine *thallium_client;
           tl::engine *thallium_shm_client;
           std::vector<tl::endpoint> serveraddrs;
           std::vector<std::string> ipaddrs;
           std::vector<std::string> shmaddrs;
           std::string myipaddr;
           std::string myhostname;
	   int num_io_threads;
	   std::vector<std::thread> io_threads;


   public:

	    KeyValueStoreIO(int np,int p) : nservers(np), serverid(p)
	    {
	        num_io_threads = 1;
		req_queue = new boost::lockfree::queue<struct request*> (128);
		resp_queue = new boost::lockfree::queue<struct response*> (128);
		sync_queue = new boost::lockfree::queue<struct request*> (128);
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
	       std::function<void(const tl::request &,struct request &)> putRequestFunc(
               std::bind(&KeyValueStoreIO::ThalliumLocalPutRequest,this,std::placeholders::_1,std::placeholders::_2));

	       std::function<void(const tl::request &,struct response &)> putResponseFunc(
               std::bind(&KeyValueStoreIO::ThalliumLocalPutResponse,this,std::placeholders::_1,std::placeholders::_2));

	       std::function<void(const tl::request &,struct request &)> putSyncRequestFunc(
               std::bind(&KeyValueStoreIO::ThalliumLocalPutSyncRequest,this,std::placeholders::_1,std::placeholders::_2));
               
	       thallium_server->define("RemotePutIORequest",putRequestFunc);
               thallium_shm_server->define("RemotePutIORequest",putRequestFunc);

               thallium_server->define("RemotePutIOResponse",putResponseFunc);
               thallium_shm_server->define("RemotePutIOResponse",putResponseFunc);

	       thallium_server->define("RemotePutSyncIORequest",putSyncRequestFunc);
	       thallium_shm_server->define("RemotePutSyncIORequest",putSyncRequestFunc);
	     }

	     bool LocalPutSyncRequest(struct request &r)
	     {
		struct request *s = new struct request();
                s->name = r.name;
                s->id = r.id;
                s->keytype = r.keytype;
                s->intkey = r.intkey;
                s->floatkey = r.floatkey;
                s->doublekey = r.doublekey;
                s->sender = r.sender;
                s->flush = r.flush;

                sync_queue->push(s);
                return true;
	     }
	     bool LocalPutRequest(struct request &r)
	     {
		struct request *s = new struct request();
		s->name = r.name;
		s->id = r.id;
		s->keytype = r.keytype;
  		s->intkey = r.intkey;
  		s->floatkey = r.floatkey;
  		s->doublekey = r.doublekey;
  		s->sender = r.sender;
  		s->flush = r.flush;
		
		req_queue->push(s);
		return true;
	     }

	     bool LocalPutResponse(struct response &r)
	     {
		struct response *s = new struct response();
		s->name = r.name;
  		s->id = r.id;
  		s->response_id = r.response_id;
  		s->e = r.e;
  		s->sender = r.sender;
  		s->complete = r.complete;
		
		resp_queue->push(s);
		return true;
	     }

	     void ThalliumLocalPutRequest(const tl::request &req,struct request &r)
	     {
		req.respond(LocalPutRequest(r));
	     }

	     void ThalliumLocalPutResponse(const tl::request &req,struct response &r)
	     {
		req.respond(LocalPutResponse(r));
	     }

	     void ThalliumLocalPutSyncRequest(const tl::request &req,struct request &r)
	     {
		req.respond(LocalPutSyncRequest(r));
	     }

	     struct request *GetRequest()
	     {
		
		struct request *r=nullptr;
		bool b = req_queue->pop(r);
		return r;
	     }
	     struct response *GetResponse()
	     {
		struct response *r=nullptr;
		bool b = resp_queue->pop(r);
		return r;
	     }

	     struct request *GetSyncRequest()
	     {
		struct request *r=nullptr;
		bool b = sync_queue->pop(r);
		return r;
	     }

	     bool RequestQueueEmpty()
	     {
		return req_queue->empty();
	     }
		
	     bool ResponseQueueEmpty()
	     {
		return resp_queue->empty();
	     }

	     bool SyncRequestQueueEmpty()
	     {
		return sync_queue->empty();
	     }

	     bool PutRequest(struct request &r,int destid)
	     {
		if(ipaddrs[destid].compare(myipaddr)==0)
                {
                    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
                    tl::remote_procedure rp = thallium_shm_client->define("RemotePutIORequest");
                    return rp.on(ep)(r);
                }
                else
                {
                    tl::remote_procedure rp = thallium_client->define("RemotePutIORequest");
                    return rp.on(serveraddrs[destid])(r);
                }
	     }
	     bool PutResponse(struct response &r,int destid)
	     {

		if(ipaddrs[destid].compare(myipaddr)==0)
                {
                    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
                    tl::remote_procedure rp = thallium_shm_client->define("RemotePutIOResponse");
                    return rp.on(ep)(r);
                }
                else
                {
                    tl::remote_procedure rp = thallium_client->define("RemotePutIOResponse");
                    return rp.on(serveraddrs[destid])(r);
                }
	     }

	     bool PutAll(struct request &r)
	     {
		bool ret = false;
		for(int i=0;i<nservers;i++)
		{
		  int destid = i;
		  if(ipaddrs[destid].compare(myipaddr)==0)
                 {
                    tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
                    tl::remote_procedure rp = thallium_shm_client->define("RemotePutSyncIORequest");
                    bool b = rp.on(ep)(r);
		    ret = ret & b;
                 }
                 else
                 {
                    tl::remote_procedure rp = thallium_client->define("RemotePutSyncIORequest");
                    bool b = rp.on(serveraddrs[destid])(r);
		    ret = ret & b;
                 }

		}
		return ret;
	     }

	     void service_request_queue();
	     void service_sync_queue();
	     void read_query();
	     void sync_writes();

	    ~KeyValueStoreIO()
	    {
		delete req_queue;
		delete resp_queue;
		delete sync_queue;
	    }









};


#endif
