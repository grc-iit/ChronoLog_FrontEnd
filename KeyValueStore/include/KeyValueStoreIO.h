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

struct request
{
  std::string name;
  int id;
  int keytype;
  int intkey;
  float floatkey;
  double doublekey;
  bool flush;
};

struct response
{
  std::string name;
  int id;
  int response_id;
  struct event e;
  bool complete;
};

namespace tl=thallium;

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


   public:

	    KeyValueStoreIO(int np,int p) : nservers(np), serverid(p)
	    {
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

	    ~KeyValueStoreIO()
	    {
		delete req_queue;
		delete resp_queue;
		delete sync_queue;
	    }









};


#endif
