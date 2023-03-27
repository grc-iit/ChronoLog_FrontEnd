#ifndef __HASHMAP_H_
#define __HASHMAP_H_

#include "block_map.h" 
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
#include "event.h"
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>

namespace tl=thallium;

template<typename A>
void serialize(A &ar,event &e)
{
	ar & e.ts;
	ar & e.data;
}

template <class KeyT,
          class ValueT,
          class HashFcn=std::hash<KeyT>,
          class EqualFcn=std::equal_to<KeyT>>
class distributed_hashmap
{
   public :
      typedef BlockMap<KeyT,ValueT,HashFcn,EqualFcn> map_type;
      typedef memory_pool<KeyT,ValueT,HashFcn,EqualFcn> pool_type;

 private:
        uint64_t totalSize;
        uint64_t maxSize;
        uint64_t min_range;
        uint64_t max_range;
        uint32_t nservers;
        uint32_t serverid;
	int numcores;
        KeyT emptyKey;
        pool_type *pl;
        map_type *my_table;
	tl::engine *thallium_server;
	tl::engine *thallium_shm_server;
	tl::engine *thallium_client;
	tl::engine *thallium_shm_client;
	std::vector<tl::endpoint> serveraddrs;
	std::vector<std::string> ipaddrs;
	std::vector<std::string> shmaddrs;
	std::string myipaddr;
	ClockSynchronization<ClocksourceCPPStyle> *CM;
	int dropped_events;
	double time_m;
 public: 

   uint64_t serverLocation(KeyT &k)
   {
      uint64_t localSize = totalSize/nservers;
      uint64_t rem = totalSize%nservers;
      uint64_t hashval = HashFcn()(k);
      uint64_t v = hashval % totalSize;
      uint64_t offset = rem*(localSize+1);
      uint64_t id = -1;
      if(v >= 0 && v < totalSize)
      {
         if(v < offset)
           id = v/(localSize+1);
         else id = rem+((v-offset)/localSize);
      }

      return id;
   }


   void initialize_tables(uint64_t n,uint32_t np,int nc,uint32_t rank,KeyT maxKey)
    {
        totalSize = n;
        nservers = np;
	numcores = nc;
        serverid = rank;
        emptyKey = maxKey;
        my_table = nullptr;
        pl = nullptr;
        assert (totalSize > 0 && totalSize < UINT64_MAX);
        uint64_t localSize = totalSize/nservers;
        uint64_t rem = totalSize%nservers;
        if(serverid < rem) maxSize = localSize+1;
        else maxSize = localSize;
        assert (maxSize > 0 && maxSize < UINT64_MAX);
        min_range = 0;

        if(serverid < rem)
           min_range = serverid*(localSize+1);
        else
           min_range = rem*(localSize+1)+(serverid-rem)*localSize;

        max_range = min_range + maxSize;

        pl = new pool_type(200);
        my_table = new map_type(maxSize,pl,emptyKey);

    }

   void server_client_addrs(tl::engine *t_server,tl::engine *t_client,tl::engine *t_server_shm, tl::engine *t_client_shm,std::vector<tl::endpoint> &s_addrs,std::vector<std::string> &ips,std::vector<std::string> &shm_addrs)
   {
	   thallium_server = t_server;
	   thallium_shm_server = t_server_shm;
	   thallium_client = t_client;
	   thallium_shm_client = t_client_shm;
	   serveraddrs.assign(s_addrs.begin(),s_addrs.end());
	   ipaddrs.assign(ips.begin(),ips.end());
	   shmaddrs.assign(shm_addrs.begin(),shm_addrs.end());
   } 

   void bind_functions()
   {
	std::function<void(const tl::request &, KeyT &, ValueT &)> insertFunc(
        std::bind(&distributed_hashmap<KeyT, ValueT,HashFcn,EqualFcn>::ThalliumLocalInsert,
        this, std::placeholders::_1, std::placeholders::_2,std::placeholders::_3));

	std::function<void(const tl::request &,KeyT &)> findFunc(
	std::bind(&distributed_hashmap<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalFind,
	this,std::placeholders::_1,std::placeholders::_2));

	std::function<void(const tl::request &,KeyT &)> eraseFunc(
	std::bind(&distributed_hashmap<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalErase,
	this,std::placeholders::_1,std::placeholders::_2));

	thallium_server->define("RemoteInsert",insertFunc);
	thallium_server->define("RemoteFind",findFunc);
	thallium_server->define("RemoteErase",eraseFunc);
	thallium_shm_server->define("RemoteInsert",insertFunc);
	thallium_shm_server->define("RemoteFind",findFunc);
	thallium_shm_server->define("RemoteErase",eraseFunc);
   }

  distributed_hashmap()
  {
	pl = nullptr;
	my_table = nullptr;
	dropped_events = 0;
	time_m = 0;

  }
 ~distributed_hashmap()
  {
    if(my_table != nullptr) delete my_table;
    if(pl != nullptr) delete pl;
  }

   void setClock(ClockSynchronization<ClocksourceCPPStyle> *C)
   {

	 CM = C;
   }
   bool LocalInsert(KeyT &k,ValueT &v)
  {
      if(!CM->NearTime(k))
      {
	      dropped_events++;
	      return false;
      }
      uint32_t r = my_table->insert(k,v);
      if(r == INSERTED) return true;
      else 
	   return false;
  }
  bool LocalFind(KeyT &k)
  {
    if(my_table->find(k) != NOT_IN_TABLE) return true;
    else return false;
  }
  bool LocalErase(KeyT &k)
  {
     return my_table->erase(k);
  }
  bool LocalUpdate(KeyT &k,ValueT &v)
  {
       return my_table->update(k,v);
  }
  bool LocalGet(KeyT &k,ValueT* v)
  {
       return my_table->get(k,v);
  }

  ValueT LocalGetValue(KeyT &k)
  {
        ValueT v;
        new (&v) ValueT();
        bool b = LocalGet(k,&v);
        return v;
  }
  
  bool LocalGetMap(std::vector<ValueT> &values)
  {
	my_table->get_map(values);
	return true;
  }

  bool LocalClearMap()
  {
	my_table->clear_map();
	dropped_events = 0;
	return true;
  }

  template<typename... Args>
  bool LocalUpdateField(KeyT &k,void(*f)(ValueT*,Args&&... args),Args&&...args_)
  {
     return my_table->update_field(k,f,std::forward<Args>(args_)...);
  }

  uint64_t allocated()
  {
     return my_table->allocated_nodes();
  }

  uint64_t removed()
  {
     return my_table->removed_nodes();
  }

  int num_dropped()
  {
	 return dropped_events;
  }
  void ThalliumLocalInsert(const tl::request &req, KeyT &k, ValueT &v)
  {
	req.respond(LocalInsert(k,v));
  }

  void ThalliumLocalFind(const tl::request &req, KeyT &k)
  {
	  req.respond(LocalFind(k));
  }

  void ThalliumLocalErase(const tl::request &req, KeyT &k)
  {
	  req.respond(LocalErase(k));
  }
  bool Insert(KeyT &k, ValueT &v)
  {
    int destid = serverLocation(k);
    if(ipaddrs[destid].compare(myipaddr)==0)
    {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteInsert");
	return rp.on(ep)(k,v);
    }
    else
    {
      tl::remote_procedure rp = thallium_client->define("RemoteInsert");
      return rp.on(serveraddrs[destid])(k,v);
    }
  }
  bool Find(KeyT &k)
  {
    int destid = serverLocation(k);
    if(ipaddrs[destid].compare(myipaddr)==0)
    {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteFind");
	return rp.on(ep)(k);
    }
    else
    {
      tl::remote_procedure rp = thallium_client->define("RemoteFind");
      return rp.on(serveraddrs[destid])(k);
    }
  }
  bool Erase(KeyT &k)
  {
     int destid = serverLocation(k);
     if(ipaddrs[destid].compare(myipaddr)==0)
     {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteErase");
	return rp.on(ep)(k);
     }
     else
     {
       tl::remote_procedure rp = thallium_client->define("RemoteErase");
       return rp.on(serveraddrs[destid])(k);
     }
  }  
};

#endif
