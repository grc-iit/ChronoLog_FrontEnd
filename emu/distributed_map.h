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
        std::vector<uint64_t> totalSizes;
	std::vector<uint64_t> maxSizes;
        uint32_t nservers;
        uint32_t serverid;
	int numcores;
	std::vector<KeyT> emptyKeys;
	std::vector<pool_type *>pls;
	std::vector<map_type *>my_tables;
	std::unordered_map<std::string,int> table_names;
	tl::engine *thallium_server;
	tl::engine *thallium_shm_server;
	tl::engine *thallium_client;
	tl::engine *thallium_shm_client;
	std::vector<tl::endpoint> serveraddrs;
	std::vector<std::string> ipaddrs;
	std::vector<std::string> shmaddrs;
	std::string myipaddr;
	std::string myhostname;
	ClockSynchronization<ClocksourceCPPStyle> *CM;
	int dropped_events;
	double time_m;
	std::mutex name_lock;
	int maxtables;
	std::atomic<int> num_tables;
 public: 

   uint64_t serverLocation(KeyT &k,int i)
   {
      uint64_t localSize = totalSizes[i]/nservers;
      uint64_t rem = totalSizes[i]%nservers;
      uint64_t hashval = HashFcn()(k);
      uint64_t v = hashval % totalSizes[i];
      uint64_t offset = rem*(localSize+1);
      uint64_t id = -1;
      if(v >= 0 && v < totalSizes[i])
      {
         if(v < offset)
           id = v/(localSize+1);
         else id = rem+((v-offset)/localSize);
      }

      return id;
   }


   void create_table(uint64_t n,KeyT maxKey,std::string &table_name)
    {
          uint64_t tsize = n;
          KeyT emptyKey = maxKey;
          assert (tsize > 0 && tsize < UINT64_MAX);
          uint64_t localSize = tsize/nservers;
          uint64_t rem = tsize%nservers;
	  uint64_t maxSize;
          if(serverid < rem) maxSize = localSize+1;
          else maxSize = localSize;
          assert (maxSize > 0 && maxSize < UINT64_MAX);

          pool_type *pl = new pool_type(200);
          map_type *my_table = new map_type(maxSize,pl,emptyKey);
	  int pv = num_tables.fetch_add(1);
	  if(pv < maxtables)
	  {
	  	totalSizes[pv] = tsize;
	  	maxSizes[pv] = maxSize;
	  	emptyKeys[pv] = emptyKey;
          	my_tables[pv] = my_table;
          	pls[pv] = pl;
	        name_lock.lock();
		std::pair<std::string,int> p(table_name,pv);
		table_names.insert(p);
		name_lock.unlock();	
	  }

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
	std::function<void(const tl::request &, KeyT &, ValueT &, std::string &)> insertFunc(
        std::bind(&distributed_hashmap<KeyT, ValueT,HashFcn,EqualFcn>::ThalliumLocalInsert,
        this, std::placeholders::_1, std::placeholders::_2,std::placeholders::_3,std::placeholders::_4));

	std::function<void(const tl::request &,KeyT &,std::string &)> findFunc(
	std::bind(&distributed_hashmap<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalFind,
	this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

	std::function<void(const tl::request &,KeyT &,std::string &)> eraseFunc(
	std::bind(&distributed_hashmap<KeyT,ValueT,HashFcn,EqualFcn>::ThalliumLocalErase,
	this,std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));

	thallium_server->define("RemoteInsert",insertFunc);
	thallium_server->define("RemoteFind",findFunc);
	thallium_server->define("RemoteErase",eraseFunc);
	thallium_shm_server->define("RemoteInsert",insertFunc);
	thallium_shm_server->define("RemoteFind",findFunc);
	thallium_shm_server->define("RemoteErase",eraseFunc);
   }

  distributed_hashmap(uint64_t np,uint64_t nc, int rank)
  {
	nservers = np;
	numcores = nc;
	serverid = rank;
	maxtables = 1000;
	num_tables.store(0);
	maxSizes.resize(maxtables);
	totalSizes.resize(maxtables);
	emptyKeys.resize(maxtables);
	my_tables.resize(maxtables);
	pls.resize(maxtables);
	for(int i=0;i<maxtables;i++)
	{
	  my_tables[i] = nullptr;
	  pls[i] = nullptr;
	}
  }
 ~distributed_hashmap()
  {
	  for(int i=0;i<maxtables;i++)
	  {
		if(my_tables[i] != nullptr)  delete my_tables[i];
		if(pls[i] != nullptr) delete pls[i];
	  }
  }

   void setClock(ClockSynchronization<ClocksourceCPPStyle> *C)
   {
	 CM = C;
   }
   bool LocalInsert(KeyT &k,ValueT &v,std::string &s)
  {
      int index = -1;
      name_lock.lock();
      auto r = table_names.find(s);
      if(r != table_names.end()) index = r->second;
      name_lock.unlock();
      if(!CM->NearTime(k))
      {
	      dropped_events++;
	      return false;
      }
      if(index != -1)
      {
        uint32_t b = my_tables[index]->insert(k,v);
        if(b == INSERTED) return true;
        else 
	   return false;
      }
      return false;
  }
  bool LocalFind(KeyT &k,std::string &s)
  {
    int index = -1;
    name_lock.lock();
    auto r = table_names.find(s);
    if(r != table_names.end()) index = r->second;
    name_lock.unlock();
    if(my_tables[index]->find(k) != NOT_IN_TABLE) return true;
    else return false;
  }
  bool LocalErase(KeyT &k,std::string &s)
  {
     int index = -1;
     name_lock.lock();
     auto r = table_names.find(s);
     if(r != table_names.end()) index = r->second;
     name_lock.unlock();
     return my_tables[index]->erase(k);
  }
  bool LocalUpdate(KeyT &k,ValueT &v,int i)
  {
       return my_tables[i]->update(k,v);
  }
  bool LocalGet(KeyT &k,ValueT* v,int i)
  {
       return my_tables[i]->get(k,v);
  }

  ValueT LocalGetValue(KeyT &k,int i)
  {
        ValueT v;
        new (&v) ValueT();
        bool b = LocalGet(k,&v,i);
        return v;
  }
  
  bool LocalGetMap(std::vector<ValueT> &values,int i)
  {
	my_tables[i]->get_map(values);
	return true;
  }

  bool LocalClearMap(std::string &s)
  {
	int index = -1;
	name_lock.lock();
	auto r = table_names.find(s);
	if(r != table_names.end()) index = r->second;
	name_lock.unlock();
	my_tables[index]->clear_map();
	dropped_events = 0;
	return true;
  }

  template<typename... Args>
  bool LocalUpdateField(KeyT &k,int i,void(*f)(ValueT*,Args&&... args),Args&&...args_)
  {
     return my_tables->update_field(k,f,std::forward<Args>(args_)...);
  }

  uint64_t allocated(int i)
  {
     return my_tables[i]->allocated_nodes();
  }

  uint64_t removed(int i)
  {
     return my_tables[i]->removed_nodes();
  }

  int num_dropped()
  {
	 return dropped_events;
  }
  void ThalliumLocalInsert(const tl::request &req, KeyT &k, ValueT &v, std::string &s)
  {
	req.respond(LocalInsert(k,v,s));
  }

  void ThalliumLocalFind(const tl::request &req, KeyT &k,std::string &s)
  {
	  req.respond(LocalFind(k,s));
  }

  void ThalliumLocalErase(const tl::request &req, KeyT &k,std::string &s)
  {
	  req.respond(LocalErase(k,s));
  }
  bool Insert(KeyT &k, ValueT &v,std::string &s)
  {
    name_lock.lock();
    int index = -1;
    auto r = table_names.find(s);
    if(r != table_names.end()) index = r->second;
    name_lock.unlock();
    int destid = serverLocation(k,index);
    if(ipaddrs[destid].compare(myipaddr)==0)
    {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteInsert");
	return rp.on(ep)(k,v,s);
    }
    else
    {
      tl::remote_procedure rp = thallium_client->define("RemoteInsert");
      return rp.on(serveraddrs[destid])(k,v,s);
    }
  }
  bool Find(KeyT &k,std::string &s)
  {
    int index = -1;
    name_lock.lock();
    auto r = table_names.find(s);
    if(r != table_names.end()) index = r->second;
    name_lock.unlock();
    int destid = serverLocation(k,index);
    if(ipaddrs[destid].compare(myipaddr)==0)
    {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteFind");
	return rp.on(ep)(k,s);
    }
    else
    {
      tl::remote_procedure rp = thallium_client->define("RemoteFind");
      return rp.on(serveraddrs[destid])(k,s);
    }
  }
  bool Erase(KeyT &k,std::string &s)
  {
     int index = -1;
     name_lock.lock();
     auto r = table_names.find(s);
     if(r != table_names.end()) index = r->second;
     name_lock.unlock();
     int destid = serverLocation(k,index);
     if(ipaddrs[destid].compare(myipaddr)==0)
     {
	tl::endpoint ep = thallium_shm_client->lookup(shmaddrs[destid]);
	tl::remote_procedure rp = thallium_shm_client->define("RemoteErase");
	return rp.on(ep)(k,s);
     }
     else
     {
       tl::remote_procedure rp = thallium_client->define("RemoteErase");
       return rp.on(serveraddrs[destid])(k,s);
     }
  }  
};

#endif
