#ifndef __CLIENT_H_
#define __CLIENT_H_
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
#include <vector>

namespace tl=thallium;


class metadata_client
{

   private:
	   std::string serveraddr;
	   std::vector<tl::endpoint> endpoint;
	   tl::engine *thallium_client; 

   public:
	   metadata_client(std::string &s) : serveraddr(s)
	   {

		thallium_client = new tl::engine("ofi+sockets",THALLIUM_CLIENT_MODE,true,1);
		tl::endpoint ep = thallium_client->lookup(serveraddr.c_str());
	        endpoint.push_back(ep);	
	   }

	   ~metadata_client()
	   {
	       delete thallium_client;
	   }

	   void Connect(std::string &client_id)
	   {
		tl::remote_procedure rp = thallium_client->define("connect");
		rp.on(endpoint[0])(client_id);
	   }


};


#endif
