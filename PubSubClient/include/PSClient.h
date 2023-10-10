#ifndef __PSCLIENT_H_
#define __PSCLIENT_H_

#include "KeyValueStore.h"
#include "MessageCache.h"
#include <mpi.h>

class pubsubclient
{
   private :
	   int numprocs;
	   int myrank;
	   std::vector<std::vector<int>> subscribers;
	   std::vector<std::pair<std::string,int>> table_role;
  	   KeyValueStore *ks; 
	   data_server_client *ds;
   public :

	   pubsubclient(int n,int p) : numprocs(n),myrank(p)
	   {  
		ks = new KeyValueStore(numprocs,myrank);
		ds = ks->get_rpc_client();
	   }


	   ~pubsubclient()
	   {
		ks->close_sessions();
		delete ks;

	   }
};


#endif
