#ifndef __PSCLIENT_H_
#define __PSCLIENT_H_

#include "KeyValueStore.h"

class pubsubclient
{
   private :
	   int numprocs;
	   int myrank;
	   std::vector<std::vector<int>> subscribers;
	   std::vector<std::string,int> table_role;
  	   KeyValueStore *ks; 
   public :

	   pubsubclient(int n,int p) : numprocs(n),myrank(p)
	   {  
		ks = new KeyValueStore(numprocs,myrank);
	   }


	   ~pubsubclient()
	   {
		ks->close_sessions();
		delete ks;

	   }
};


#endif
