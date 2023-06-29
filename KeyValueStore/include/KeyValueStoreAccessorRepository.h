#ifndef __KeyValueStoreAccessorRepository_H_
#define __KeyValueStoreAccessorRepository_H_


#include "KeyValueStoreAccessor.h"
#include "blockmap.h"

#include "KeyValueStoreIO.h"

class KeyValueStoreAccessorRepository
{

   private :
	      int numprocs;
	      int myrank;
	      BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *accessor_maps;
	      memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> *t_pool;
	      KeyValueStoreIO *io;
	      data_server_client *d; 
   public:
	      KeyValueStoreAccessorRepository(int np,int p,KeyValueStoreIO *io_layer,data_server_client *ds) : numprocs(np), myrank(p)
	      {
		io = io_layer;
		d = ds;
		t_pool = new memory_pool<std::string,KeyValueStoreAccessor*,stringhash,stringequal> (100);
		std::string emptykey;
		accessor_maps = new BlockMap<std::string,KeyValueStoreAccessor*,stringhash,stringequal> (128,t_pool,emptykey);
	      }
	      ~KeyValueStoreAccessorRepository()
	      {
		delete accessor_maps;
		delete t_pool;
	      }

	      bool add_accessor(std::string &s,KeyValueStoreMetadata &m)
	      {
		int ret = -1;
		bool b = false;
		if(accessor_maps->find(s)==NOT_IN_TABLE)
		{	
			KeyValueStoreAccessor *ka = new KeyValueStoreAccessor(numprocs,myrank,m,io,d);
			ret = accessor_maps->insert(s,ka);
			if(ret==INSERTED||EXISTS) b = true;
		}
		else b = true;
		return b;
	      }

	      bool find_accessor(std::string &s)
	      {
		if(accessor_maps->find(s)==NOT_IN_TABLE) return false;
		return true;
	      }

	      bool create_invertedlist(std::string &s,std::string &a)
	      {
	           KeyValueStoreAccessor *ka = nullptr;

		   accessor_maps->get(s,&ka);

		   if(ka==nullptr) return false;
		   
		   int ret = ka->create_invertedlist(a);

		   if(ret == -1) return false;
		   return true;
	      }

	      KeyValueStoreAccessor *get_accessor(std::string &s)
	      {
		KeyValueStoreAccessor *k = nullptr;
		accessor_maps->get(s,&k);
		return k;
	      }
	      

};


#endif

