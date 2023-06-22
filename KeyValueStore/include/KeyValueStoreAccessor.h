#ifndef __KeyValueStoreAccessor_H_
#define __KeyValueStoreAccessor_H_

#include "rw.h"
#include "query_engine.h"
#include "inverted_list.h"

typedef hdf5_invlist<int,uint64_t> integer_invlist;
typedef hdf5_invlist<uint64_t,uint64_t> unsigned_long_invlist;
typedef hdf5_invlist<float,uint64_t> float_invlist;
typedef hdf5_invlist<double,uint64_t> double_invlist;

class KeyValueStoreAccessor
{

   private :
	    KeyValueStoreMetadata md; 
	    std::vector<integer_invlist*> integer_lists;
	    std::vector<unsigned_long_invlist*> unsigned_long_lists;
	    std::vector<float_invlist*> float_lists;
	    std::vector<double_invlist*> double_lists;

   public :
	  KeyValueStoreAccessor(KeyValueStoreMetadata &m)
          {
		md = m;
	  }
	  void create_invertedlist(std::string &attr_name)
	  {




	  }	
	  template<typename T>
	  bool insert_invertedlist(std::string &attr_name, T&key,uint64_t &ts);

	  template<typename T>
	  bool find_invertedlist(std::string &attr_name,T &key);
	 
	  template<typename T>
	  uint64_t get_invertedlist(std::string &attr_name,T &key);

	  void flush_invertedlist(std::string &attr_name);
	  ~KeyValueStoreAccessor()
	  {




	  }

};

#endif
