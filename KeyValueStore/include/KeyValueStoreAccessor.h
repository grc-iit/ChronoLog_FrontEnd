#ifndef __KeyValueStoreAccessor_H_
#define __KeyValueStoreAccessor_H_

#include "rw.h"
#include "query_engine.h"
#include "inverted_list.h"
#include "external_sort.h"
#include "KeyValueStoreIO.h"
#include "util.h"

typedef hdf5_invlist<int,uint64_t,inthashfunc,std::equal_to<int>> integer_invlist;
typedef hdf5_invlist<uint64_t,uint64_t,unsignedlonghashfunc,std::equal_to<uint64_t>> unsigned_long_invlist;
typedef hdf5_invlist<float,uint64_t,floathashfunc,std::equal_to<float>> float_invlist;
typedef hdf5_invlist<double,uint64_t,doublehashfunc,std::equal_to<double>> double_invlist;

class KeyValueStoreAccessor
{

   private :
	    KeyValueStoreMetadata *md;
	    KeyValueStoreIO *kio; 
	    std::vector<std::pair<std::string,integer_invlist*>> integer_lists;
	    std::vector<std::pair<std::string,unsigned_long_invlist*>> unsigned_long_lists;
	    std::vector<std::pair<std::string,float_invlist*>> float_lists;
	    std::vector<std::pair<std::string,double_invlist*>> double_lists;
	    std::vector<std::string> secondary_attributes;

   public :
	  KeyValueStoreAccessor(KeyValueStoreMetadata &m,KeyValueStoreIO *k)
          {
		*md = m;
		kio = k;
	  }
	  KeyValueStoreMetadata & get_metadata()
	  {
		return *md;
	  }
	  void create_invertedlist(std::string &attr_name);
	  template<typename T>
	  bool insert_entry(std::string &attr_name, T&key,uint64_t &ts);
	  template<typename T>
	  bool find_entry(std::string &attr_name,T &key);
	  template<typename T>
	  uint64_t get_entry(std::string &attr_name,T &key);
	  void flush_invertedlist(std::string &attr_name);
	  void fill_invertedlist(std::string &);
	  template <typename T>
	  bool Put(T &key, char *value);
	  template <typename T>
          bool Get(T &key,char *value);
	  void sort_on_secondary_key(std::string &attr_name);
	  ~KeyValueStoreAccessor()
	  {




	  }

};

#endif
