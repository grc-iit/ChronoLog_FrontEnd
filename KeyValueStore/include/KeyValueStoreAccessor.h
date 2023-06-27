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
	    int numprocs;
	    int myrank;
	    KeyValueStoreMetadata md;
	    KeyValueStoreIO *kio; 
	    std::vector<std::pair<std::string,integer_invlist*>> integer_lists;
	    std::vector<std::pair<std::string,unsigned_long_invlist*>> unsigned_long_lists;
	    std::vector<std::pair<std::string,float_invlist*>> float_lists;
	    std::vector<std::pair<std::string,double_invlist*>> double_lists;
	    std::vector<std::string> secondary_attributes;

   public :
	  KeyValueStoreAccessor(int np,int p,KeyValueStoreMetadata &m,KeyValueStoreIO *k)
          {
		numprocs = np;
		myrank = p;
		md = m;
		kio = k;
	  }
	  KeyValueStoreMetadata & get_metadata()
	  {
		return md;
	  }
	  void create_invertedlist(std::string &attr_name)
	  {
		std::vector<std::string> &names = md.attribute_names();
		std::vector<std::string> &types = md.attribute_types();

		int pos = -1;
		for(int i=0;i<names.size();i++)
		{
		    if(names[i].compare(attr_name)==0)
		    {
			pos = i; break;
		    }
		}	
		if(pos==-1) return;

		if(types[pos].compare("int")==0)
		{
		   secondary_attributes.push_back(attr_name);
		   integer_invlist *iv = new integer_invlist(numprocs,myrank);
		   std::pair<std::string,integer_invlist*> sp;
		   sp.first = attr_name;
		   sp.second = iv;
		   integer_lists.push_back(sp);
		}
		else if(types[pos].compare("unsignedlong")==0)
		{
		  secondary_attributes.push_back(attr_name);
		  unsigned_long_invlist *iv = new unsigned_long_invlist(numprocs,myrank);
		  std::pair<std::string,unsigned_long_invlist*> sp;
		  sp.first = attr_name;
		  sp.second = iv;
		  unsigned_long_lists.push_back(sp);
		}
		else if(types[pos].compare("float")==0)
		{	
		   secondary_attributes.push_back(attr_name);
		   float_invlist *iv = new float_invlist(numprocs,myrank);
		   std::pair<std::string,float_invlist*> sp;
		   sp.first = attr_name;
		   sp.second = iv;
		   float_lists.push_back(sp);
		}
		else if(types[pos].compare("double")==0)
		{
		   secondary_attributes.push_back(attr_name);
		   double_invlist *iv = new double_invlist(numprocs,myrank);
		   std::pair<std::string,double_invlist*> sp;
		   sp.first = attr_name;
		   sp.second = iv;
		   double_lists.push_back(sp);
		}
	  }
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
		for(int i=0;i<integer_lists.size();i++)
			delete integer_lists[i].second;
		for(int i=0;i<unsigned_long_lists.size();i++)
			delete unsigned_long_lists[i].second;
		for(int i=0;float_lists.size();i++)
			delete float_lists[i].second;
		for(int i=0;i<double_lists.size();i++)
			delete double_lists[i].second;
	  }

};

#include "../srcs/KeyValueStoreAccessor.cpp"

#endif
