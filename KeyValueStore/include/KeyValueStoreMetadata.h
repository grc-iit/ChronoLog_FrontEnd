#ifndef __KeyValueStoreMetadata_H_
#define __KeyValueStoreMetadata_H_

#include <vector>
#include <climits>
#include <string>
#include <numeric>
#include <cfloat>
#include <cmath>


class KeyValueStoreMetadata
{
   private :
	    std::string &name;
	    int numattributes;
	    std::vector<std::string> attr_types;
	    std::vector<std::string> attr_names;
	    std::vector<int> attr_lens;
	    int datalength;

   public :
	   KeyValueStoreMetadata(std::string &sname,int n,std::vector<std::string> &types, std::vector<std::string>&names,std::vector<int> &lens,int len) 
	   {
		name = sname;
		numattributes = n;
		attr_types.assign(types.begin(),types.end());
		attr_names.assign(names.begin(),names.end());
		int l = 0;
		for(int i=0;i<attr_lens.size();i++) l+=lens[i];
		assert (l == len);
		attr_lens.assign(lens.begin(),lens.end());
		datalength = len;
	   }
	   void operator=(KeyValueStoreMetadata &m)
	   {
		name = m.db_name();
		num_attributes = m.num_attributes();
		std::vector<std::string> types = m.attribute_types();
		attr_types.assign(types.begin(),types.end());
		std::vector<std::string> names = m.attribute_names();
		attr_names.assign(names.begin(),names.end());
		std::vector<int> lengths = m.attribute_lengths();
		attr_lens.assign(lengths.begin(),lengths.end());
		datalength = m.value_size();

	   }
	   std::string &s db_name()
	   {
		return name;
	   }
	   int num_attributes()
	   {
		return numattributes;
	   }
	   std::vector<std::string> & attribute_types()
	   {
		return attr_types;
	   }
	   std::vector<std::string> & attribute_names()
	   {
		return attr_names;
	   }
	   std::vector<int> &attribute_lengths()
	   {
		return attr_lens;
	   }
	   int value_size()
	   {
		return datalength;
	   }
	   void get_attribute_features(std::string &sname,std::vector<std::string> &types,std::vector<std::string> &names,std::vector<int> &lens)
	   {
		sname.assign(name);
		types.assign(attr_types.begin(),attr_types.end());
		names.assign(attr_names.begin(),attr_names.end());
		lens.assign(attr_lens.begin(),attr_lens.end());
	   }
	   int locate_offset(std::string &name)
	   {	
		int ret = -1;
		int offset = 0;
		bool found = false;
		for(int i=0;i<attr_names.size();i++)
		{
			if(attr_names[i].compare(name)==0)
			{
				found = true;
				break;
			}
			offset += attr_lens[i];
		}
		if(found) ret = offset;
		return ret;
	   }
	   ~KeyValueStoreMetadata()
	   {	   


	   }

};


#endif
