#ifndef __EVENT_METADATA_H_
#define __EVENT_METADATA_H_

#include <vector>
#include <algorithm>
#include <string>
#include <iostream>

class event_metadata
{

  private:
  int numattrs;
  std::vector<std::string> attr_names;
  std::vector<int> value_sizes;
  std::vector<bool> is_signed;
  std::vector<bool> big_endian;
  int total_v;

  public:
      event_metadata()
      {
      }
      ~event_metadata()
      {
      }
      event_metadata& operator=(event_metadata &m1)
      {
         numattrs = m1.get_numattrs();	
	 attr_names = m1.get_attr_names();
	 value_sizes = m1.get_value_sizes();
	 return *this; 
      }
      void set_numattrs(int n)
      {
	   numattrs = n;
      }
      int get_numattrs()
      {
	    return numattrs;
      }
      std::vector<std::string> &get_attr_names()
      {
	      return attr_names;
      }
      std::vector<int> &get_value_sizes()
      {
	      return value_sizes;
      }
      int get_datasize()
      {
	int n = 0;
	for(int i=0;i<value_sizes.size();i++)
		n += value_sizes[i];
	total_v = n;
	return total_v;
      }
      void add_attr(std::string &s,int size2,bool &sig, bool &end)
      {
	   attr_names.push_back(s);
	   value_sizes.push_back(size2);
	   is_signed.push_back(sig);
	   big_endian.push_back(end);
      }
      bool get_attr(std::string &s,int &v_size,bool &sign,bool &end)
      {
	   auto r = std::find(attr_names.begin(),attr_names.end(),s);
	   if(r != attr_names.end())
	   {
		int pos = std::distance(attr_names.begin(),r);
		v_size = value_sizes[pos];
		sign = is_signed[pos];
		end = big_endian[pos];
		return true;
	   }
	   return false;
      }
      bool get_offset(std::string &s,int &offset)
      {
	auto r = std::find(attr_names.begin(),attr_names.end(),s);
	if(r != attr_names.end())
	{
	   int pos = std::distance(attr_names.begin(),r);
	   int c = 0;
	   for(int i=0;i<pos;i++)
	   {
	        c += value_sizes[i];	
	   }
	   offset = c;
	   return true;
	}
	return false;
      }
};


#endif
