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
  std::vector<int> attr_sizes;
  std::vector<int> value_sizes;
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
	 attr_sizes = m1.get_attr_sizes();
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
      std::vector<int> &get_attr_sizes()
      {
	      return attr_sizes;
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
      void add_attr(std::string &s,int size1,int size2)
      {
	   attr_names.push_back(s);
	   attr_sizes.push_back(size1);
	   value_sizes.push_back(size2);
      }
      bool get_attr(std::string &s,int &a_size,int &v_size)
      {
	   auto r = std::find(attr_names.begin(),attr_names.end(),s);
	   if(r != attr_names.end())
	   {
		int pos = std::distance(attr_names.begin(),r);
		a_size = attr_sizes[pos];
		v_size = value_sizes[pos];
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
		c += attr_sizes[i];
	        c += value_sizes[i];	
	   }
	   offset = c;
	   return true;
	}
	return false;
      }
};


#endif
