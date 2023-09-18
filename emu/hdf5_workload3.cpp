#include <vector>
#include <iostream>
#include <sstream>
#include "hdf5.h"
#include "h5_async_lib.h"
#include <string>
#include <cfloat>
#include <cstdlib>
#include <fstream>
#include <cstring>
#include <chrono>
#include <ctime>
#include <mpi.h>
#include <algorithm>
#include <cfloat>
#include <cmath>

struct keydata
{
  uint64_t ts;
  char *data;
};

struct keyindex
{
  uint64_t ts;
  int pos;
};


int main(int argc,char **argv)
{
   //std::string filename = "timeseries_ycsb.log";

   //std::string hfile = "timeseries_ycsb.h5";

    std::string filename = "loadb.log";

   int prov;

   MPI_Init_thread(&argc,&argv,MPI_THREAD_SINGLE,&prov);

   std::ifstream ist(filename.c_str(),std::ios_base::in);

   std::vector<std::string> lines;

   std::vector<uint64_t> keys;
   std::vector<std::string> values;
   int columnsize = 100;

   if(ist.is_open())
   {
     std::string line;
     while(getline(ist,line))
     {
	std::stringstream ss(line);
	std::string string1;
	ss >> string1;
	if(string1.compare("INSERT")==0)
	lines.push_back(line);
     }

     std::cout <<" numlines = "<<lines.size()<<std::endl;

     std::string str1 = "usertable";
     std::string str2 = "user";
     std::vector<std::string> fields;
     fields.push_back("field0=");
     fields.push_back("field1=");
     fields.push_back("field2=");
     fields.push_back("field3=");
     fields.push_back("field4=");
     fields.push_back("field5=");
     fields.push_back("field6=");
     fields.push_back("field7=");
     fields.push_back("field8=");
     fields.push_back("field9=");
     std::string b1 = "[";
     std::string b2 = "]";
     for(int i=0;i<lines.size();i++)
     {
       int pos1 = lines[i].find(str1.c_str())+str1.length();
       std::string substr1 = lines[i].substr(pos1);
       std::stringstream ss(substr1);
       std::string tss;
       ss >> tss;
       int pos2 = tss.find(str2.c_str())+str2.length();
       std::string substr2 = tss.substr(pos2);
       uint64_t key = std::stoul(substr2,nullptr);
       std::vector<std::string> fieldstrings;
       fieldstrings.resize(fields.size());
       std::string data;
       for(int j=0;j<fields.size();j++)
       {
       pos1 = lines[i].find(fields[j].c_str())+fields[j].length();
       substr1 = lines[i].substr(pos1);
       pos2 = substr1.find("field");
       fieldstrings[j].resize(columnsize);
       if(pos2 != std::string::npos)
       {
       substr1 = lines[i].substr(pos1,pos2-1);
       for(int k=0;k<substr1.length();k++)
       fieldstrings[j][k] = substr1[k];
       }
       else
       {
	 pos2 = substr1.find("]");
	 if(pos2 != std::string::npos)
	 {
	   substr1 = lines[i].substr(pos1,pos2-1);
	   for(int k=0;k<substr1.length();k++)
	   fieldstrings[j][k] = substr1[k];
	 }
       }
       data += fieldstrings[j];
       }
       keys.push_back(key);
       values.push_back(data);
     }

     ist.close();
   }


   MPI_Finalize();


}
