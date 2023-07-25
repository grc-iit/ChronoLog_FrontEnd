#include "util_t.h"


int nearest_power_two(int n)
{
   int c = 1;
   while(c < n) c = 2*c;
   return c;
}

void create_integertestinput(std::string &name,int numprocs,int myrank,int offset,std::vector<int> &keys,std::vector<uint64_t> &timestamps)
{
   std::string filename = "file";
   filename += name+".h5";
   hid_t xfer_plist = H5Pcreate(H5P_DATASET_XFER);
   hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
   H5Pset_fapl_mpio(fapl,MPI_COMM_WORLD, MPI_INFO_NULL);
   H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);

   hid_t fid = H5Fopen(filename.c_str(),H5F_ACC_RDONLY,fapl);

   hsize_t attr_size[1];
   attr_size[0] = MAXBLOCKS*4+4;
   const char *attrname[1];
   hid_t attr_space[1];
   attr_space[0] = H5Screate_simple(1, attr_size, NULL);

   attrname[0] = "Datasizes";

  
   std::string data_string = "Data1";
   hid_t dataset1 = H5Dopen2(fid,data_string.c_str(), H5P_DEFAULT);

   hid_t file_dataspace = H5Dget_space(dataset1);
   hid_t attr_id = H5Aopen(dataset1,attrname[0],H5P_DEFAULT);
   std::vector<uint64_t> attrs;
   attrs.resize(attr_size[0]);

   int ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

   hsize_t adims[1];
   adims[0] = VALUESIZE;
   hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
   hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
   if(s1< 0 || s2 < 0) std::cout <<" data types "<<std::endl;
   H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
   H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

 

   int pos = 4;
   int numblocks = attrs[3];

   int offset_r = 0;
   std::vector<struct event> *buffer = new std::vector<struct event> ();

   for(int i=0;i<numblocks;i++)
   {
	int nrecords = attrs[pos+i*4+3];

        int records_per_proc = nrecords/numprocs;
        int rem = nrecords%numprocs;

        hsize_t pre = (hsize_t)offset_r;
        for(int j=0;j<myrank;j++)
        {
           int size_p=0;
           if(j < rem) size_p = records_per_proc+1;
           else size_p = records_per_proc;
           pre += size_p;
        }

        hsize_t blocksize;
        if(myrank < rem) blocksize = records_per_proc+1;
        else blocksize = records_per_proc;

        buffer->clear();
        buffer->resize(blocksize);

        ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&pre,NULL,&blocksize,NULL);
        hid_t mem_dataspace = H5Screate_simple(1,&blocksize, NULL);
        ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist,buffer->data());
        H5Sclose(mem_dataspace);
	
	offset_r += nrecords;

	for(int j=0;j<buffer->size();j++)
	{
	   timestamps.push_back((*buffer)[j].ts);
	   int key = *(int*)((*buffer)[j].data+offset);
	   keys.push_back(key);
	}
   }

   delete buffer;
   H5Aclose(attr_id);
   H5Sclose(attr_space[0]);
   H5Dclose(dataset1);
   H5Tclose(s1);
   H5Tclose(s2);
   H5Sclose(file_dataspace);
   H5Fclose(fid);
   H5Pclose(fapl);
   H5Pclose(xfer_plist);
  

}
