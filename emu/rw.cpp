#include "rw.h"

#define DATASETNAME1 "Data1"
#define DATASETNAME2 "Data2"
#define DATASETNAME3 "Data3"
#define DATASETNAME4 "Data4"
#define DATASETNAME5 "Data5"
#define DATASETNAME6 "Data6"
#define DATASETNAME7 "Data7"
#define DATASETNAME8 "Data8"
#define DATASETNAME9 "Data9"

typedef int DATATYPE;

void read_write_process::create_events(int num_events,std::string &s)
{
    int datasize = 0;
    auto r = write_names.find(s);
    int index = (r->second).first;
    event_metadata em = (r->second).second;
    datasize = em.get_datasize();

    for(int i=0;i<num_events;i++)
    {
	event e;
	e.data.resize(datasize);
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	dm->add_event(e,index);
    }

}

void read_write_process::clear_events(std::string &s)
{
   auto r = read_names.find(s);
   if(r!=read_names.end())
   {
	int index = (r->second).first;
	readevents[index].clear();
   }
   
   r = write_names.find(s);

   if(r != write_names.end())
   {
	int index = (r->second).first;
	myevents[index]->clear();
	dm->clear_write_buffer(index);
   }

}

void read_write_process::pwrite(const char *filename,std::string &name)
{

    hid_t       fid;                                    
    hid_t       acc_tpl;                                
    hid_t       xfer_plist;                            
    hid_t       sid;                                   
    hid_t       file_dataspace;                         
    hid_t       mem_dataspace;                          
    hid_t       dataset1, dataset2, dataset3, dataset4;
    hid_t       dataset5, dataset6, dataset7;           
    hid_t       datatype;

    const char *attr_name[1];
    hsize_t     dims[1];                             
    hid_t attr_id[1];
    hsize_t attr_size[1];
    hid_t attr_space[1];

    hsize_t start[1];               
    hsize_t count[1], stride[1]; 
    hsize_t block[1];              

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT,fapl);
    
    if(fid < 0) std::cout <<" file not created"<<std::endl;

    H5Pclose(fapl);

    auto r = write_names.find(name);

    int index = (r->second).first;
    event_metadata em = (r->second).second;
    int datasize = em.get_datasize();

    std::vector<int> num_events_recorded_l,num_events_recorded;
    num_events_recorded_l.resize(numprocs);
    num_events_recorded.resize(numprocs);
    std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
    std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

    num_events_recorded_l[myrank] = myevents[index]->size();

    MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

    int total_records = 0;
    for(int i=0;i<num_events_recorded.size();i++) total_records += num_events_recorded[i];

    if(myrank==0) std::cout <<" total bytes = "<<(uint64_t)total_records*(datasize+8)<<std::endl;

    uint64_t num_records = total_records;
    uint64_t total_size = num_records*datasize+num_records*sizeof(uint64_t);

    int record_size = datasize+sizeof(uint64_t);
    attr_size[0] = 3;

    std::vector<int> attr_data;
    attr_data.push_back(total_records);
    attr_data.push_back(8);
    attr_data.push_back(datasize);

    uint64_t block_size = num_events_recorded[myrank]*record_size;
   
    hsize_t adims[1];
    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event_hdf));
    H5Tinsert(s2,"key",HOFFSET(struct event_hdf,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event_hdf,data),s1);
    H5Tinsert(s2,"value_d",HOFFSET(struct event_hdf,data_d),H5T_NATIVE_DOUBLE); 

    dims[0] = (hsize_t)num_records;
    sid     = H5Screate_simple(1, dims, NULL);

    int ret = 0;
    dataset1 = H5Dcreate2(fid, DATASETNAME1,s2, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    H5Sclose(sid);

    file_dataspace = H5Dget_space(dataset1);

    hsize_t offset = 0;
    for(int i=0;i<myrank;i++)
	 offset += (hsize_t)num_events_recorded[i];
    hsize_t block_count = (hsize_t)num_events_recorded[myrank];

    ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offset,NULL,&block_count,NULL);

    
    mem_dataspace = H5Screate_simple(1,&block_count, NULL);

    std::vector<struct event_hdf> *data_array1 = new std::vector<struct event_hdf> ();

    for(int i=0;i<myevents[index]->size();i++)
    {
	event e = (*myevents[index])[i];
	struct event_hdf ef;
	ef.ts = e.ts;
	memcpy(ef.data,e.data.data(),e.data.size());
	ef.data_d = 1;
	data_array1->push_back(ef);
    }

   xfer_plist = H5Pcreate(H5P_DATASET_XFER);

   ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);

   ret = H5Dwrite(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());

    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    attr_space[0] = H5Screate_simple(1, attr_size, NULL);

    attr_name[0] = "Data Sizes";
    attr_id[0] = H5Acreate2(dataset1, attr_name[0], H5T_NATIVE_INT, attr_space[0], H5P_DEFAULT, H5P_DEFAULT);
    ret = H5Awrite(attr_id[0], H5T_NATIVE_INT, attr_data.data());

    H5Sclose(attr_space[0]);
    H5Aclose(attr_id[0]);

    ret = H5Dclose(dataset1);
    H5Fclose(fid); 

    data_array1->clear();
}


void read_write_process::pread(const char *filename,std::string &name)
{

    hid_t       fid;                                              
    hid_t       acc_tpl;                                         
    hid_t       xfer_plist;                                       
    hid_t       file_dataspace;                                   
    hid_t       mem_dataspace;                                    
    hid_t       dataset1, dataset2, dataset5, dataset6, dataset7;

    const char *attr_name[1];
    size_t   num_points;    
    int      i, j, k;

    std::vector<struct event_hdf> *data_array1 = nullptr;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);

    attr_name[0] = "Data Sizes";

    dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    hid_t attr_id = H5Aopen(dataset1,attr_name[0],H5P_DEFAULT);
    std::vector<int> attrs;
    attrs.resize(3);

    ret = H5Aread(attr_id,H5T_NATIVE_INT,attrs.data());

    int total_k = attrs[0];
    int k_size = attrs[1];
    int data_size = attrs[2];

    int k_per_process = total_k/numprocs;
    int rem = total_k%numprocs;

    hsize_t offset = 0;

    for(int i=0;i<myrank;i++)
    {
	    if(i < rem) offset += k_per_process+1;
	    else offset += k_per_process;
    }

    int num_events = 0;
    if(myrank < rem) num_events = k_per_process+1;
    else num_events = k_per_process;

    hsize_t block_size = (hsize_t)num_events;

    data_array1 = new std::vector<struct event_hdf> ();

    data_array1->resize(num_events);

    auto r = read_names.find(name);
    if(r == read_names.end())
    {
        event_metadata em1;
        em1.set_numattrs(5);
        for(int i=0;i<5;i++)
        {
           std::string a="attr"+std::to_string(i);
           int vsize = sizeof(double);
           bool is_signed = false;
           bool is_big_endian = true;
           em1.add_attr(a,vsize,is_signed,is_big_endian);
        }
        create_read_buffer(name,em1);
        r = read_names.find(name);
    }
    int index = (r->second).first;
    event_metadata em = (r->second).second;
    int datasize = em.get_datasize();

    hsize_t adims[1];
    adims[0] = datasize;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event_hdf));
    H5Tinsert(s2,"key",HOFFSET(struct event_hdf,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event_hdf,data),s1);
    H5Tinsert(s2,"value_d",HOFFSET(struct event_hdf,data_d),H5T_NATIVE_DOUBLE);


    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&block_size,NULL);
    mem_dataspace = H5Screate_simple(1,&block_size, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());
     
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    ret = H5Aclose(attr_id);
    ret = H5Dclose(dataset1);

    for(int i=0;i<data_array1->size();i++)
    {
	struct event e;
	e.ts = (*data_array1)[i].ts;
	e.data.resize(datasize);
	memcpy(e.data.data(),(*data_array1)[i].data,datasize);
	readevents[index].push_back(e);
    }

    delete data_array1;

   H5Fclose(fid);  

}
