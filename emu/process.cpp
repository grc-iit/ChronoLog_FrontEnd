#include "process.h"

#define RANK 2
#define DIM0 600
#define DIM1 1200
#define EVENTSIZE 108

#define DATASETNAME1 "Data1"
#define DATASETNAME2 "Data2"
#define DATASETNAME3 "Data3"
#define DATASETNAME4 "Data4"
#define DATASETNAME5 "Data5"
#define DATASETNAME6 "Data6"
#define DATASETNAME7 "Data7"
#define DATASETNAME8 "Data8"
#define DATASETNAME9 "Data9"

#include "mds.h"

typedef int DATATYPE;

void read_write_process::create_events(int num_events)
{
    for(int i=0;i<num_events;i++)
    {
	event e;
	std::fill(e.data,e.data+DATASIZE,0);
	uint64_t ts = CM->Timestamp();

	e.ts = ts;
	      
	dm->add_event(e);
    }

}

void read_write_process::pwrite(const char *filename)
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

    std::vector<char> *data_array1 = new std::vector<char> ();

    hsize_t start[1];               
    hsize_t count[1], stride[1]; 
    hsize_t block[1];              

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT,fapl);
    
    if(fid < 0) std::cout <<" file not created"<<std::endl;

    H5Pclose(fapl);

    std::vector<int> num_events_recorded_l,num_events_recorded;
    num_events_recorded_l.resize(numprocs);
    num_events_recorded.resize(numprocs);
    std::fill(num_events_recorded_l.begin(),num_events_recorded_l.end(),0);
    std::fill(num_events_recorded.begin(),num_events_recorded.end(),0);

    num_events_recorded_l[myrank] = myevents.size();

    MPI_Allreduce(num_events_recorded_l.data(),num_events_recorded.data(),numprocs,MPI_INT,MPI_SUM,MPI_COMM_WORLD);

    int total_records = 0;
    for(int i=0;i<num_events_recorded.size();i++) total_records += num_events_recorded[i];

    if(myrank==0) std::cout <<" total bytes = "<<(uint64_t)total_records*(DATASIZE+8)<<std::endl;

    uint64_t num_records = total_records;
    uint64_t total_size = num_records*DATASIZE+num_records*sizeof(uint64_t);

    int record_size = DATASIZE+sizeof(uint64_t);
    attr_size[0] = 3;

    std::vector<int> attr_data;
    attr_data.push_back(total_records);
    attr_data.push_back(8);
    attr_data.push_back(DATASIZE);

    uint64_t block_size = num_events_recorded[myrank]*record_size;



    dims[0] = (hsize_t)(total_size);
    sid     = H5Screate_simple(1, dims, NULL);

    int ret = 0;
    dataset1 = H5Dcreate2(fid, DATASETNAME1, H5T_NATIVE_CHAR, sid, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    H5Sclose(sid);

    file_dataspace = H5Dget_space(dataset1);

    hsize_t offset = 0;
    for(int i=0;i<myrank;i++)
	    offset += (hsize_t)(num_events_recorded[i]*record_size);
    hsize_t block_count = (hsize_t)(block_size);

    ret = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,&offset,NULL,&block_count,NULL);
    //hid_t ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride, count, block);

    
    mem_dataspace = H5Screate_simple(1,&block_count, NULL);

    data_array1 = new std::vector<char> ();

    for(int i=0;i<myevents.size();i++)
    {
	event e = myevents[i];
	uint64_t key = e.ts;
	uint64_t mask = 255;
	int numchars = sizeof(uint64_t);
	for(int k=0;k<numchars;k++)
	{
	    uint64_t key_t = key&mask;
	    data_array1->push_back(key_t);
	    key = key >> 8;
	}
	for(int k=0;k<DATASIZE;k++)
		data_array1->push_back(e.data[k]);

    }

   xfer_plist = H5Pcreate(H5P_DATASET_XFER);

   ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);

   ret = H5Dwrite(dataset1, H5T_NATIVE_CHAR, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());

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
    myevents.clear();
}


void read_write_process::pread(const char *filename)
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

    std::vector<char> *data_array1 = nullptr;

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

    offset *= (k_size+data_size);

    int num_events = 0;
    if(myrank < rem) num_events = k_per_process+1;
    else num_events = k_per_process;

    hsize_t block_size = (hsize_t)(num_events*(data_size+k_size));

    data_array1 = new std::vector<char> ();

    data_array1->resize(block_size);

    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset,NULL,&block_size,NULL);
    mem_dataspace = H5Screate_simple(1,&block_size, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset1, H5T_NATIVE_CHAR, mem_dataspace, file_dataspace, xfer_plist, data_array1->data());
     
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    ret = H5Aclose(attr_id);
    ret = H5Dclose(dataset1);
    delete data_array1;

   H5Fclose(fid);  

}
