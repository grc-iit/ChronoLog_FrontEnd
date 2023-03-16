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


typedef int DATATYPE;

void read_write_process::create_events(int num_events)
{
    dm->clear_buffer();

    for(int i=0;i<num_events;i++)
    {
	event e;
	std::fill(e.data,e.data+DATASIZE,0);
	uint64_t ts = CM->Timestamp();
	int pid = writer_id(ts);

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

    int num_records = total_records;
    int total_size = num_records*DATASIZE+num_records*sizeof(uint64_t);

    int record_size = DATASIZE+sizeof(uint64_t);
    attr_size[0] = 3;

    std::vector<int> attr_data;
    attr_data.push_back(total_records);
    attr_data.push_back(8);
    attr_data.push_back(DATASIZE);

    int block_size = num_events_recorded[myrank]*record_size;

    std::cout <<" block_size = "<<block_size<<" total_size = "<<total_size<<std::endl;
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


void read_write_process::pread()
{

    hid_t       fid;                                              
    hid_t       acc_tpl;                                         
    hid_t       xfer_plist;                                       
    hid_t       file_dataspace;                                   
    hid_t       mem_dataspace;                                    
    hid_t       dataset1, dataset2, dataset5, dataset6, dataset7;
    DATATYPE *  data_array1  = NULL;                              
    DATATYPE *  data_origin1 = NULL;                             
    const char *filename;

    hsize_t start[RANK];               
    hsize_t count[RANK], stride[RANK]; 
    hsize_t block[RANK];               

    size_t   num_points;    
    hsize_t *coords = NULL; 
    int      i, j, k;


    filename = "file1.h5";

    coords     = (hsize_t *)malloc((size_t)DIM0 * (size_t)DIM1 * RANK * sizeof(hsize_t));

    data_array1 = (int *)malloc((size_t)DIM0 * (size_t)DIM1 * sizeof(int));
    data_origin1 = (int *)malloc((size_t)DIM0 * (size_t)DIM1 * sizeof(int));

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_fapl_mpio(fapl, MPI_COMM_WORLD, MPI_INFO_NULL);
    fid = H5Fopen(filename, H5F_ACC_RDONLY, fapl);

    hid_t ret = H5Pclose(fapl);

    dataset1 = H5Dopen2(fid, DATASETNAME1, H5P_DEFAULT);

    dataset2 = H5Dopen2(fid, DATASETNAME2, H5P_DEFAULT);
 
    block[0]  = (hsize_t)DIM0;
    block[1]  = (hsize_t)(DIM1 / numprocs);
    stride[0] = block[0];
    stride[1] = block[1];
    count[0]  = 1;
    count[1]  = 1;
    start[0]  = 0;
    start[1]  = (hsize_t)myrank * block[1];

    file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride, count, block);
    mem_dataspace = H5Screate_simple(RANK, block, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset1, H5T_NATIVE_INT, mem_dataspace, file_dataspace, xfer_plist, data_array1);
     
    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    block[0]  = (hsize_t)DIM0/numprocs;
    block[1]  = (hsize_t)DIM1;
    stride[0] = block[0];
    stride[1] = block[1];
    count[0]  = 1;
    count[1]  = 1;
    start[0]  = (hsize_t)myrank*block[0];
    start[1]  = 0;

    file_dataspace = H5Dget_space(dataset2);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start, stride, count, block);
    mem_dataspace = H5Screate_simple(RANK, block, NULL);
    xfer_plist = H5Pcreate(H5P_DATASET_XFER);
    ret = H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);
    ret = H5Dread(dataset2, H5T_NATIVE_INT, mem_dataspace, file_dataspace, xfer_plist, data_array1);


    H5Sclose(file_dataspace);
    H5Sclose(mem_dataspace);
    H5Pclose(xfer_plist);

    ret = H5Dclose(dataset1);
    ret = H5Dclose(dataset2);

   free(data_array1);
   free(data_origin1);
   free(coords);
   H5Fclose(fid);  

}
