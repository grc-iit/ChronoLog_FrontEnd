#include <hdf5.h>
#include <vector>
#include <string>
#include <thread>
#include <iostream>

struct thread_arg
{
int tid;
};

void pcreate(struct thread_arg *t)
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

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    std::string dsetname = "dataset1";

    std::string filename = "file"+std::to_string(t->tid)+".h5";

    hid_t file = H5Fcreate(filename.c_str(),H5F_ACC_TRUNC|H5F_ACC_SWMR_WRITE,H5P_DEFAULT,fapl);

    H5Pclose(fapl);

    hsize_t total_ints = 500000000;
    hid_t filespace = H5Screate_simple(1,&total_ints,NULL);

    hid_t dset = H5Dcreate2(file,dsetname.c_str(),H5T_NATIVE_UINT,filespace,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);

    H5Dclose(dset);
    H5Sclose(filespace);
    H5Fclose(file);
}

void pwrite(struct thread_arg *t)
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

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    std::string dsetname = "dataset1";

    std::string filename = "file"+std::to_string(t->tid)+".h5";

    hid_t file = H5Fopen(filename.c_str(),H5F_ACC_RDWR|H5F_ACC_SWMR_WRITE,fapl);

    H5Pclose(fapl);

    hid_t dset = H5Dopen2(file,"dataset1",H5P_DEFAULT);

    hsize_t block_size = 500000000;

    hid_t filespace =  H5Screate_simple(1,&block_size,NULL);

    hid_t memspace = H5Screate_simple(1,&block_size,NULL);

    std::vector<uint32_t> *data_array = new std::vector<uint32_t> ();

    data_array->resize(block_size);

    hsize_t offset = 0;
    int ret = H5Sselect_hyperslab(filespace, H5S_SELECT_SET,&offset, NULL,&block_size, NULL);

    ret = H5Dwrite(dset,H5T_NATIVE_UINT,memspace,filespace,H5P_DEFAULT,data_array->data());


    H5Dclose(dset);
    H5Sclose(filespace);
    H5Sclose(memspace);

    H5Fclose(file);
    delete data_array;

}

void pread(struct thread_arg *t)
{
    hid_t       fid;
    hid_t       acc_tpl;
    hid_t       xfer_plist;
    hid_t       sid;
    hid_t       dataset1, dataset2, dataset3, dataset4;
    hid_t       dataset5, dataset6, dataset7;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);

    int id = t->tid-4;
    std::string filename = "file"+std::to_string(id)+".h5";

    fid = H5Fopen(filename.c_str(),H5F_ACC_RDONLY|H5F_ACC_SWMR_READ,fapl);

    if(fid < 0) return;


    hid_t dataset = H5Dopen2(fid,"dataset1",H5P_DEFAULT);

    hsize_t total_ints = 500000000;

    hid_t filespace = H5Screate_simple(1,&total_ints,NULL);
    hsize_t block_size = total_ints;
    hid_t memspace = H5Screate_simple(1,&block_size,NULL);
	    //H5Dget_space(dataset);

    hsize_t offset = 0;
    int ret = H5Sselect_hyperslab(filespace, H5S_SELECT_SET,&offset, NULL,&block_size, NULL);

    std::vector<uint32_t> *rdata = new std::vector<uint32_t> ();

    rdata->resize(total_ints);

    ret = H5Dread(dataset,H5T_NATIVE_UINT,memspace,filespace,H5P_DEFAULT,rdata->data());


    H5Dclose(dataset);
    H5Sclose(filespace);
    H5Sclose(memspace);
    H5Pclose(fapl);

    H5Fclose(fid);

}

void thread_create(struct thread_arg *t)
{
   if(t->tid/4==0) pcreate(t);

}

void thread_work(struct thread_arg *t)
{


  if(t->tid/4==0) pwrite(t);
  if(t->tid/4==1)
  pread(t);


}

int main(int argc,char **argv)
{

H5open();

hbool_t is_ts = false;

H5is_library_threadsafe(&is_ts);
if(is_ts)
std::cout <<" is_ts = "<<is_ts<<std::endl;

int num_threads = 4;

std::vector<struct thread_arg> t_args(num_threads);
std::vector<std::thread> workers(num_threads);

auto t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	t_args[i].tid = i;
	std::thread t{thread_create,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();
auto t2 = std::chrono::high_resolution_clock::now();

auto ctime = std::chrono::duration<double>(t2-t1).count();

std::cout <<" ctime = "<<ctime<<std::endl;

t1 = std::chrono::high_resolution_clock::now();

  for(int i=0;i<num_threads;i++)
  {
	std::thread t{thread_work,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();
  
t2 = std::chrono::high_resolution_clock::now();

double t = std::chrono::duration<double> (t2-t1).count();

  std::cout <<" num_threads = "<<num_threads<<" write time = "<<t<<std::endl;

H5close();

}
