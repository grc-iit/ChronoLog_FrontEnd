#include <hdf5.h>
#include <vector>
#include <string>
#include <thread>
#include <iostream>

#define DATASETNAME1 "Data1"
#define DATASETNAME2 "Data2"
#define DATASETNAME3 "Data3"
#define DATASETNAME4 "Data4"
#define DATASETNAME5 "Data5"
#define DATASETNAME6 "Data6"
#define DATASETNAME7 "Data7"
#define DATASETNAME8 "Data8"
#define DATASETNAME9 "Data9"

struct thread_arg
{
int tid;
};

void pread(struct thread_arg *t)
{
    hid_t       fid;
    hid_t       acc_tpl;
    hid_t       xfer_plist;
    hid_t       sid;
    hid_t       dataset1, dataset2, dataset3, dataset4;
    hid_t       dataset5, dataset6, dataset7;

    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);

    int id = t->tid;
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

void thread_work(struct thread_arg *t)
{


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
	std::thread t{thread_work,&t_args[i]};
	workers[i] = std::move(t);
  }

  for(int i=0;i<num_threads;i++)
	  workers[i].join();

  auto t2 = std::chrono::high_resolution_clock::now();

  double t = std::chrono::duration<double> (t2-t1).count();

  std::cout <<" num_threads = "<<num_threads<<" write time = "<<t<<std::endl;

H5close();

}
