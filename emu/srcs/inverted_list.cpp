#include "inverted_list.h"

template<typename T,class hashfcn=std::hash<T>>
int hdf5_invlist::partition_no(T &k)
{

      uint64_t hashval = hashfcn()(k);
      int np = nearest_power_two(numprocs);   
      int nbits = log2(np);
      uint64_t key = hashval;
      key = key >> (64-nbits);   
      int id = (int)key;
      return id;
}

template<typename T>
void hdf5_invlist::create_invlist(std::string &s,int maxsize)
{

	struct head_node *h  = new struct head_node();

	h->maxsize = maxsize;
	h->inttable = nullptr;
	h->floattable = nullptr;
	h->doubletable = nullptr;

	if(std::is_same<T,int>::value)
	{
	     h->inttable = new struct invnode<int,int> ();
	     h->inttable->ml = new memory_pool<int,int> (100);
	     h->inttable->bm = new BlockMap<int,int> (maxsize,h->inttable->ml,INT_MAX); 
	}

	else if(std::is_same<T,float>::value)
	{
	   h->floattable = new struct invnode<float,int> ();
	   h->floattable->ml = new memory_pool<float,int> (100);
	   h->floattable->bm = new BlockMap<float,int> (maxsize,h->floattable->ml,DBL_MAX);
	}
	else if(std::is_same<T,double>::value)
	{
	   h->doubletable = new struct invnode<double,int> ();
	   h->doubletable->ml = new memory_pool<double,int> (100);
	   h->doubletable->bm = new BlockMap<double,int> (maxsize,h->doubletable->ml,DBL_MAX);
	}

	std::pair<std::string,struct head_node*> p;
	p.first.assign(s);
	p.second = h;
	invlists.insert(p);

}


template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
void hdf5_invlist::fill_invlist_from_file(std::string &s,int offset)
{
   std::string filename = "file";
   filename += s+".h5";

   hid_t xfer_plist = H5Pcreate(H5P_DATASET_XFER);
   hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
   H5Pset_fapl_mpio(fapl,merge_comm, MPI_INFO_NULL);
   H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE);

   hsize_t chunkdims[1];
   chunkdims[0] = 8192;
   hsize_t maxdims[1];
   maxdims[0] = (hsize_t)H5S_UNLIMITED;

   hid_t dataset_pl = H5Pcreate(H5P_DATASET_CREATE);

   int ret = H5Pset_chunk(dataset_pl,1,chunkdims);

   hid_t fid = H5Fopen(filename2.c_str(),H5F_ACC_RDWR,fapl);     

   hsize_t attr_size[1];
   attr_size[0] = MAXBLOCKS*4+4;
   const char *attrname[1];
   hid_t attr_space[1];
   attr_space[0] = H5Screate_simple(1, attr_size, NULL);

   attrname[0] = "Datasizes";

   std::string data_string = "Data1";
   dataset1 = H5Dopen2(fid,data_string.c_str(), H5P_DEFAULT);

   hid_t attr_id = H5Aopen(dataset1,attrname[0],H5P_DEFAULT);
   std::vector<uint64_t> attrs;
   attrs.resize(attr_size[0]);

   ret = H5Aread(attr_id,H5T_NATIVE_UINT64,attrs.data());

    hsize_t adims[1];
    adims[0] = VALUESIZE;
    hid_t s1 = H5Tarray_create(H5T_NATIVE_CHAR,1,adims);
    hid_t s2 = H5Tcreate(H5T_COMPOUND,sizeof(struct event));
    H5Tinsert(s2,"key",HOFFSET(struct event,ts),H5T_NATIVE_UINT64);
    H5Tinsert(s2,"value",HOFFSET(struct event,data),s1);

    int numblocks = attrs[3];

    hid_t file_dataspace = H5Dget_space(dataset1);
    ret = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,&offset_f,NULL,&blocksize,NULL);
    mem_dataspace = H5Screate_simple(1,&blocksize, NULL);
    ret = H5Dread(dataset1,s2, mem_dataspace, file_dataspace, xfer_plist,inp->data());


    std::vector<struct event> *buffer = new std::vector<struct event> ();

    int pos = 4;
    hsize_t offset_r = 0;
    for(int i=0;i<numblocks;i++)
    {
	int nrecords = attrs[pos+i*4+3];
	
	int records_per_proc = nrecords/numprocs;
	int rem = nrecords%numprocs;

	hsize_t pre = offset_r;
	for(int i=0;i<myrank;i++)
	{	
	   int size_p=0;
	   if(i < rem) size_p = records_per_proc+1;
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

	add_entries_to_tables<T,hashfcn,equalfcn>(s,buffer,pre,offset);

	offset_r += nrecords;

    }

    std::vector<std::vector<T>> keys;
    std::vector<std::vector<int>> values;

    int key_pre=0;
    int totalkeys=0;

    get_entries_from_tables<T,hashfcn,equalfcn>(s,keys,values,key_pre,totalkeys);

    struct intkey{int key;int index;};
    struct floatkey{float key;int index;};
    struct doublekey{double key;int index;};

    hid_t kv1 = H5Tcreate(H5T_COMPOUND,sizeof(struct intkey));
    H5Tinsert(kv1,"key",HOFFSET(struct intkey,key),H5T_INT);
    H5Tinsert(kv1,"index",HOFFSET(struct intkey,index),H5T_INT);

    hid_t kv2 = H5Tcreate(H5T_COMPOUND,sizeof(struct floatkey));
    H5Tinsert(kv2,"key",HOFFSET(struct floatkey,key),H5T_FLOAT);
    H5Tinsert(kv2,"index",HOFFSET(struct floatkey,index),H5T_INT);

    hid_t kv3 = H5Tcreate(H5T_COMPOUND,sizeof(struct doublekey));
    H5Tinsert(kv3,"key",HOFFSET(struct doublekey,key),H5T_DOUBLE);
    H5Tinsert(kv3,"index",HOFFSET(struct doublekey,index),H5_INT);

    std::vector<struct intkey> *buf1 = nullptr;
    std::vector<struct floatkey> *buf2 = nullptr;
    std::vector<struct doublekey> *buf3 = nullptr;

    if(h->keytype==0)
	buf1 = new std::vector<struct intkey> ();
    else if(h->keytype==1)
	 buf2 = new std::vector<struct floatkey> ();
    else if(h->keytype==2)
	 buf3 = new std::vector<struct doublekey> ();

    for(int i=0;i<keys.size();i++)
    {
	for(int j=0;j<keys[i].size();j++)
	{	
		struct intkey nk;
		nk.key = keys[i][j];
		nk.index = values[i][j];
		if(buf1 != nullptr) buf1->push_back(nk);
		else if(buf2 != nullptr) buf2->push_back(nk);
		else if(buf3 != nullptr) buf3->push_back(nk);
	 }
    }

    hsize_t blockcount = 0;
    if(buf1 != nullptr) blockcount = buf1->size();
    else if(buf2 != nullptr) blockcount = buf2->size();
    else if(buf3 != nullptr) blockcount = buf3->size();

    std::string kv_string = "key_index";
    hsize_t totalkeys_t = (hsize_t)totalkeys;
    hid_t file_dataspace2 = H5Screate_simple(1,&totalkeys_t,maxdims);
    hid_t mem_dataspace2 = H5Screate_simple(1,&blockcount, NULL);
    ret = H5Sselect_hyperslab(file_dataspace2,H5S_SELECT_SET,&offsetf2,NULL,&blockcount,NULL);
    if(h->keytype==0)
    {
       hid_t dataset2 = H5Dcreate(fid,kv_string.c_str(),kv1,file_dataspace2, H5P_DEFAULT,dataset_pl,H5P_DEFAULT);
       ret = H5Dwrite(dataset2,kv1, mem_dataspace2,file_dataspace2,xfer_plist,buf1->data());
    }
    else if(h->keytype==1)
    {
       hid_t dataset2 = H5Dcreate(fid,kv_string.c_str(),kv2,file_dataspace2, H5P_DEFAULT,dataset_pl,H5P_DEFAULT);
       ret = H5Dwrite(dataset2,kv2, mem_dataspace2,file_dataspace2,xfer_plist,buf2->data());


    }
    else if(h->keytype==2)
    {
       hid_t dataset2 = H5Dcreate(fid,kv_string.c_str(),kv3,file_dataspace2, H5P_DEFAULT,dataset_pl,H5P_DEFAULT);
       ret = H5Dwrite(dataset2,kv3, mem_dataspace2,file_dataspace2,xfer_plist,buf3->data());
    }


    delete buffer;
    H5Tclose(kv1);
    H5Tclose(kv2);
    H5Tclose(kv3);
    H5Tclose(s2);
    H5Tclose(s1);
   H5Sclose(attr_space[0]);
   H5Dclose(dataset1);
   H5Aclose(attr_id);
   H5Fclose(fid);

}

template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
void hdf5_invlist::add_entries_to_tables(std::string &s,std::vector<struct event> *buffer,int f_offset,int offset)
{
  std::vector<int> send_count,recv_count;

  send_count.resize(numprocs); recv_count.resize(numprocs);
  std::fill(send_count.begin(),send_count.end(),0);
  std::fill(recv_count.begin(),recv_count.end(),0); 

  MPI_Request *reqs = (MPI_Request *)std::malloc(2*numprocs*sizeof(MPI_Request));

  int nreq = 0;
  
  std::vector<std::vector<double>> send_buffers;
  std::vector<std::vector<double>> recv_buffers;
  send_buffers.resize(numprocs); recv_buffers.resize(numprocs);

  int recsize = sizeof(struct event);  
  int offsets = f_offset;

  for(int i=0;i<buffer->size();i++)
  {
     T key = *(T*)((*buffer)[i].data+offset);
     int p = partition_no<T,hashfcn>(key); 
     send_count[p]+=2;
     send_buffers[p].push_back((double)key);
     send_buffers[p].push_back(offsets);
     offsets += recsize;
  }

  for(int i=0;i<numprocs;i++)
  {
     MPI_Isend(&send_count[i],1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
     nreq++;
     MPI_Irecv(&recv_count[i],1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
     nreq++;
  }

  MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

  for(int i=0;i<numprocs;i++)
  {
     recv_buffers[i].resize(recv_count[i]);
  }

  nreq = 0;
  for(int i=0;i<numprocs;i++)
  {
    
    MPI_Isend(send_buffers[i].data(),send_count[i],MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
    nreq++;
    MPI_Irecv(recv_buffers[i].data(),recv_count[i],MPI_DOUBLE,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
    nreq++;
  }

  MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

  auto r = invlists.find(s);

  struct head_node *h = r->second; 

  struct invnode<int,int> *table1 = nullptr;
  struct invnode<float,int> *table2 = nullptr;
  struct invnode<double,int> *table3 = nullptr;

  if(h->keytype==0)
     table1 = h->inttable;
  else if(h->keytype==1) table2 = h->floattable;
  else if(h->keytype==2) table3 = h->doubletable;


  for(int i=0;i<numprocs;i++)
  {
	for(int j=0;j<recv_buffers[i].size();j+=2)
	{
	   T key = (T)(recv_buffers[i][j]);
	   int offset = (int)recv_buffers[i][j+1];
           if(h->keytype==0) table1->bm->insert(key,offset);
	   else if(h->keytype==1) table2->bm->insert(key,offset);
	   else if(h->keytype==2) table3->bm->insert(key,offset);
	}
  }


  std::free(reqs);
}

template<typename T,class hashfcn=std::hash<T>,class equalfcn=std::equal_to<T>>
void hdf5_invlist::get_entries_from_tables(std::string &s,std::vector<std::vector<T>>& keys,std::vector<std::vector<int>> &offsets,int &key_b,int &numkeys)
{

	auto r = invlists.find(s);

	struct head_node *h = r->second;

	struct invnode<int,int> *table1 = nullptr;
        struct invnode<float,int> *table2 = nullptr;
        struct invnode<double,int> *table3 = nullptr;	

	if(h->keytype==0)
	{
	   table1 = h->inttable;
	   table1->bm->get_map_keyvalue(keys,offsets);
	}
	else if(h->keytype==1) 
	{
	   table2 = h->floattable;
	   table2->bm->get_map_keyvalue(keys,offsets);
	}
	else if(h->keytype==2) 
	{
	   table3 = h->doubletable;
	   table3->bm->get_map_keyvalue(keys,offsets);
	}

	MPI_Request *reqs = (MPI_Request *)std::malloc(2*numprocs*sizeof(MPI_Request));
	int nreq = 0;

	int numentries = 0;
	for(int i=0;i<keys.size();i++)
		numentries += keys[i].size();
	numentries = 2*numentries;

	std::vector<int> recv_counts(numprocs);
	int send_count = numentries;

	for(int i=0;i<numprocs;i++)
	{
	   MPI_Isend(&send_count,1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	   MPI_Irecv(&recv_counts[i],1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	numkeys = 0;
	for(int i=0;i<recv_counts.size();i++)
	   numkeys += recv_counts[i];

	key_b = 0;
	for(int i=0;i<myrank;i++)
	  key_b += recv_counts[i];


	std::free(reqs);
}
