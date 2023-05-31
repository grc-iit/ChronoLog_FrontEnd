#include "query_engine.h"

void query_engine::send_query(std::string &s)
{
       int count = query_number.fetch_add(1);
       struct query_req r;
       r.name = s;
       r.minkey = 0;
       r.maxkey = UINT64_MAX;
       r.collective = false;
       r.id = count;
       r.sorted = true;
       r.output_file = false;
       if(myrank==0)
       {
          Q->PutAll(r);
        }

}

void query_engine::sort_response(std::string &s,int id,std::vector<struct event> *buf,uint64_t &maxkey)
{
     int index1,index2;
     index1 = create_buffer(s,index2);

     boost::upgrade_lock<boost::shared_mutex> lk(sbuffers[index1]->m);
     {
	for(int i=0;i<buf->size();i++)
	    if((*buf)[i].ts > maxkey) sbuffers[index1]->buffer->push_back((*buf)[i]);
	ds->get_unsorted_data(sbuffers[index1]->buffer,index2);
	int tag = 10000+id;
	uint64_t minkey,maxkey;
	ds->sort_data(index2,tag,sbuffers[index1]->buffer->size(),minkey,maxkey);
	buf->assign(sbuffers[index1]->buffer->begin(),sbuffers[index1]->buffer->end());
	sbuffers[index1]->buffer->clear();
     }
}

bool query_engine::end_file_read(bool end_read,int id)
{
    int s_req = (end_read==true) ? 1 : 0;

    std::vector<int> r_req(numprocs);

    int tag = 10000+id;

    MPI_Request *reqs = (MPI_Request *)std::malloc(2*numprocs*sizeof(MPI_Request));

    int nreq = 0;

    for(int i=0;i<numprocs;i++)
    {
	MPI_Isend(&s_req,1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
	MPI_Irecv(&r_req[i],1,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
    }

    MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

    bool end_read_g = true;

    for(int i=0;i<numprocs;i++)
    {
	if(r_req[i]==0) end_read_g = false;
    }

    std::free(reqs);
    return end_read_g;
}
void query_engine::get_range(std::vector<struct event> *buf1,std::vector<struct event> *buf2,std::vector<struct event> *buf3,uint64_t minkeys[3],uint64_t maxkeys[3],int id)
{
    int tag = 10000+id;
    MPI_Request *reqs = (MPI_Request*)std::malloc(3*numprocs*sizeof(MPI_Request));

    std::vector<uint64_t> send_ts; send_ts.resize(6);
    std::vector<uint64_t> recv_ts; recv_ts.resize(6*numprocs);
   
    send_ts[0] = UINT64_MAX; send_ts[2] = UINT64_MAX; send_ts[4] = UINT64_MAX;
    send_ts[1] = 0; send_ts[3] = 0; send_ts[5] = 0;

    if(buf1 != nullptr && buf1->size()>0)
    {
	send_ts[0] = (*buf1)[0].ts;
	send_ts[1] = (*buf1)[buf1->size()-1].ts;
    }

    if(buf2 != nullptr && buf2->size()>0)
    {
	send_ts[2] = (*buf2)[0].ts;
	send_ts[3] = (*buf2)[buf2->size()-1].ts;
    }

    if(buf3 != nullptr && buf3->size()>0)
    {
	send_ts[4] = (*buf3)[0].ts;
	send_ts[5] = (*buf3)[buf3->size()-1].ts;
    }

    int nreq=0;

    for(int i=0;i<numprocs;i++)
    {
	MPI_Isend(send_ts.data(),6,MPI_UINT64_T,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
	MPI_Irecv(&recv_ts[6*i],6,MPI_UINT64_T,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
    }

    MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

    minkeys[0] = UINT64_MAX; minkeys[1] = UINT64_MAX; minkeys[2] = UINT64_MAX;
    maxkeys[0] = 0; maxkeys[1] = 0; maxkeys[2] = 0;

    for(int i=0;i<numprocs;i++)
    {
	if(minkeys[0] > recv_ts[6*i]) minkeys[0] = recv_ts[6*i];
	if(maxkeys[0] < recv_ts[6*i+1]) maxkeys[0] = recv_ts[6*i+1];
	if(minkeys[1] > recv_ts[6*i+2]) minkeys[1] = recv_ts[6*i+2];
	if(maxkeys[1] < recv_ts[6*i+3]) maxkeys[1] = recv_ts[6*i+3];
	if(minkeys[2] > recv_ts[6*i+4]) minkeys[2] = recv_ts[6*i+4];
	if(maxkeys[2] < recv_ts[6*i+5]) maxkeys[2] = recv_ts[6*i+5];
    }

     std::free(reqs);
}

void query_engine::service_query(struct thread_arg_q* t) 
{
	int end_service = 0;

	std::vector<std::string> names;
	names.push_back("table0");
	names.push_back("table1");
	names.push_back("table2");
	names.push_back("table3");

	bool sorted = false;

	int numrounds = 0;

	//usleep(10*128*20000);

	for(int n=0;n<numrounds;n++)
	{
	  for(int i=0;i<names.size();i++)
          {

	      uint64_t min_key1,max_key1;

	      bool b = false;

	      std::vector<struct event> *buf1 = new std::vector<struct event> ();
	      std::vector<struct event> *buf2 = new std::vector<struct event> ();
	      std::vector<struct event> *buf3 = new std::vector<struct event> ();
	      std::vector<struct event> *resp_vec = nullptr;


	      std::string filename = "file";
	      filename += names[i]+".h5";

	      usleep(128*20000);

	      uint64_t minkey_f,maxkey_f;

	      uint64_t minkey_r,maxkey_r;

	      uint64_t minkey_e = 0; 
	      uint64_t maxkey_e = UINT64_MAX;
	
		  
	      struct io_request *nq = new struct io_request();
	      nq->name.assign(names[i]);
	      nq->completed.store(0);
	      nq->buf1 = buf1;
	      nq->buf2 = buf2;
	      nq->buf3 = buf3;
	      rwp->sync_queue_push(nq);
	      while(!nq->completed.load()==1);

	      delete nq;

	      uint64_t minkeys[3],maxkeys[3];
	      
	      get_range(buf1,buf2,buf3,minkeys,maxkeys,100);

	      if(myrank==0)
              {
         	std::cout <<" id = "<<i<<" minkey 3 = "<<minkeys[2]<<" maxkey 3 = "<<maxkeys[2]<<std::endl;
         	std::cout <<" id = "<<i<<" minkey 2 = "<<minkeys[1]<<" maxkey 2 = "<<maxkeys[1]<<std::endl;
         	std::cout <<" id = "<<i<<" minkey 1 = "<<minkeys[0]<<" maxkey 1 = "<<maxkeys[0]<<std::endl;
     	      }


	      if(sorted)
	      {

		  uint64_t maxkey = std::max(maxkeys[1],maxkeys[2]);
		  sort_response(names[i],i,buf1,maxkey);
	      }
	        
	      resp_vec = new std::vector<struct event> ();

	      uint64_t minkey = UINT64_MAX;
	      uint64_t maxkey = 0;
	      if(buf3 != nullptr) 
	      {
		  resp_vec->assign(buf3->begin(),buf3->end()); buf3->clear();
	      }

	      if(buf2 != nullptr)
	      {
		  for(int i=0;i<buf2->size();i++)
	      	  {
		   if((*buf2)[i].ts > maxkeys[2]) 
	           {
		     resp_vec->push_back((*buf2)[i]);
		   }
		 }
		 buf2->clear();			
	      }

	      if(buf1 != nullptr)
	      {
		for(int i=0;i<buf1->size();i++)
		{
		   if((*buf1)[i].ts > maxkeys[2] && (*buf1)[i].ts > maxkeys[1])
		   {
			resp_vec->push_back((*buf1)[i]);
		   }
		}
		buf1->clear();
	       }

      	      /*struct query_resp *p = new struct query_resp();

	      p->response_vector = nullptr;
	      p->response_vector = resp_vec;

	      O->push(p);	 */   

	      delete buf1; 
	      delete buf2;
	      delete buf3;
	      delete resp_vec;
           }
	}
}

void query_engine::sort_file(std::string &s)
{

      std::string attrname = "attr"+std::to_string(0);
      std::string outputfile = hs->sort_on_secondary_key(s,attrname,0,0,UINT64_MAX);

}
