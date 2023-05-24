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

std::vector<struct event>* query_engine::sort_response_full(std::vector<struct event>* buf1,std::vector<struct event>* buf2,std::vector<struct event> *buf3,int tag,uint64_t maxkeys[3])
{
	std::vector<struct event> *result_vec = new std::vector<struct event> ();

	MPI_Datatype value_field;

	MPI_Type_contiguous(VALUESIZE,MPI_CHAR,&value_field);
	MPI_Type_commit(&value_field);

	struct event e;
  	MPI_Aint tdispl1[2];

        MPI_Get_address(&e,&tdispl1[0]);
        MPI_Get_address(&e.data,&tdispl1[1]);

        MPI_Aint base = tdispl1[0];
        MPI_Aint valuef = MPI_Aint_diff(tdispl1[1],base);

        MPI_Datatype key_value;
        int blocklens[2];
        MPI_Aint tdispl[2];
        int types[2];
  	blocklens[0] = 1;
        blocklens[1] = 1;
        tdispl[0] = 0;
        tdispl[1] = valuef;
        types[0] = MPI_UINT64_T;
        types[1] = value_field;

        MPI_Type_create_struct(2,blocklens,tdispl,types,&key_value);
        MPI_Type_commit(&key_value);
	std::vector<int> buf_counts_l,buf_counts;
	buf_counts_l.resize(3); buf_counts.resize(numprocs*3);
	buf_counts_l[0] = buf1 == nullptr ? 0 : buf1->size();
	buf_counts_l[1] = 0;
	if(buf2 != nullptr)
	{
	    for(int i=0;i<buf2->size();i++)
	    if((*buf2)[i].ts > maxkeys[2]) buf_counts_l[1]++;
	}

	buf_counts_l[2] = 0;
	if(buf3 != nullptr)
	{
	    for(int i=0;i<buf3->size();i++)
	     if((*buf3)[i].ts > maxkeys[2] && (*buf3)[i].ts > maxkeys[1]) buf_counts_l[2]++;
	}

	MPI_Request *reqs = (MPI_Request *)std::malloc(numprocs*3*sizeof(MPI_Request));

	int nreq = 0;
	for(int i=0;i<numprocs;i++)
	{
	   MPI_Isend(buf_counts_l.data(),3,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	   MPI_Irecv(&buf_counts[i*3],3,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	int file_events=0, nvevents =0, memevents = 0;

	for(int i=0;i<numprocs;i++)
	{
	   file_events += buf_counts[3*i];
	   nvevents += buf_counts[3*i+1];
	   memevents += buf_counts[3*i+2];
	}

	int total_events = file_events+nvevents+memevents;
	int events_per_proc = total_events/numprocs;
	int rem = total_events%numprocs;
	int offset = (events_per_proc+1)*rem;

	std::vector<int> dest1,dest2,dest3;
	std::vector<int> send_counts, recv_counts;
	send_counts.resize(3*numprocs); recv_counts.resize(3*numprocs);
        std::fill(send_counts.begin(),send_counts.end(),0);
	std::fill(recv_counts.begin(),recv_counts.end(),0);
	
	int start = 0;

	for(int i=0;i<myrank;i++) start += buf_counts[3*i];

	for(int i=0;i<buf_counts_l[0];i++)
	{
	   int c = start+i;
	   int dest_proc=-1;
	   if(c < offset) dest_proc = c/(events_per_proc+1); 
	   else dest_proc = rem+(c-offset)/events_per_proc;
	   dest1.push_back(dest_proc);
	   send_counts[3*dest_proc]++;
	}
		
	start = file_events;
	for(int i=0;i<myrank;i++) start += buf_counts[3*i+1];

	if(buf2 != nullptr)
	for(int i=0;i<buf2->size();i++)
	{
	   if((*buf2)[i].ts > maxkeys[2])
	   {	   
	     int c = start+i;
	     int dest_proc = -1;
	     if(c < offset) dest_proc = c/(events_per_proc+1);
	     else dest_proc = rem+(c-offset)/events_per_proc;
	     dest2.push_back(dest_proc);
	     send_counts[3*dest_proc+1]++;
	   }
	   else dest2.push_back(-1);
	}

	start = file_events+nvevents;

	for(int i=0;i<myrank;i++) start += buf_counts[3*i+2];

	if(buf3 != nullptr)
	for(int i=0;i<buf3->size();i++)
	{
           if((*buf3)[i].ts > maxkeys[2] && (*buf3)[i].ts > maxkeys[1])
	   {
	     int c = start+i;
	     int dest_proc=-1;
	     if(c < offset) dest_proc = c/(events_per_proc+1);
	     else dest_proc = rem+(c-offset)/events_per_proc;
	     dest3.push_back(dest_proc);
	     send_counts[3*dest_proc+2]++;
	   }
	   else dest3.push_back(-1);
	}

	nreq = 0;

	for(int i=0;i<numprocs;i++)
	{
	   MPI_Isend(&send_counts[3*i],3,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	   MPI_Irecv(&recv_counts[3*i],3,MPI_INT,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	std::vector<std::vector<struct event>> send_buffer, recv_buffer;

	send_buffer.resize(numprocs);
	recv_buffer.resize(numprocs);

	for(int i=0;i<buf_counts_l[0];i++)
	{	
	   int dest_proc = dest1[i];
	   if(dest_proc != -1)
	     send_buffer[dest_proc].push_back((*buf1)[i]);
	}

	for(int i=0;i<numprocs;i++)
	{	
	    if(recv_counts[3*i] > 0)
	    recv_buffer[i].resize(recv_counts[3*i]);
	}

	nreq = 0;
	for(int i=0;i<numprocs;i++)
	{
           if(send_counts[3*i]>0)
	   {
	     MPI_Isend(send_buffer[i].data(),send_counts[3*i],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	   if(recv_counts[3*i]>0)
	   {
	     MPI_Irecv(recv_buffer[i].data(),recv_counts[3*i],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	for(int i=0;i<numprocs;i++)
	{
	   send_buffer[i].clear(); 
	   if(buf1 != nullptr) buf1->clear();
	   for(int j=0;j<recv_buffer[i].size();j++) result_vec->push_back(recv_buffer[i][j]);
	   recv_buffer[i].clear();
	   if(recv_counts[3*i+1] > 0) recv_buffer[i].resize(recv_counts[3*i+1]);
	}

	if(buf2 != nullptr)
	for(int i=0;i<buf2->size();i++)
	{
	   int dest_proc = dest2[i];
	   if(dest_proc != -1)
	   send_buffer[dest_proc].push_back((*buf2)[i]);
	}

	nreq = 0;
	for(int i=0;i<numprocs;i++)
	{
           if(send_counts[3*i+1]>0)
	   {
	     MPI_Isend(send_buffer[i].data(),send_counts[3*i+1],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	   if(recv_counts[3*i+1]>0)
	   {
	     MPI_Irecv(recv_buffer[i].data(),recv_counts[3*i+1],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	for(int i=0;i<numprocs;i++)
	{
	   send_buffer[i].clear(); 
	   if(buf2 != nullptr) buf2->clear();
	   for(int j=0;j<recv_buffer[i].size();j++)
		 result_vec->push_back(recv_buffer[i][j]);
	   recv_buffer[i].clear();
	   if(recv_counts[3*i+2]>0) recv_buffer[i].resize(recv_counts[3*i+2]);
	}

	if(buf3 != nullptr)
	for(int i=0;i<buf3->size();i++)
	{
	   int dest_proc = dest3[i];
	   if(dest_proc != -1)
	   send_buffer[dest_proc].push_back((*buf3)[i]);
	}

	nreq = 0;
	for(int i=0;i<numprocs;i++)
	{
	   if(send_counts[3*i+2]>0)
	   {
	     MPI_Isend(send_buffer[i].data(),send_counts[3*i+2],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	   if(recv_counts[3*i+2]>0)
	   {
	     MPI_Irecv(recv_buffer[i].data(),recv_counts[3*i+2],key_value,i,tag,MPI_COMM_WORLD,&reqs[nreq]);
	     nreq++;
	   }
	}

	MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

	for(int i=0;i<numprocs;i++)
	{
	   send_buffer[i].clear();
	   if(buf3 != nullptr) buf3->clear();
	   for(int j=0;j<recv_buffer[i].size();j++)
		result_vec->push_back(recv_buffer[i][j]);
	   recv_buffer[i].clear();
	}

	MPI_Type_free(&key_value);
	MPI_Type_free(&value_field);
	std::free(reqs);

	return result_vec;
}

void query_engine::service_query(struct thread_arg_q* t) 
{
	int end_service = 0;

         while(true)
         {
              while(!Q->Empty())
             {
              struct query_req *r=nullptr;
              r = Q->Get();

	      if(r==nullptr || r->name.compare("endsession")==0) 
	      {
		      if(r != nullptr) 
		      {
			  delete r;
			  end_session.store(1);
		      }
		      break;
	      }

	      uint64_t min_key1,max_key1;

	      bool b = false;
	      //rwp->get_range_in_write_buffers(r->name,min_key1,max_key1);

	      /*uint64_t min_key2, max_key2;

	      b = rwp->get_range_in_read_buffers(r->name,min_key2,max_key2);
	    */

	      std::vector<struct event> *buf1 = new std::vector<struct event> ();
	      std::vector<struct event> *buf2 = new std::vector<struct event> ();
	      std::vector<struct event> *buf3 = new std::vector<struct event> ();
	      std::vector<struct event> *resp_vec = nullptr;

	      std::string filename = "file";
	      filename += r->name+".h5";

	      uint64_t minkey_f,maxkey_f;

	      uint64_t minkey_e = r->minkey; uint64_t maxkey_e = r->maxkey;
	
	      int num_tries = 0;

	      while(true)
	      {
	        bool file_exists = rwp->file_existence(filename);
		bool end_read = false;
		uint64_t minkey_fp = UINT64_MAX;
		uint64_t maxkey_fp = 0;

	        if(file_exists && minkey_e <= maxkey_e)
	        {
		  minkey_f = UINT64_MAX; maxkey_f = 0;
		  b = rwp->preaddata(filename.c_str(),r->name,minkey_e,maxkey_e,minkey_f,maxkey_f,buf3);

		  if(b)
		  {
		    if(maxkey_f >= maxkey_e) 
		    {
			minkey_e = UINT64_MAX; maxkey_e = 0;
			end_read = true;
		    } 
		    else
		    {
			if(minkey_e < maxkey_e) minkey_e = maxkey_f+1;
			else end_read = true;
		    }
		  }
	        }
	        else 
		{
		   if(!file_exists) num_tries++;
		   if(minkey_fp == minkey_f && maxkey_fp==maxkey_f) num_tries++;
		}	
	        rwp->get_nvme_buffer(buf2,r->name);
		if(buf2->size() > 0 || end_read || num_tries > 1) break;
	      }

              atomic_buffer *au = nullptr;

              au = rwp->get_write_buffer(r->name);

              int size1 = 0;
              if(au != nullptr)
              {

                boost::shared_lock<boost::shared_mutex> lk(au->m);
                {
                    size1 = au->buffer_size.load();
	            if(size1 > 0) 
		    {
			buf1->resize(size1);
			for(int i=0;i<size1;i++)
		          (*buf1)[i] = (*(au->buffer))[i];
		    }
                }

	      }

	      uint64_t minkeys[3],maxkeys[3];
	      
	      get_range(buf1,buf2,buf3,minkeys,maxkeys,r->id);

	      if(myrank==0)
              {
         	std::cout <<" id = "<<r->id<<" minkey 3 = "<<minkeys[2]<<" maxkey 3 = "<<maxkeys[2]<<std::endl;
         	std::cout <<" id = "<<r->id<<" minkey 2 = "<<minkeys[1]<<" maxkey 2 = "<<maxkeys[1]<<std::endl;
         	std::cout <<" id = "<<r->id<<" minkey 1 = "<<minkeys[0]<<" maxkey 1 = "<<maxkeys[0]<<std::endl;
     	      }


	      if(r->sorted)
	      {

		  uint64_t maxkey = std::max(maxkeys[1],maxkeys[2]);
		  sort_response(r->name,r->id,buf1,maxkey);
	          resp_vec = sort_response_full(buf3,buf2,buf1,10000+r->id,maxkeys);
	      }
	      else
	      {
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
	      }

      	      struct query_resp *p = new struct query_resp();

	      p->response_vector = nullptr;
	      p->response_vector = resp_vec;

	      O->push(p);	      

	      if(buf1 != nullptr) delete buf1; 
	      if(buf2 != nullptr) delete buf2;
	      if(buf3 != nullptr) delete buf3;
              delete r;
             }

             if(Q->Empty() && end_session.load()==1)
	     {
		break;
	     }
           }

	   //workers[t->tid].join();
}

