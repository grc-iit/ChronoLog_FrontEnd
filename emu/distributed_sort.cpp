#include "distributed_sort.h"
#include <string.h>
#include <algorithm>

bool compare_fn(struct event &e1, struct event &e2) 
{
    return e1.ts <= e2.ts;
}

void dsort::sort_data(int index,uint64_t& min_v,uint64_t &max_v)
{

   int total_events = 0;

   int local_events = events[index]->size();
   
   std::vector<uint64_t> mysplitters;
   if(local_events >= 2)
   {
     int r1 = random()%local_events;

     int r2 = r1;
   
     do
     {
	r2 = random()%local_events;
     }while(r2==r1);
   
     mysplitters.push_back((*events[index])[r1].ts);
     mysplitters.push_back((*events[index])[r2].ts);
   }

   std::vector<int> splitter_counts(numprocs);
   std::fill(splitter_counts.begin(),splitter_counts.end(),0);
   std::vector<int> splitter_counts_l(numprocs);

   splitter_counts_l[myrank] = mysplitters.size();

   MPI_Request *reqs = (MPI_Request *)std::malloc(3*numprocs*sizeof(MPI_Request));
   MPI_Status *stats = (MPI_Status *)std::malloc(3*numprocs*sizeof(MPI_Status));

   int nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(&splitter_counts_l[myrank],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&splitter_counts[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   int num_splitters = 0;
   for(int i=0;i<numprocs;i++) num_splitters += splitter_counts[i];

   if(myrank==0)
   std::cout <<" num_splitters = "<<num_splitters<<" index = "<<index<<std::endl;

   std::vector<uint64_t> splitters;
   splitters.resize(num_splitters);

   std::vector<int> displ(numprocs);
   std::fill(displ.begin(),displ.end(),0);

   for(int i=1;i<numprocs;i++)
	   displ[i] = displ[i-1]+splitter_counts[i-1];

   nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(mysplitters.data(),splitter_counts[myrank],MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&splitters[displ[i]],splitter_counts[i],MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   std::sort(splitters.begin(),splitters.end());

   int splitters_per_proc =  num_splitters/numprocs;
   int rem = num_splitters%numprocs;
   int offset = rem*(splitters_per_proc+1);

   mysplitters.clear();

   std::vector<int> procs;

   for(int i=0;i<splitters.size();i++)
   {
	int proc=-1;
	if(i < offset) 
	{
	   proc = i/(splitters_per_proc+1);
	}
	else proc = rem+((i-offset)/splitters_per_proc);
	procs.push_back(proc);
   }

   std::vector<int> send_counts(numprocs);
   std::vector<int> recv_counts(numprocs);
   std::vector<int> recv_displ(numprocs);
   std::vector<int> send_displ(numprocs);
   std::fill(send_counts.begin(),send_counts.end(),0);
   std::fill(send_displ.begin(),send_displ.end(),0);
   std::fill(recv_counts.begin(),recv_counts.end(),0);
   std::fill(recv_displ.begin(),recv_displ.end(),0);

   std::vector<int> event_dest;

   std::vector<int> event_count(numprocs);
   std::fill(event_count.begin(),event_count.end(),0);

   for(int i=0;i<events[index]->size();i++)
   {
	int dest = -1;
        uint64_t ts = (*events[index])[i].ts;
	for(int j=0;j<splitters.size();j++)
	{
	    if(ts <= splitters[j])
	    {
		 dest = procs[j]; break;
	    }
	}
	if(dest == -1) dest = procs[splitters.size()-1];
	send_counts[dest]++;
	event_dest.push_back(dest);	
   }

   for(int i=1;i<numprocs;i++)
	send_displ[i] = send_displ[i-1]+send_counts[i-1];

   std::vector<uint64_t> send_buffer_u;
   std::vector<uint64_t> recv_buffer_u;
   std::fill(recv_displ.begin(),recv_displ.end(),0);

   nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(&send_counts[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&recv_counts[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   int total_recv_size = 0;
   for(int i=0;i<numprocs;i++)
	   total_recv_size += recv_counts[i];

   int total_records;

   std::vector<int> recv_sizes(numprocs);
   std::fill(recv_sizes.begin(),recv_sizes.end(),0);

   nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(&total_recv_size,1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&recv_sizes[i],1,MPI_INT,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   total_records = 0;
   for(int i=0;i<numprocs;i++) total_records += recv_sizes[i];

   send_buffer_u.resize(events[index]->size());
   recv_buffer_u.resize(total_recv_size);

   int datasize = VALUESIZE;
   for(int i=0;i<events[index]->size();i++)
   {
	uint64_t ts = (*events[index])[i].ts;
	int dest = event_dest[i];
	send_buffer_u[send_displ[dest]] = ts;
	send_displ[dest]++;
   }

   std::fill(send_displ.begin(),send_displ.end(),0);

   for(int i=1;i<numprocs;i++)
	   send_displ[i] = send_displ[i-1]+send_counts[i-1];

   for(int i=1;i<numprocs;i++)
	   recv_displ[i] = recv_displ[i-1]+recv_counts[i-1];

   nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(&send_buffer_u[send_displ[i]],send_counts[i],MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;

   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&recv_buffer_u[recv_displ[i]],recv_counts[i],MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   std::vector<int> key_counts;
   key_counts.assign(recv_counts.begin(),recv_counts.end());

   std::vector<int> key_displ;
   key_displ.assign(recv_displ.begin(),recv_displ.end());
	   
   std::vector<char> send_buffer_char;
   std::vector<char> recv_buffer_char;

   for(int i=0;i<numprocs;i++)
	   send_counts[i] *= datasize;
   for(int i=0;i<numprocs;i++)
	   recv_counts[i] *= datasize;

   std::fill(send_displ.begin(),send_displ.end(),0);
   std::fill(recv_displ.begin(),recv_displ.end(),0);

   for(int i=1;i<numprocs;i++)
	   send_displ[i] = send_displ[i-1]+send_counts[i-1];

   uint64_t total_recv_size_data = 0;
   for(int i=0;i<numprocs;i++) total_recv_size_data += recv_counts[i];

   uint64_t total_send_size_data = 0;
   for(int i=0;i<numprocs;i++) total_send_size_data += send_counts[i];

   send_buffer_char.resize(total_send_size_data);
   recv_buffer_char.resize(total_recv_size_data);

   for(int i=0;i<events[index]->size();i++)
   {
	int dest = event_dest[i];
	int start = send_displ[dest];
	memcpy(send_buffer_char.data()+start,(*events[index])[i].data,datasize);
	send_displ[dest]+=datasize;
   }

   std::fill(send_displ.begin(),send_displ.end(),0);

   for(int i=1;i<numprocs;i++)
	   send_displ[i] = send_displ[i-1]+send_counts[i-1];

   for(int i=1;i<numprocs;i++)
	   recv_displ[i] = recv_displ[i-1]+recv_counts[i-1];

   nreq = 0;
   for(int i=0;i<numprocs;i++)
   {
	MPI_Isend(&send_buffer_char[send_displ[i]],send_counts[i],MPI_CHAR,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   for(int i=0;i<numprocs;i++)
   {
	MPI_Irecv(&recv_buffer_char[recv_displ[i]],recv_counts[i],MPI_CHAR,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	nreq++;
   }

   MPI_Waitall(nreq,reqs,MPI_STATUS_IGNORE);

   events[index]->clear();

   for(int i=0;i<numprocs;i++)
   {
	   for(int j=0,k=0;j<key_counts[i];j++,k+=datasize)
	   {
		struct event e;   
		e.ts = recv_buffer_u[key_displ[i]+j];
		memcpy(e.data,&(recv_buffer_char[recv_displ[i]+k]),datasize);
		events[index]->push_back(e);
	   }
   }
   std::sort(events[index]->begin(),events[index]->end(),compare_fn);
   
   uint64_t min_ts, max_ts;
   
   nreq=0;
   if(myrank==0)
   {
       min_ts = (*events[index])[0].ts;

       for(int i=0;i<numprocs;i++)
       {
	 MPI_Isend(&min_ts,1,MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	 nreq++;
       }
   }
   if(myrank==numprocs-1)
   {
	int n = events[index]->size();
	max_ts = (*events[index])[n-1].ts;
	for(int i=0;i<numprocs;i++)
	{
	   MPI_Isend(&max_ts,1,MPI_UINT64_T,i,index,MPI_COMM_WORLD,&reqs[nreq]);
	   nreq++;
	}
   }

   MPI_Irecv(&min_v,1,MPI_UINT64_T,0,index,MPI_COMM_WORLD,&reqs[nreq]);
   nreq++;
   MPI_Irecv(&max_v,1,MPI_UINT64_T,numprocs-1,index,MPI_COMM_WORLD,&reqs[nreq]);
   nreq++;

   MPI_Waitall(nreq,reqs,stats);

   free(reqs); free(stats);   
}
