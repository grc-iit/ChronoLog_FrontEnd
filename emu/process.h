#ifndef __PROCESS_H_
#define __PROCESS_H_

#include "mds.h"
#include "rw.h"
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "client.h"

class emu_process
{

private:
      int numprocs;
      int myrank;
      read_write_process *rwp;
      ClockSynchronization<ClocksourceCPPStyle> *CM;
      std::string server_addr;
      metadata_server *MS; 	
      metadata_client *MC;
public:

      emu_process(int np,int r) : numprocs(np), myrank(r)
      {
	std::string unit = "microsecond";
	CM = new ClockSynchronization<ClocksourceCPPStyle> (myrank,numprocs,unit);
	rwp = new read_write_process(r,np,CM);
	int nchars;
	std::vector<char> addr_string;

	if(myrank==0)
	{
	  char processor_name[1024];
          int len = 0;
          MPI_Get_processor_name(processor_name, &len);
	  std::string myhostname;
          myhostname.assign(processor_name);
          char ip[16];
          struct hostent *he = gethostbyname(myhostname.c_str());
          auto **addr_list = (struct in_addr **) he->h_addr_list;
          strcpy(ip, inet_ntoa(*addr_list[0]));
	  std::string myipaddr;
          myipaddr.assign(ip);
	  nchars = myipaddr.length();
	  for(int i=0;i<myipaddr.length();i++)
		  addr_string.push_back(myipaddr[i]);
	}
	
	MPI_Bcast(&nchars,1,MPI_INT,0,MPI_COMM_WORLD);

	if(myrank != 0) addr_string.resize(nchars);

	MPI_Bcast(addr_string.data(),nchars,MPI_CHAR,0,MPI_COMM_WORLD);
	std::string addr_ip(addr_string.data());

	int port_no = 1234;
	server_addr = "ofi+sockets://"+addr_ip+":"+std::to_string(port_no);

	MS = nullptr;
	if(myrank==0)
	{
	   MS = new metadata_server(numprocs,myrank,server_addr);
	   MS->bind_functions();
	}	
	MPI_Barrier(MPI_COMM_WORLD);
        MC = new metadata_client(server_addr);
      }

      metadata_client *getclientobj()
      {
	return MC;
      }
      void synchronize()
      {
	CM->SynchronizeClocks();
	CM->ComputeErrorInterval();

      }

      std::string & get_serveraddr()
      {
	      return server_addr;
      }
      void create_events(int num_events)
      {
	rwp->create_events(num_events);

	MPI_Barrier(MPI_COMM_WORLD);
	int de = rwp->dropped_events();
	int total_de = 0;
	MPI_Allreduce(&de,&total_de,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
	if(myrank==0) std::cout <<" dropped events = "<<total_de<<std::endl;
      }

      void write_events(const char *filename)
      {
	rwp->sort_events();

	rwp->pwrite(filename);

      }

      void read_events(const char *filename)
      {
	rwp->pread(filename);

      }
      ~emu_process()
      {
	if(MS != nullptr) delete MS;
	delete MC;
	delete rwp;
	delete CM;
      }


};

#endif
