#include "query_parser.h"



bool query_parser::sort_by_attr(std::string &s, std::vector<struct event> &r_events, std::string &a_name,event_metadata &em)
{
   bool b; 
   int offset; 
   b = em.get_offset(a_name,offset);
   int vsize;
   bool sign_v,end_v;
   b = em.get_attr(a_name,vsize,sign_v,end_v);  

   for(int i=0;i<r_events.size();i++)
   {
        for(int j=0;j<sizeof(double);j++)
	{
           double r = (double)(random()%INT_MAX);
	   r_events[i].data[j+offset];
	}
   }

   auto compare = [&keysize=vsize,&key=offset,&sign_b=sign_v,&end_b=end_v](struct event &e1, struct event &e2)
	          {
		       if(!sign_b)
		       {
			  if(end_b)
			  {
		             for(int i=0;i<keysize;i++)
			       if((unsigned char)e1.data[key+i] < (unsigned char)e2.data[key+i]) return true;
		              return false;
			  }
			  else
			  {
			     for(int i=keysize-1;i>=0;i--)
				if((unsigned char)e1.data[key+i] < (unsigned char)e2.data[key+i]) return true;
			     return false;
			  }
		       }
		       else if(sign_b)
		       {
			  unsigned char mask = 128;
			  unsigned char msb1 = (end_b) ? (unsigned char)e1.data[key] : (unsigned char)e1.data[key+keysize-1];
			  unsigned char s_bit1 = msb1 & mask;
			  s_bit1 = s_bit1 >> 7;
			  unsigned char msb2 = (end_b) ? (unsigned char)e2.data[key] : (unsigned char)e2.data[key+keysize-1];
			  unsigned char s_bit2 = msb2 & mask;
			  s_bit2 = s_bit2 >> 7;
			  
			  if(s_bit1==s_bit2)
			  {
			      if(s_bit1==0)
			      {
				if(end_b)
				{
				   for(int i=0;i<keysize;i++)
				     if((unsigned char)e1.data[key+i] < (unsigned char)e2.data[key+i]) return true;		
				   return false;
				}
				else
				{
				    for(int i=keysize-1;i>=0;i--)
					if((unsigned char)e1.data[key+i] < (unsigned char)e2.data[key+i]) return true;
				    return false;
				}
			      }
			      if(s_bit1==1)
			      {
				 if(end_b) 
				 {		 
				   e1.data[key] = e1.data[key] ^ mask;
				   e2.data[key] = e2.data[key] ^ mask;
			           		
				   for(int i=0;i<keysize;i++)
				    if((unsigned char)e1.data[key+i] > (unsigned char)e2.data[key+i]) return true;
				   return false;
				 }
				 else
				 {
				    e1.data[key+keysize-1] = e1.data[key+keysize-1]^mask;
				    e2.data[key+keysize-1] = e2.data[key+keysize-1]^mask;
				    for(int i=keysize-1;i>=0;i--)
				      if((unsigned char)e1.data[key+i] > (unsigned char)e2.data[key+i]) return true;
				    return false;
				 }
			      }
			  }
			  else if(s_bit1 == 1) return true;
		       }
		  };



   std::sort(r_events.begin(),r_events.end(),compare);
   return true;
}
