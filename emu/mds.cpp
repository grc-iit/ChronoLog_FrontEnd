#include "Chronicle.h"

void add_story(Chronicle **c)
{


}

bool acquisition_count_zero(Chronicle **c)
{
  if((*c)->get_acquisition_count()==0) return true;
  return false;     	
}

void increment_acquisition(Chronicle **c)
{
   (*c)->increment_acquisition_count();
}

void decrement_acquisition(Chronicle **c)
{
   (*c)->decrement_acquisition_count();
}
