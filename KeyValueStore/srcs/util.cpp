#include "util_t.h"

int nearest_power_two(int n)
{
   int c = 1;
   while(c < n) c = 2*c;
   return c;
}

