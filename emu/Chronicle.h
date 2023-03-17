#ifndef __CHRONICLE_H_
#define __CHRONICLE_H_

#include <string>
#include <cmath>
#include <vector>

class Story
{
   private :
	 std::string name;

   public:
	Story()
	{
	}
	~Story()
	{
	}	
	void setname(std::string &s)
	{
		name = s;
	}
	std::string &getname()
	{
		return name;
	}
};



class Chronicle
{
   private:
	std::string name;
	std::vector<Story> stories;
	int acquisition_count;

   public:
	Chronicle()
	{
	   acquisition_count=0;

	}
	~Chronicle()
	{

	}
	void setname(std::string &s)
	{
		name = s;
	}
	std::string &getname()
	{
		return name;
	}
	std::vector<Story> &get_stories()
	{
	   return stories;
	}
	void increment_acquisition_count()
	{
		acquisition_count++;
	}
	void decrement_acquisition_count()
	{
		acquisition_count--;
	}
	int get_acquisition_count()
	{
		return acquisition_count;
	}
	bool add_story_to_chronicle(std::string &story_name)
	{
	    bool found = false;
	    for(int i=0;i<stories.size();i++)
	    {
		    if(stories[i].getname().compare(story_name)==0)
		    {
			    found = true; break;
		    }
	    }

	    bool ret = false;
	    if(!found)
	    {
		    Story s;
		    s.setname(story_name);
		    stories.push_back(s);
		    ret = true;
	    }
	    return ret;
	}

};

void add_story(Chronicle **);
void increment_acquisition(Chronicle **);
void decrement_acquisition(Chronicle **);
bool acquisition_count_zero(Chronicle **);
#endif
