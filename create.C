#include "catalog.h"


const Status RelCatalog::createRel(const string & relation, 
				   const int attrCnt,
				   const attrInfo attrList[])
{
  Status status;
  RelDesc rd;
  AttrDesc ad;

  if (relation.empty() || attrCnt < 1)
    return BADCATPARM;

  if (relation.length() >= sizeof rd.relName)
    return NAMETOOLONG;

    
    

  /*
   First make sure that a relation with the same name doesn't already exist (by using the getInfo() function described below). 
  */
    
    
    
  /*Next add a tuple to the relcat relation. Do this by filling in an instance of the RelDesc structure above and then invoking the RelCatalog::addInfo() method. 
  */
    
    
    
    
    
  /*Third, for each of the attrCnt attributes, invoke the AttrCatalog::addInfo() method of the attribute catalog table (remember that this table is referred to by the global variable attrCat), passing the appropriate attribute information from the attrList[] array as an instance of the AttrDesc structure (see below). 
  */
   
    
    
    
    
  /*
   Finally, create a HeapFile instance to hold tuples of the relation (hint: there is a procedure to do this which we have seen in the last project stage; you need to give it a string that is the relation name). Implement this function in create.C
   */

}
