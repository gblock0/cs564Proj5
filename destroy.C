#include "catalog.h"

//
// Destroys a relation. It performs the following steps:
//
// 	removes the catalog entry for the relation
// 	destroys the heap file containing the tuples in the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status RelCatalog::destroyRel(const string & relation)
{
  Status status;

  if (relation.empty() || 
      relation == string(RELCATNAME) || 
      relation == string(ATTRCATNAME))
    return BADCATPARM;


  /*
   First remove all relevant schema information from both the relcat and attrcat relations. 
   
  */
    
  /*Then destroy the heapfile corresponding to the relation (hint: again, there is a procedure to destroy heap file that we have seen in the last project stage; you need to give it a string that is the relation name). Implement this function in destroy.C
  */


}


//
// Drops a relation. It performs the following steps:
//
// 	removes the catalog entries for the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status AttrCatalog::dropRelation(const string & relation)
{
  Status status;
  AttrDesc *attrs;
  int attrCnt, i;

  if (relation.empty()) return BADCATPARM;


  /*
  Deletes all tuples in attrcat for the relation relName. 

  Again use a scan to find all tuples whose relName attribute equals relation and then call deleteRecord() on each one. Implement this function in destroy.C
  */



}

