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

  //remove information from attrCat
  status = attrCat->dropRelation(relation);
  if(status != OK) return RELNOTFOUND;

  //then remove information from relCat
  status = relCat->removeInfo(relation);
  if(status != OK) return RELNOTFOUND;
  
  //destroy the actual file
  status = destroyHeapFile(relation);
  if(status != OK) return status;

  return status;

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

  status = getRelInfo(relation, attrCnt, attrs);
  if(status != OK) return status;

  //delete all information in attrCat for the relation relName
  for(i = 0; i < attrCnt; i++){
    status = attrCat->removeInfo(relation, attrs[i].attrName);
    if(status != OK) return status;
  }

  //we are responsible from the memory from the getRelInfo() call
  delete attrs;

  return status;
}


