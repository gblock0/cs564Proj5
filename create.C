#include "catalog.h"


const Status RelCatalog::createRel(const string & relation, 
				   const int attrCnt,
				   const attrInfo attrList[])
{
  Status status;
  RelDesc rd;
  AttrDesc ad;
  int offset;

  if (relation.empty() || attrCnt < 1)
    return BADCATPARM;

  if (relation.length() >= sizeof rd.relName)
    return NAMETOOLONG;
    
  //check to see if relation already exisits
    //if it does what should we return???
  status = relCat->getInfo(relation, rd);
  if(status == OK) {
    return status;
  } 
  
  //initializing the RelDesc and adding it to relCat
  memcpy(rd.relName, relation.c_str(),sizeof(rd.relName));
  rd.attrCnt = attrCnt;
  status = relCat->addInfo(rd);
  if (status != OK) return status;
  
  offset = 0;
    
  //for all in array copy the attr data into the AttrDesc struct, and add to attrCat
  for(int i = 0; i < attrCnt; i++){
      strcpy(ad.relName, attrList[i].relName);
      strcpy(ad.attrName, attrList[i].attrName);
      ad.attrType = attrList[i].attrType;
      ad.attrLen = attrList[i].attrLen;
      ad.attrOffset = offset;
      status = attrCat->addInfo(ad);
      if(status != OK) return status;
      
      //add offset for next attribute
      offset = (offset + attrList[i].attrLen);
    } 

  //create the heapFile instance to hold tuples for this relation 
  status = createHeapFile(relation);
  
  return status;

}

