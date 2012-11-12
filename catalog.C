#include "catalog.h"


RelCatalog::RelCatalog(Status &status) :
	 HeapFile(RELCATNAME, status)
{
// nothing should be needed here
}


const Status RelCatalog::getInfo(const string & relation, RelDesc &record)
{
  if (relation.empty())
    return BADCATPARM;

  Status status;
  Record rec;
  RID rid;

  /*
  Open a scan on the relcat relation by invoking the startScan() method on itself.
   */
  
  /*You want to look for the tuple whose first attribute matches the string relName. 
  */
    
  /*
   Then call scanNext() and getRecord() to get the desired tuple. 
  */
    
  /*
   Finally, you need to memcpy() the tuple out of the buffer pool into the return parameter record.
  */


}


const Status RelCatalog::addInfo(RelDesc & record)
{
  RID rid;
  InsertFileScan*  ifs;
  Status status;

  /*
   Adds the relation descriptor contained in record to the relcat relation RelDesc represents both the in-memory format and on-disk format of a tuple in relcat. 
   */
    
  /*
   First, create an InsertFileScan object on the relation catalog table. 
  */
    
  /*
   Next, create a record
  */
    
  /*
   and then insert it into the relation catalog table using the method insertRecord of InsertFileScan.
  */


}

const Status RelCatalog::removeInfo(const string & relation)
{
  Status status;
  RID rid;
  HeapFileScan*  hfs;

  if (relation.empty()) return BADCATPARM;

  /*
   Remove the tuple corresponding to relName from relcat. Once again, you have to start a filter scan on relcat to locate the rid of the desired tuple.
  */
    
  /*
    Then you can call deleteRecord() to remove it.
  */
    
}


RelCatalog::~RelCatalog()
{
// nothing should be needed here
}


AttrCatalog::AttrCatalog(Status &status) :
	 HeapFile(ATTRCATNAME, status)
{
// nothing should be needed here
}


const Status AttrCatalog::getInfo(const string & relation, 
				  const string & attrName,
				  AttrDesc &record)
{

  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;

  if (relation.empty() || attrName.empty()) return BADCATPARM;

  /*
   Returns the attribute descriptor record for attribute attrName in relation relName. Uses a scan over the underlying heapfile to get all tuples for relation and check each tuple to find whether it corresponds to attrName. (Or maybe do it the other way around !) This has to be done this way because a predicated HeapFileScan does not allow conjuncted predicates. Note that the tuples in attrcat are of type AttrDesc (structure given above).
  */


}


const Status AttrCatalog::addInfo(AttrDesc & record)
{
  RID rid;
  InsertFileScan*  ifs;
  Status status;


  /*
   Adds a tuple (corresponding to an attribute of a relation) to the attrcat relation.
  */


}


const Status AttrCatalog::removeInfo(const string & relation, 
			       const string & attrName)
{
  Status status;
  Record rec;
  RID rid;
  AttrDesc record;
  HeapFileScan*  hfs;

  if (relation.empty() || attrName.empty()) return BADCATPARM;
    
  /*
   Removes the tuple from attrcat that corresponds to attribute attrName of relation.
  */

}


const Status AttrCatalog::getRelInfo(const string & relation, 
				     int &attrCnt,
				     AttrDesc *&attrs)
{
  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;

  if (relation.empty()) return BADCATPARM;

  /*
   While getInfo() above returns the description of a single attribute, this method returns (by reference) descriptors for all attributes of the relation via attr, an array of AttrDesc structures,  and the count of the number of attributes in attrCnt. The attrs array is allocated by this function, but it should be deallocated by the caller.
  */


}


AttrCatalog::~AttrCatalog()
{
// nothing should be needed here
}
