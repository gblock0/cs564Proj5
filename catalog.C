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
    
  //we want to scan the relcat
  HeapFileScan* hfs = new HeapFileScan(RELCATNAME, status);
  if(status != OK) return status;
    
  //search for the string matching relation
  int offset = ((char*)&record.relName) - ((char*)&record);

  status = hfs->startScan(offset,sizeof(record.relName),STRING,relation.c_str(),EQ);
  if(status != OK) 
  {
    delete hfs;
    return status;
  }

  //Scan the record and get the desired tuple. 
  status = hfs->scanNext(rid);
    if(status != OK) {
      delete hfs;
      return status;
    }

  status = hfs->getRecord(rec);
  if(status != OK){
    delete hfs;
    return status;
  }
  
  //copy the tuple out of the buffer pool into the return parameter record.
  memcpy(&record, rec.data, rec.length);

  delete hfs;
    
  return status;

}

/*
 Adds the relation descriptor contained in record to the relcat relation RelDesc represents both the in-memory 
 format and on-disk format of a tuple in relcat. 
 */
const Status RelCatalog::addInfo(RelDesc & record)
{
  RID rid;
  Status status;
  InsertFileScan*  ifs;

  //First, create an InsertFileScan object on the relation catalog table. 
  ifs = new InsertFileScan(RELCATNAME, status);
  if(status != OK) 
  {
    delete ifs;
    return status;
  }

  //Next, create a record
  Record rec;
  rec.data = &record;
  rec.length = sizeof(RelDesc);

  //insert it into the relation catalog table    
  status = ifs->insertRecord(rec, rid);
  if(status != OK) {
    delete ifs;
    return status;
  }

  delete ifs;

  return status;
}

//Remove the tuple corresponding to relName from relcat. 
const Status RelCatalog::removeInfo(const string & relation)
{
  Status status;
  RID rid;
  HeapFileScan*  hfs;
  RelDesc relDesc;

  if (relation.empty()) return BADCATPARM;
 
  //we want to scan the relcat
  hfs = new HeapFileScan(RELCATNAME, status);
  if(status != OK) {
    delete hfs;
    return status;
  }
    
  //search for the string matching relation
  int offset = (char*)&relDesc.relName - (char*)&relDesc;
  status = hfs->startScan(offset,sizeof(relDesc.relName),STRING,relation.c_str(),EQ);
  if(status != OK) {
    delete hfs;
    return status;
  }
    
  //find the record
  status = hfs->scanNext(rid);
  if(status != OK){
    delete hfs;
    return status;
  }
  
  //delete the record
  status = hfs->deleteRecord();
  if(status != OK) {
    delete hfs;
    return status;
  }
  
  delete hfs;
    
  return status;
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

/*
 Returns the attribute descriptor record for attribute attrName in relation relName. 
 */
const Status AttrCatalog::getInfo(const string & relation, 
				  const string & attrName,
				  AttrDesc &record)
{

  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;

  if (relation.empty() || attrName.empty()) return BADCATPARM;

  //we want to scan the attrCat
  hfs = new HeapFileScan(ATTRCATNAME, status);
    if(status != OK){ delete hfs; return status;}
    
  //search for the string matching relation
  int offset = (char*)&record.relName - (char*)&record;
    cout << "offset: " <<offset << endl;
  status = hfs->startScan(offset,sizeof(record.relName),STRING,relation.c_str(),EQ);
  if(status != OK) { delete hfs; return status;}
    
  //will scan until it finds a match for both relation and attrName
  //or until end of file is reached
  while(true)
  {
      status = hfs->scanNext(rid);
      if(status != OK) { delete hfs; return status;}

      status = hfs->getRecord(rec);
      if(status != OK) { delete hfs; return status;}
      
      //the record data is actually of type AttrDesc so we can cast it as such
      memcpy(&record, rec.data, rec.length);
      
      //check to see if the record has a matching attrName too
      if(memcmp(&record.attrName,attrName.c_str(),sizeof(record.attrName))){
          //ahh! we found a match!
          delete hfs;
          return status;
      }
  }
  
}

/*
 Adds a tuple (corresponding to an attribute of a relation) to the attrcat relation.
*/
const Status AttrCatalog::addInfo(AttrDesc & record)
{
    RID rid;
    Status status;
    InsertFileScan*  ifs;
    
    //First, create an InsertFileScan object on the attribute catalog table. 
    ifs = new InsertFileScan(ATTRCATNAME, status);
    if(status != OK) { delete ifs; return status;}
    
    //Next, create a record
    Record rec;
    rec.data = &record;
    rec.length = sizeof(AttrDesc);
    
    //insert it into relCat table using the method
    status = ifs->insertRecord(rec, rid);
    if(status != OK) { delete ifs; return status;}
    
    delete ifs;
    
    return status;
}

/*
 Removes the tuple from attrcat that corresponds to attribute attrName of relation.
 */
const Status AttrCatalog::removeInfo(const string & relation, 
			       const string & attrName)
{
    Status status;
    RID rid;
    HeapFileScan*  hfs;
    AttrDesc attrDesc;
    
    if (relation.empty() || attrName.empty()) return BADCATPARM;
    
    //we want to scan the attr cat
    hfs = new HeapFileScan(ATTRCATNAME, status);
    if(status != OK) { delete hfs; return status;}
    
    //search for the string matching relation
    int offset = (char*)&attrDesc.relName - (char*)&attrDesc;
    status = hfs->startScan(offset,sizeof(attrDesc.relName),STRING,relation.c_str(),EQ);
    if(status != OK){ delete hfs; return status;}
    
    //find the record
    status = hfs->scanNext(rid);
    if(status != OK) { delete hfs; return status;}
    
    //delete the record
    status = hfs->deleteRecord();
    if(status != OK) { delete hfs; return status;}
    
    delete hfs;
    
    return status;
}

/*
 While getInfo() above returns the description of a single attribute,
 this method returns (by reference) descriptors for all attributes of the relation via attrs,
 an array of AttrDesc structures,  and the count of the number of attributes in attrCnt.
 The attrs array is allocated by this function, but it should be deallocated by the caller.
*/
const Status AttrCatalog::getRelInfo(const string & relation, 
				     int &attrCnt,
				     AttrDesc *&attrs)
{
  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;
  RelDesc relDesc;

  if (relation.empty()) return BADCATPARM;

  //get the number of attributes for the relation and create the array
  status = relCat->getInfo(relation, relDesc);
  if(status != OK) return status;
  attrCnt = relDesc.attrCnt;
  attrs = new AttrDesc[attrCnt];
    
  //we want to scan the attrCat
  hfs = new HeapFileScan(ATTRCATNAME, status);
  if(status != OK) {delete hfs; return status;}
 
  //search for the string matching relation
  int offset = (char*)&attrs[0].relName - (char*)&attrs[0];
  status = hfs->startScan(offset, sizeof(attrs[0].relName), STRING, relation.c_str(), EQ);
  if(status != OK) return status;

  //scan until it has reached EOF or found all the attribute descriptions
  //each match add it to the array
  int i = 0;
  while(((status = hfs->scanNext(rid)) != FILEEOF))
  {
    if(status != OK) {delete hfs; return status;}
 
    status = hfs->getRecord(rec);
    if(status != OK) {delete hfs; return status;}
      
    //the record data is actually of type AttrDesc so we can cast it as such
    memcpy(&(attrs[i]), rec.data, rec.length);
    
    i++;
    
    //if found all attributes then exit
    if(i == attrCnt)
    {
        break;
    }
  }
    
  delete hfs;
    
  return status;
}

AttrCatalog::~AttrCatalog()
{
// nothing should be needed here
}

