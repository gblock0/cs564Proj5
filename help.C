#include <sys/types.h>
#include <functional>
#include <string.h>
#include <stdio.h>
using namespace std;

#include "error.h"
#include "utility.h"
#include "catalog.h"

// define if debug output wanted


//
// Retrieves and prints information from the catalogs about the (*relation?*) for the
// user. If no relation is given (relation is NULL), then it lists all
// the relations in the database, along with the width in bytes of the
// relation, the number of attributes in the relation, and the number of
// attributes that are indexed.  If a relation is given, then it lists
// all of the attributes of the relation, as well as its type, length,
// and offset, whether it's indexed or not, and its index number.
//
// Returns:
// 	OK on success
// 	error code otherwise
//
const Status RelCatalog::help(const string & relation)
{
  Status status;
  RelDesc rd;
  AttrDesc *attrs;
  int attrCnt;

    /*
     If relation.empty() is true (empty() is a method on the string class),
     print (to standard output) a list of all the relations in relcat (including how many attributes it has).
     */

    //currently has error, because of the create in DB
    
   if (relation.empty()) return UT_Print(RELCATNAME); 
    
    /*
    Otherwise, print all the tuples in attrcat that are relevant to relName.
    **************NEED TO COME BACK FOR INDEXING*************************************
     */
  if((status = attrCat->getRelInfo(relation, attrCnt, attrs)) != OK) return status;
  AttrDesc temp;
  cout << "Relation Name: " << relation << endl;
  for(int i = 0; i < attrCnt; i++){
    temp = attrs[i];
    cout << "    " << "Attribute Name: " << temp.attrName << endl;
    cout << "    " << "Attribute Type: " << temp.attrType << endl;
    cout << "    " << "Attribute Length: " << temp.attrLen << endl;
    cout << "    " << "Attribute Offset: " << temp.attrOffset << endl;
  }
  free(attrs);

  return OK;
}
