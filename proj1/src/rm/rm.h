
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"
#include<iostream>
using namespace std;

#define RM_EOF (-1)  // end of a scan operator
#define CATALOG_TABLE_FILE "Tables"
#define CATALOG_COLUMN_FILE "Columns"
#define CATALOG_TABLE_ID_FILE "TableId"
#define SUCCESS 0
#define sizeOfInt sizeof(int)
#define USER_TYPE "user"
#define USER_TYPE_LENGTH 4
#define SYSTEM_TYPE "system"
#define SYSTEM_TYPE_LENGTH 6

typedef enum {
	Tables = 0, Columns, TableId
} Catalog;


// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
	RM_ScanIterator();

	~RM_ScanIterator();

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data);

	RC close();

	void setRbfm_scanIterator(RBFM_ScanIterator &rbfm_scanIterator);

	RBFM_ScanIterator getRbfm_scanIterator();

	RBFM_ScanIterator rbfm_scanIterator;

	FileHandle* currentFileHandle;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator();

  ~RM_IndexScanIterator();

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);

  RC close();

  void setIx_scanIterator(IX_ScanIterator &ix_ScanIterator);

  IX_ScanIterator ix_ScanIterator;

  IXFileHandle* ixfileHandle;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr){
	  return -1;
  };

  RC dropAttribute(const string &tableName, const string &attributeName){
	  return -1;
  };

protected:
  RelationManager();
  ~RelationManager();

private:
    static RelationManager rm;
	RecordBasedFileManager *rbfm;
	IndexManager *im;

	FileHandle tablesFileHandle, columnsFileHandle;
	string getRecordForTables(int tableId, string tableName, bool isUserTable);

	string getRecordForColumns(int tableId, string columnName, int columnType, int columnLength, int columnPosition);

	void PrintBytesWithRange(const char *pBytes, const uint32_t end, uint32_t start);

	void putNumberInStringStream(stringstream &stringstream, int number);

	RC getOrDeleteAttributes(const string &tableName, vector<Attribute> &attrs, bool toDelete);

	static vector<Attribute> getAttributesOfMeta(bool forTable);

	static vector<string> getAttributeNamesOfMeta(bool forTable);

	int getNewTableId(const string &tableName);

	Attribute mapAttributeName(const string &tableName, const string &attributeName);

	vector<string> getIndexAttributeNames(const string &tableName, vector<string> &indexNames);

	RC insertDeleteIndexEntry(const string &tableName, vector<Attribute> tableAttributes, const void *data,const RID &rid, bool op);
};

#endif
