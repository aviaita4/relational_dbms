#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <math.h>

#include "../rbf/pfm.h"


using namespace std;

// Record ID
typedef struct
{
	unsigned pageNum;    // page number
	unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
	LT_OP,      // <
	LE_OP,      // <=
	GT_OP,      // >
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
 ********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
	RBFM_ScanIterator() {
		this->isOpen = false;

		this->currentPageData = calloc(PAGE_SIZE,1);
//		this->value = calloc(PAGE_SIZE,1);
//		this->fileHandle = (FileHandle *)calloc(sizeof(FileHandle),1);

		this->fileHandle = 0;

//		this->fileHandle = new FileHandle();
	};
	~RBFM_ScanIterator() {
//		free(this->currentPageData);
//		free(this->value);
//		free(this->fileHandle);
	};

	void initialize(FileHandle &fileHandle, const vector<Attribute> recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames);


	// Never keep the results in the memory. When getNextRecord() is called,
	// a satisfying record needs to be fetched from the file.
	// "data" follows the same format as RecordBasedFileManager::insertRecord().
	RC getNextRecord(RID &rid, void *data);
	RC close();

private:
	RID currentRID;
	void* currentPageData;
	bool isOpen;

	FileHandle *fileHandle;
	vector<Attribute> recordDescriptor;
	string conditionAttribute;
	CompOp compOp;
	void *value;
	vector<string> attributeNames;

	bool readRecordDataAndCompareValue(const void* record, const void* pageData, const unsigned short offset, RID &rid, void *data);

	RC setNextRID();

};


class RecordBasedFileManager
{
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
	//  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
	//     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
	//     Each bit represents whether each field value is null or not.
	//     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
	//     If k-th bit from the left is set to 0, k-th field contains non-null values.
	//     If there are more than 8 fields, then you need to find the corresponding byte first,
	//     then find a corresponding bit inside that byte.
	//  2) Actual data is a concatenation of values of the attributes.
	//  3) For Int and Real: use 4 bytes to store the value;
	//     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
	// For example, refer to the Q8 of Project 1 wiki page.
	RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

	RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

	// This method will be mainly used for debugging/testing.
	// The format is as follows:
	// field1-name: field1-value  field2-name: field2-value ... \n
	// (e.g., age: 24  height: 6.1  salary: 9000
	//        age: NULL  height: 7.5  salary: 7500)
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
	 ******************************************************************************************************************************************************************/
	RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the RID does not change after an update
	RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

	RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

	// Scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);

public:
	short encodeRecord(void* record,const void* data, const vector<Attribute> &recordDescriptor,char flagByte, RID recordActualRID, bool debugMode); //returns size of record
	RC readAttributesFromRecordFormat(void* record, const vector<Attribute> & recordDescriptor, const vector<string> &attributeNames, void *data);

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
	static PagedFileManager *_pbf_manager;

	unsigned formatDataToRecord(void* record,const void* data, const vector<Attribute> &recordDescriptor); //returns size of record

	RC freeSpaceDecrease(void* pageData,unsigned sizeOfRecord);
	short addSlot(void* pageData, short* slotNum, void* record, unsigned sizeOfRecord, bool newPage); // return offset of the new slot created
	RC createAppendPage(FileHandle &fileHandle,void* record,short sizeOfRecord, RID &rid , char neatRecord, const void* data, const vector<Attribute> recordDescriptor, bool debug);
	int searchFreePage(FileHandle &fileHandle, short spaceRequired);
	short getFreeSpace(FileHandle &fileHandle,int pageNum);

	short decodeRecord(void* record, void* data, const vector<Attribute> &recordDescriptor, FileHandle &fileHandle);

//	RC readAttributeFromRecord(void* record, const vector<Attribute> & recordDescriptor, const string & attributeName, void *data);
	RC findDeletedSlot(short* slotNum, const void* pageData, const short numSlots);

	RC insertFormattedRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, char flag, RID ridToInsert);

};

#endif