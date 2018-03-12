#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <map>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define sizeOfUnsigned sizeof(unsigned)
#define sizeOfInt sizeof(int)
#define sizeOfFloat sizeof(float)
#define sizeOfChar sizeof(char)
#define sizeOfFooter (sizeOfInt + sizeOfChar + sizeOfInt)

#define SUCCESS 0
#define META_FILE "root_page_index"
#define LEAF_NODE 'l'
#define INTERNAL_NODE 'i'
#define ROOT_NODE 'r'

#define INSERT_OPERATION 'I'
#define DELETE_OPERATION 'D'
#define SEARCH_OPERATION 'S'

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
	static IndexManager *instance();
	map<string, int> indexRootPageMap;

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle,
			const Attribute &attribute,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);
//
//	template<class T>
//	void printRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth);

	template<class T>
	void printRecursive2(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth);

	template<class T>
	vector<T> getKeysPointers(const void *data, int freeSpace, const Attribute &attribute, vector<int> &pointers);

//	template<class T>
//	vector<T> getKeysRIDs(const void *data, int freeSpace, const Attribute &attribute, vector<RID> &rids);

	template<class T>
	map<T,vector<RID> > getKeysRIDMap(const void *data, int freeSpace, const Attribute &attribute);

protected:
	IndexManager();

	~IndexManager();

private:
	static IndexManager index_manager;
	static PagedFileManager *_pbf_manager;

	RC loadMapFromFile();

	RC storeMapIntoFile();

	void traverseTree(IXFileHandle &ixFileHandle, const int &currPageNum, const Attribute &attribute,
					const void *inputKeyVoid, const RID &inputRID, int &newPageNum, void *returnSplitKey, char operation , int &opOffset ,  int &opPageNum);

	void splitThePage(IXFileHandle &ixFileHandle, const void *input, const Attribute &attribute, const void *insertKey,
					  int insertPosition, const RID &rid, const int childPageNum, void *splitKey, int &newPageNum);
};


class IX_ScanIterator {
public:

	// Constructor
	IX_ScanIterator();

	// Destructor
	~IX_ScanIterator();

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);

	// Terminate index scan
	RC close();

	RC initialize(IXFileHandle &ixfileHandle,
			  const Attribute &attribute,
			  const void *lowKey,
			  const void *highKey,
			  bool lowKeyInclusive,
			  bool highKeyInclusive,
			  int startPageNumber,
			  int searchOffset);

	RC setNextEntryOffset(int moveOffsetBy);

//	RC setNextEntryOffsetNew(int moveOffsetBy);

	bool checkKey(void* keyTemp);

	template <class myType>
	bool compare(myType a, CompOp compOp, myType b);

	bool compareStrings(string a, CompOp compOp, string b);

	RC getIntoSearchRange();

	void allocateSpace(){
		this->currentPageData = (char*)calloc(PAGE_SIZE,1);
	};

private:

	char* currentPageData;
	int occupationSize;
	bool isOpen;
	IXFileHandle *ixfileHandle;

	int offset;
	Attribute attribute;

	void* lowKey;
	void* highKey;

	int lowKeyInt;
	int highKeyInt;

	float lowKeyFloat;
	float highKeyFloat;

	string lowKeyString;
	string highKeyString;

	bool lowKeyInclusive;
	bool highKeyInclusive;

	bool reachedSearchRange;

	int currentPageNum;

};


class IXFileHandle {

public:
	FileHandle fileHandle;
	string fileName;

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;
	int rootPageNum;

	// Constructor
	IXFileHandle();

	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

	RC syncCounterValues();
};

#endif
