
#include <iostream>
#include "ix.h"
#include "../rbf/rbfm.h"
#include <map>
#include <sys/stat.h>


using namespace std;

IndexManager IndexManager::index_manager;

PagedFileManager *IndexManager::_pbf_manager = 0;

IndexManager *IndexManager::instance() {
	return &index_manager;
}

IndexManager::IndexManager() {
	_pbf_manager = PagedFileManager::instance();
	loadMapFromFile();
}

IndexManager::~IndexManager() {
	storeMapIntoFile();
}

RC IndexManager::createFile(const string &fileName) {
	return _pbf_manager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName) {
	return _pbf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	auto iterator = indexRootPageMap.find(fileName);
	if (iterator != indexRootPageMap.end()) {
		ixfileHandle.rootPageNum = iterator->second;
	} else {
		ixfileHandle.rootPageNum = 0;
		indexRootPageMap[fileName] = 0;
	}
	ixfileHandle.fileName = fileName;
	return _pbf_manager->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	indexRootPageMap[ixfileHandle.fileName] = ixfileHandle.rootPageNum;
	return _pbf_manager->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::loadMapFromFile() {

	struct stat stFileInfo;
	if (stat(META_FILE, &stFileInfo) == 0) {
		ifstream fileStream;
		fileStream.open(META_FILE, ios::in);
		string indexName;
		int rootPageNum;
		while (fileStream >> indexName >> rootPageNum) {
//			cout << indexName << " " << rootPageNum << endl;
			indexRootPageMap[indexName] = rootPageNum;
		}
		fileStream.close();
	}
}

RC IndexManager::storeMapIntoFile() {

	ofstream fileStream;
	fileStream.open(META_FILE, ios::out);
	for (auto &entry: indexRootPageMap) {
		fileStream << entry.first << " " << entry.second << endl;
	}
	fileStream.close();
}

int getKeyLength(const Attribute &attribute, const void *key) {

	if (attribute.type == TypeVarChar) {
		int length;
		memcpy(&length, key, sizeOfInt);
		return (sizeOfInt + length);
	}
	return sizeOfInt;
}

bool isLeafPage(const void *page) {
	char pageType;
	memcpy(&pageType, (char *) page + PAGE_SIZE - sizeOfInt - sizeOfChar, sizeOfChar);
	return (pageType == LEAF_NODE);
}

int getFreeSpace(const void *currPage) {
	int freeSpace;
	char *pointer = (char *) currPage;
	memcpy(&freeSpace, pointer + PAGE_SIZE - sizeOfInt, sizeOfInt);
	return freeSpace;
}

void updateFreeSpace(const void *currPage, int freeSpace) {
	char *pointer = (char *) currPage;
	memcpy(pointer + PAGE_SIZE - sizeOfInt, &freeSpace, sizeOfInt);
}

void prepareWriteAndUpdateFreeSpace(const char *page, int keyPosition, int keyLength, bool isLeafPage) {

	int freeSpace = getFreeSpace(page);
	int fragmentLeft = keyPosition;
	int fragmentRight = PAGE_SIZE - freeSpace - sizeOfInt;
	int fragmentSize = fragmentRight - fragmentLeft;
	int slideToRight = keyLength;
	slideToRight += isLeafPage ? sizeof(RID) : sizeOfInt;
	char *keyData = (char *) page + keyPosition;
	memmove(keyData + slideToRight, keyData, fragmentSize);
	freeSpace -= slideToRight;
	// Free space updation
	updateFreeSpace(page, freeSpace);

}

void writeKeyWithPayLoad(const void *pageData, int keyPosition, const Attribute &attribute, const void *key,
						 const RID &rid, const int pageNum) {

	char *page = (char *) pageData;
	bool isLeafNode = isLeafPage(page);
	int keyLength = getKeyLength(attribute, key);
	prepareWriteAndUpdateFreeSpace(page, keyPosition, keyLength, isLeafNode);
	char *keyData = page + keyPosition;
	memcpy(keyData, key, keyLength);
	keyData += keyLength;

	if (isLeafNode) {
		memcpy(keyData, &rid, sizeof(RID));
	} else {
		memcpy(keyData, &pageNum, sizeOfUnsigned);
	}
}

int deleteKeyWithPayLoad(const void *pageData, int keyPosition, const Attribute &attribute, const void *key,
						 const RID &rid) {

	bool found = false;

	char *page = (char *) pageData;
	int keyLength = getKeyLength(attribute, key);

	keyPosition -= keyLength + sizeof(RID);

	int keyRIDPosition = keyPosition;

	int initialFreeSpace = getFreeSpace(pageData);
	int spaceOccupied = PAGE_SIZE - getFreeSpace(pageData) - sizeOfInt - sizeOfChar - sizeOfInt;

	RID compRID;
	void* compKey = calloc(keyLength,1);

	for(int i=keyPosition; i<=spaceOccupied; i+=keyLength+sizeof(RID)){

		memcpy(compKey, page+i,keyLength);
		if(memcmp(key,compKey,keyLength) != 0){
//			cout<<"Not found for delete!"<<endl;
			return -1;
		}

		memcpy(&compRID,page+i+keyLength,sizeof(RID));
		if(compRID.pageNum == rid.pageNum && compRID.slotNum == rid.slotNum){
			found = true;
			keyRIDPosition = i;
			break;
		}
	}

	free(compKey);

	if(found == false){
//		cout<<"Not found for delete!"<<endl;
		return -1;
	}

	// move the data
	int rightFragSize = spaceOccupied - (keyLength+sizeof(RID)+ keyRIDPosition);

	if(rightFragSize > 0){
		memmove(page+ keyRIDPosition, page+ keyRIDPosition+keyLength+sizeof(RID),  rightFragSize);
	}

	// decrease free space
	updateFreeSpace(page, initialFreeSpace+sizeof(RID)+keyLength);
	return 0;
}

/*
 * Entries at the end of the page:
 * siblingPointer, pageType, freeSpace
 */
void *getEmptyPage(bool leafPage) {
	void *newPage = calloc(PAGE_SIZE, 1);
	char *newEmptyPage = (char *) newPage;
	char nodeType = INTERNAL_NODE;
	int freeSpace = PAGE_SIZE - sizeOfInt - sizeOfChar;
	if (leafPage) {
		int sibilingPageNum = -1;
		memcpy(newEmptyPage + PAGE_SIZE - sizeOfInt - sizeOfChar - sizeOfInt, &sibilingPageNum, sizeOfInt);
		freeSpace -= sizeOfInt;
		nodeType = LEAF_NODE;
	}
	memcpy(newEmptyPage + PAGE_SIZE - sizeOfInt - sizeOfChar, &nodeType, sizeOfChar);
	updateFreeSpace(newEmptyPage, freeSpace);
	return newEmptyPage;
}

/**
 *
 * return 0 = equal, -1 = entry < search, 1 = entry > inputKey
 */
int compareValues(const void *currPageEntry, const Attribute &attribute, const char *inputKey) {

	int compareResult = -1;
	char *currPageData = (char *) currPageEntry;
	switch (attribute.type) {
		case AttrType::TypeInt:
			int searchKeyInt, entryKeyInt;
			memcpy(&searchKeyInt, inputKey, sizeOfInt);
			memcpy(&entryKeyInt, currPageData, sizeOfInt);
			if (entryKeyInt == searchKeyInt) {
				compareResult = 0;
			} else if (entryKeyInt > searchKeyInt) {
				compareResult = 1;
			}
			break;

		case AttrType::TypeReal:
			float searchKeyFloat, entryKeyFloat;
			memcpy(&searchKeyFloat, inputKey, sizeOfFloat);
			memcpy(&entryKeyFloat, currPageData, sizeOfFloat);
//			cout << "entryKeyFloat: " << entryKeyFloat << " searchKeyFloat: " << searchKeyFloat << endl;
			if (entryKeyFloat == searchKeyFloat) {
				compareResult = 0;
			} else if (entryKeyFloat > searchKeyFloat) {
				compareResult = 1;
			}
			break;

		case AttrType::TypeVarChar:
			int searchKeySize, entryKeySize;
			memcpy(&searchKeySize, inputKey, sizeOfInt);
			memcpy(&entryKeySize, currPageData, sizeOfInt);
			inputKey += sizeOfInt;
			currPageData += sizeOfInt;
			string searchKey(inputKey, searchKeySize), entryKey(currPageData, entryKeySize);
			if (entryKey == searchKey) {
				compareResult = 0;
			} else if (entryKey.compare(searchKey) > 0) {
				compareResult = 1;
			}
			break;
	}
	return compareResult;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {

//	cout<< "IM "<<attribute.name<< " is "<<*(float*)((char*)key+1)<<endl;

	//Inserting first key into the tree
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0) {
		void *rootPage = getEmptyPage(true);
		writeKeyWithPayLoad(rootPage, 0, attribute, key, rid, 0);
		if (ixfileHandle.fileHandle.appendPage(rootPage) == SUCCESS) {
			free(rootPage);
		} else {
			free(rootPage);
			cout << "insertEntry: append root page failed" << endl;
			return -1;
		}
		ixfileHandle.rootPageNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;
	} else {
		int newPageNum;
		void *newRootEntry = calloc(PAGE_SIZE, 1);    //TODO: What will be it's size????
		int wasteOffset;
		int wastePageNum;
		traverseTree(ixfileHandle, ixfileHandle.rootPageNum, attribute, key, rid, newPageNum, newRootEntry, INSERT_OPERATION, wasteOffset, wastePageNum);
		if (newPageNum != -1) {
//			cout << "Splitting root page " << ixfileHandle.rootPageNum << " newRootEntry " << *(int *)newRootEntry << endl;
			cout.flush();
			void *newRootPage = getEmptyPage(false);
			memcpy(newRootPage, &ixfileHandle.rootPageNum, sizeOfInt);
			int freeSpace = getFreeSpace(newRootPage);
			freeSpace -= sizeOfInt;
			updateFreeSpace(newRootPage, freeSpace);
			writeKeyWithPayLoad(newRootPage, sizeOfInt, attribute, newRootEntry, rid, newPageNum);
			if (ixfileHandle.fileHandle.appendPage(newRootPage) == SUCCESS) {
				free(newRootEntry);
				free(newRootPage);
			} else {
				free(newRootEntry);
				free(newRootPage);
				cout << "insertEntry: root page split failed" << endl;
				return -1;
			}
			ixfileHandle.rootPageNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;
		} else {
			free(newRootEntry);
		}
	}

	ixfileHandle.syncCounterValues();
	return 0;
}

int findSplitOffset(const void *inputPage, const Attribute &attribute) {

	int startOfList = 0, endOfList = 0;
	bool isLeafNode = isLeafPage(inputPage);
	char *pageStart = (char *) inputPage;
	char *currKey = (char *) inputPage;
	if (!isLeafNode) {
		currKey += sizeOfInt;
	}
	char *prevKey = currKey;
	// Skipping first key
	currKey += getKeyLength(attribute, currKey);
	currKey += isLeafNode ? sizeof(RID) : sizeOfInt;

	while (true) {
		int keySize = getKeyLength(attribute, currKey);
		if (memcmp(prevKey, currKey, keySize) != 0) {
			endOfList = (int) (currKey - pageStart);
			// Middle of the page is between startOfList & endOfList
			if (endOfList >= PAGE_SIZE / 2) {
				if (startOfList + endOfList < PAGE_SIZE) {
					return endOfList;
				}
				return startOfList;
			}
			startOfList = endOfList;
		}
		prevKey = currKey;
		currKey += keySize;
		currKey += isLeafNode ? sizeof(RID) : sizeOfInt;
	}
}

/**
 *
 * @param ixFileHandle
 * @param input page to be split
 * @param attribute
 * @param insertKey
 * @param insertPosition position in the input page where a key has to be inserted
 * @param rid if input is leaf page
 * @param childPageNum if input is non-leaf page
 * @param splitKey returns where the input is split
 * @param newPageNum returns page number of the newly created page
 */
void IndexManager::splitThePage(IXFileHandle &ixFileHandle, const void *input, const Attribute &attribute,
								const void *insertKey, int insertPosition, const RID &rid, const int childPageNum,
								void *splitKey, int &newPageNum) {

	int splitOffset = findSplitOffset(input, attribute);
	bool isLeafNode = isLeafPage(input);
	int freeSpace = getFreeSpace((char *) input);
	char *inputPage = (char *) input;
	void *newPage = getEmptyPage(isLeafNode);
	int splitKeyLength = getKeyLength(attribute, inputPage + splitOffset);
	int copyingSize = PAGE_SIZE - freeSpace - splitOffset - sizeOfInt - sizeOfChar;
	int oldSibling;
	if (isLeafNode) {
		copyingSize -= sizeOfInt;    //sibling pointer
		freeSpace += copyingSize;
		int siblingOfNewPage;
		memcpy(&oldSibling, inputPage + PAGE_SIZE - sizeOfInt - sizeOfChar - sizeOfInt, sizeOfInt);
		memcpy(&siblingOfNewPage, (char*)newPage + PAGE_SIZE - sizeOfInt - sizeOfChar - sizeOfInt, sizeOfInt);
//		cout << " old sibling = " << oldSibling << " siblingOfNewPage = "<<siblingOfNewPage;
	} else {
		freeSpace += copyingSize;
		copyingSize -= splitKeyLength;    //remove splitKey
	}

	if (copyingSize > 0) {
		memcpy(splitKey, inputPage + splitOffset, splitKeyLength);
		memcpy(newPage, inputPage + splitOffset, copyingSize);
	} else {
		memcpy(splitKey, insertKey, getKeyLength(attribute, insertKey));
	}

	if(!isLeafNode) {
		splitOffset += splitKeyLength;
	}

	if (copyingSize > 0) {
		memcpy(newPage, inputPage + splitOffset, copyingSize);
	}
//	cout << "splitOffset " << splitOffset << " CopyingSize " << copyingSize << " insertPosition " << insertPosition << " splitKey "
//		 << *(int *)splitKey << endl;
	int newFreeSpace = PAGE_SIZE - copyingSize - sizeOfInt - sizeOfChar;
	if (isLeafNode) {
		newFreeSpace -= sizeOfInt;
		memcpy((char *)newPage + PAGE_SIZE - sizeOfInt - sizeOfChar - sizeOfInt, &oldSibling, sizeOfInt);
	}
	updateFreeSpace(newPage, newFreeSpace);
	updateFreeSpace(inputPage, freeSpace);

	//Inserting into old or newPage
	if (insertPosition < splitOffset) {
		writeKeyWithPayLoad(input, insertPosition, attribute, insertKey, rid, childPageNum);
	} else {
		writeKeyWithPayLoad(newPage, insertPosition - splitOffset, attribute, insertKey, rid, childPageNum);
	}

	ixFileHandle.fileHandle.appendPage(newPage);
	newPageNum = ixFileHandle.fileHandle.getNumberOfPages() - 1;
	//updating sibling pointer
	if (isLeafNode) {
		memcpy(inputPage + PAGE_SIZE - sizeOfInt - sizeOfChar - sizeOfInt, &newPageNum, sizeOfInt);
//		cout << " new sibling = " << newPageNum;
	}
	free(newPage);
}

/**
 *
 * @param compareResult 0 = equal, -1 = entry < search, 1 = entry > search
 */
void findProperOffsetForInputKey(const void *currPage, const Attribute &attribute, const void *inputVoid,
								 int &offset) {

	char *inputKey = (char *) inputVoid;
	char *currPageData = (char *) currPage;
	bool isLeafNode = isLeafPage(currPage);
	int freeSpace = getFreeSpace(currPage);
	offset = 0;
	int compareResult = -1;    // 0 = equal, -1 = entry < search, 1 = entry > search
	int searchKeySize = sizeOfInt, entryKeySize = sizeOfInt;
	int maxValueForOffset = PAGE_SIZE - freeSpace - sizeOfInt - sizeOfChar;
	if (!isLeafNode) {
		currPageData += sizeOfInt;
		offset += sizeOfInt;
	} else {
		maxValueForOffset -= sizeOfInt;
	}

//	cout << "maxValueForOffset: " << maxValueForOffset << " compareResult - ";
	while (offset < maxValueForOffset && compareResult == -1) {
//		if (attribute.type == AttrType::TypeVarChar) {
//			int searchStringLength, entryStringLength;
//			memcpy(&searchStringLength, inputKey, sizeOfInt);
//			memcpy(&entryStringLength, currPageData, sizeOfInt);
//			searchKeySize += searchStringLength;
//
//		}
		entryKeySize = getKeyLength(attribute, currPageData);
		compareResult = compareValues(currPageData, attribute, inputKey);
//		cout << compareResult << " ";
		if (compareResult == 1) {
			return;
		}
		currPageData += entryKeySize;
		currPageData += isLeafNode ? sizeof(RID) : sizeOfInt;
		offset = currPageData - (char *) currPage;
	}
//	cout << "reached the end" << endl;
}

/**
 *
 * @param newPageNum -1 if no need to split
 * @param returnSplitKey do not consider this if {@code} splitPageNum = -1
 * @return
 */
void IndexManager::traverseTree(IXFileHandle &ixFileHandle, const int &currPageNum, const Attribute &attribute,
							  const void *inputKey, const RID &inputRID, int &newPageNum, void *returnSplitKey, char operation, int &opOffset, int &opPageNum) {

	newPageNum = -1;
//	char *inputKey = (char *) inputKeyVoid;
	void *currPage = calloc(PAGE_SIZE, 1);
	ixFileHandle.fileHandle.readPage(currPageNum, currPage);

	int offset = 0;
	int freeSpace = getFreeSpace(currPage);
	bool isLeafNode = isLeafPage(currPage);
//	cout << currPageNum << " " << isLeafNode << " true:" << true << endl;
	char *currPageData = (char *) currPage;

	findProperOffsetForInputKey(currPage, attribute, inputKey, offset);
	currPageData = (char *) currPage + offset;
//	cout << "currPage:" << currPageNum << " key:" << *(int *) inputKey << " findProperOffsetForInputKey: " << offset
//		 << " freeSpace: " << freeSpace << endl;
	cout.flush();

	if (isLeafNode) {

		int searchKeySize = getKeyLength(attribute, inputKey);
		if(operation == SEARCH_OPERATION){
			opPageNum = currPageNum;
			opOffset = offset;
			free(currPage);
			return;
		}

		if(operation == DELETE_OPERATION){
			RC rc = deleteKeyWithPayLoad(currPage, offset, attribute, inputKey, inputRID);

			if(rc != SUCCESS){
				opPageNum = -1;
//				cout<<"deleteKeyWithPayLoad failed"<<endl;
				free(currPage);
				return ;
			}else{
				opPageNum = currPageNum;

				ixFileHandle.fileHandle.writePage(currPageNum, currPage);
				cout.flush();
				free(currPage);
				return ;
			}
		}

		//TODO: Check for freeSpace
		if (searchKeySize + sizeof(RID) < freeSpace) {
//			cout << "Inserting at " << currPageNum << " " << offset << endl;
			// Inserting inputKey, inputRID into leaf node
			if(operation == INSERT_OPERATION){
				writeKeyWithPayLoad(currPage, offset, attribute, inputKey, inputRID, 0);
				ixFileHandle.fileHandle.writePage(currPageNum, currPage);
			}

		} else {
//			cout << *(int *)inputKey << " Split in leaf node: " << currPageNum << " ";
//			cout.flush();
			splitThePage(ixFileHandle, currPage, attribute, inputKey, offset, inputRID, 0, returnSplitKey, newPageNum);
			ixFileHandle.fileHandle.writePage(currPageNum, currPage);
		}
	} else {

		int newChildPageNum;
		void *newEntry = calloc(PAGE_SIZE, 1);    //TODO: What will be it's size????
		//Recursion
		int childPage;
		// Traversing through the child pointer before the entry
		memcpy(&childPage, currPageData - sizeOfInt, sizeOfInt);
		traverseTree(ixFileHandle, childPage, attribute, inputKey, inputRID, newChildPageNum, newEntry, operation, opOffset, opPageNum);

		//Child node got split
		if (newChildPageNum != -1) {
			findProperOffsetForInputKey(currPage, attribute, newEntry, offset);
//			cout << "currPage:" << currPageNum << " key:" << *(int *) newEntry << " findProperOffsetForInputKey: "
//				 << offset << " freeSpace: " << freeSpace << " newChildPageNum: " << newChildPageNum << endl;
			int newEntrySize = getKeyLength(attribute, newEntry);
//			cout << "Split occurred for child node " << currPageNum << "  " << childPage << endl;
			//TODO: Check for freeSpace
			// Writing newEntry, newChildPageNum
			if (newEntrySize + sizeOfInt < freeSpace) {
				writeKeyWithPayLoad(currPage, offset, attribute, newEntry, inputRID, newChildPageNum);
			} else {
//				cout << "Split in non-leaf node: " << currPageNum << endl;
				splitThePage(ixFileHandle, currPage, attribute, newEntry, offset, inputRID, newChildPageNum,
							 returnSplitKey, newPageNum);
			}
			ixFileHandle.fileHandle.writePage(currPageNum, currPage);
		}
		free(newEntry);
	}

	cout.flush();
	free(currPage);
	return;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
	int newPageNum;
	void* newRootEntry = NULL;
	int deleteOffset;
	int deletePageNum;
	traverseTree(ixfileHandle, ixfileHandle.rootPageNum, attribute, key, rid, newPageNum, newRootEntry, DELETE_OPERATION, deleteOffset, deletePageNum);
	if(deletePageNum >= 0){
		ixfileHandle.syncCounterValues();
		return 0;
	}else{
//		cout<<"deleteEntry failed!"<<endl;
		return -1;
	}
}

IXFileHandle::IXFileHandle() {
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
	rootPageNum = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::syncCounterValues() {
	return fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
	syncCounterValues();
	return fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) {

	int rootPageNum = ixfileHandle.rootPageNum;
	switch (attribute.type) {
		case AttrType::TypeInt :
			printRecursive2<int>(ixfileHandle, attribute, rootPageNum,0);
			break;
		case AttrType::TypeReal :
			printRecursive2<float>(ixfileHandle, attribute, rootPageNum,0);
			break;
		case AttrType::TypeVarChar :
			printRecursive2<string>(ixfileHandle, attribute, rootPageNum,0);
			break;
		default:
			break;
	}
}

template<class T>
vector<T> IndexManager::getKeysPointers(const void *data, int freeSpace, const Attribute &attribute, vector<int> &pointers) {
	vector<T> keys;

	char *pageData = (char *) data;

	int sizeOfKey = 4;
	int tempPointer = 0;
	T tempKey;

	int spaceOccupied = PAGE_SIZE - freeSpace - sizeOfInt - sizeOfChar;
	while (!(spaceOccupied == 0 || spaceOccupied == sizeOfInt)) {

		// Read pointer
		memcpy(&tempPointer, pageData, sizeof(int));
		pointers.push_back(tempPointer);
		pageData += sizeof(int);

		// Read key
		memcpy(&tempKey, pageData, sizeOfKey);
		pageData += sizeOfKey;

		// push key
		keys.push_back(tempKey);

		// iterate through page
		spaceOccupied -= sizeOfInt;
		spaceOccupied -= sizeOfKey;
	}

	// if the last pointer exists
	if (spaceOccupied == sizeOfInt) {
		// read last pointer

		memcpy(&tempPointer, pageData, sizeof(int));
		pointers.push_back(tempPointer);
		pageData += sizeof(int);

		return keys;
	} else {
		return keys;
	}
}

template<>
vector<string> IndexManager::getKeysPointers<string>(const void *data, int freeSpace, const Attribute &attribute, vector<int> &pointers) {
	vector<string> keys;

	char *pageData = (char *) data;

	int sizeOfKey = 0;
	int tempPointer = 0;
	string tempStringKey;

	int spaceOccupied = PAGE_SIZE - freeSpace - sizeOfInt - sizeOfChar;
	while (!(spaceOccupied == 0 || spaceOccupied == sizeOfInt)) {

		// Read pointer

		memcpy(&tempPointer, pageData, sizeof(int));
		pointers.push_back(tempPointer);
		pageData += sizeof(int);

		// Read key
		memcpy(&sizeOfKey, pageData, 4);
		char* dummy = (char*) calloc(sizeOfKey,1);
		memcpy(dummy, pageData+4, sizeOfKey);
		pageData += 4 + sizeOfKey;

		// push key
		tempStringKey = string(dummy,sizeOfKey);
		free(dummy);
		keys.push_back(tempStringKey);

		// iterate through page
		spaceOccupied -= sizeOfInt;
		spaceOccupied -=  sizeof(int) + sizeOfKey;
	}

	// if the last pointer exists
	if (spaceOccupied == sizeOfInt) {
		// read last pointer

		memcpy(&tempPointer, pageData, sizeof(int));
		pointers.push_back(tempPointer);
		pageData += sizeof(int);

		return keys;
	} else {
		return keys;
	}
}

template<class T>
map<T,vector<RID> > IndexManager::getKeysRIDMap(const void *data, int freeSpace, const Attribute &attribute) {
	map<T,vector<RID> > mapKeyRIDs;

	char *pageData = (char *) data;

	int sizeOfKey = 4;
	RID tempRID;
	T tempKey;

	int spaceOccupied = PAGE_SIZE - freeSpace - sizeOfInt - sizeOfChar - sizeOfInt;

	while (spaceOccupied > 0) {

		// Read key
		memcpy(&tempKey, pageData, sizeOfKey);
		pageData += sizeOfKey;

		if ( mapKeyRIDs.find(tempKey) == mapKeyRIDs.end() ) {
		  // not found
			vector<RID> vect;
			mapKeyRIDs[tempKey] = vect;
		}

		// Read rid
		memcpy(&tempRID, pageData, sizeof(RID));
		pageData += sizeof(RID);

		mapKeyRIDs[tempKey].push_back(tempRID);

		// iterate through page
		spaceOccupied -= sizeOfInt+sizeof(RID);
	}
	return mapKeyRIDs;
}

template<>
map<string,vector<RID> > IndexManager::getKeysRIDMap(const void *data, int freeSpace, const Attribute &attribute) {
	map<string,vector<RID> > mapKeyRIDs;

	char *pageData = (char *) data;

	int sizeOfKey = 0;
	RID tempRID;
	string tempStringKey;

	int spaceOccupied = PAGE_SIZE - freeSpace - sizeOfInt - sizeOfChar - sizeOfInt;

	while (spaceOccupied > 0) {

		// Read key
		memcpy(&sizeOfKey, pageData, sizeof(int));
		char* dummy = (char*)calloc(sizeOfKey,1);
		memcpy(dummy, pageData+ sizeof(int), sizeOfKey);
		pageData += sizeof(int)+sizeOfKey;

		tempStringKey = string(dummy,sizeOfKey);
		free(dummy);

		if ( mapKeyRIDs.find(tempStringKey) == mapKeyRIDs.end() ) {
		  // not found
			vector<RID> vect;
			mapKeyRIDs[tempStringKey] = vect;
		}

		// Read rid
		memcpy(&tempRID, pageData, sizeof(RID));
		pageData += sizeof(RID);

		mapKeyRIDs[tempStringKey].push_back(tempRID);

		// iterate through page
		spaceOccupied -= sizeof(int)+sizeOfKey+sizeof(RID);
	}
	return mapKeyRIDs;
}

template<class T>
void IndexManager::printRecursive2(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth) {
	void *data = calloc(PAGE_SIZE, 1);

	ixfileHandle.fileHandle.readPage(pageNum, data);

	char *currPage = (char *) data;

	char pageType;
	memcpy(&pageType, currPage + PAGE_SIZE - sizeOfInt - sizeOfChar, sizeOfChar);
	int freeSpace=getFreeSpace(currPage);

	if (pageType == INTERNAL_NODE) {

		for(int i=0; i<depth;i++){
			cout<<"\t";
		}
		cout << "{"<<endl;
		for(int i=0; i<depth;i++){
			cout<<"\t";
		}
		cout<<"\"keys\":[";

		vector<int> pointers;
		vector<T> keys;

		keys = getKeysPointers<T>(data, freeSpace, attribute, pointers);
		free(data);

		for (int i = 0; i < keys.size(); i++) {
			cout << "\"";
			cout << (T)keys[i];
			cout << "\"";
			if (i != keys.size() - 1) {
				cout << ",";
			}
		}
		cout << "]," << endl;

		for(int i=0; i<depth;i++){
			cout<<"\t";
		}
		cout << "\"children\":[";
		cout<<endl;


		printRecursive2<T>(ixfileHandle, attribute, pointers[0], depth+1);
		for (int i = 1; i < pointers.size(); i++) {
			cout << ","<<endl;
			printRecursive2<T>(ixfileHandle, attribute, pointers[i], depth+1);
		}

		cout << endl;
		for(int i=0; i<depth;i++){
			cout<<"\t";
		}
		if(depth!=0){
			cout<<"]}";
		}else{
			cout<<"]"<<endl<<"}"<<endl;
		}

	} else if (pageType == LEAF_NODE) {

		for(int i=0; i<depth;i++){
			cout<<"\t";
		}
		cout << "{\"keys\":[";

		map<T,vector<RID> > mapKeyRIDs;
		mapKeyRIDs = getKeysRIDMap<T>(data, freeSpace, attribute);
		free(data);

		int count = 0;
		for (auto &entry: mapKeyRIDs) {
			count++;
			cout << "\"";
			cout << entry.first;
//			cout<< "\"" + ":" + "(";
			cout << ":[";

			for(int i = 0; i< entry.second.size() ;i++){

				cout << "(";
				cout << entry.second[i].pageNum;
				cout << ",";
				cout << entry.second[i].slotNum;
				cout << ")";

				if(i != entry.second.size()-1){
					cout<<",";
				}
			}
			cout << "]\"";
			if (count != mapKeyRIDs.size()) {
				cout << ",";
			}
		}
		cout << "]}";
		if(depth == 0) cout <<endl;

	} else {
		cout << "printRecursive: Unknown page!" << endl;
	}

}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
					  const Attribute &attribute,
					  const void *lowKey,
					  const void *highKey,
					  bool lowKeyInclusive,
					  bool highKeyInclusive,
					  IX_ScanIterator &ix_ScanIterator) {

	if(!ixfileHandle.fileHandle.isOpen()){
		return -1;
	}

	//traverse tree and get pageNum
	int newPageNum;
	void* newRootEntry = NULL;
	RID rid;
	int startPageNumber = 0;
	int searchOffset = 0;
	if(lowKey != NULL){
		traverseTree(ixfileHandle, ixfileHandle.rootPageNum, attribute, lowKey, rid, newPageNum, newRootEntry, SEARCH_OPERATION, searchOffset, startPageNumber);
//		cout<<"STARTED SCAN AT "<<startPageNumber<<endl;
	}else{
//		cout<<"STARTED SCAN AT "<<startPageNumber<<endl;
	}

	ix_ScanIterator.allocateSpace();
	return ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, startPageNumber, searchOffset);

//	return 0;
}

IX_ScanIterator::IX_ScanIterator() {
	this->isOpen = false;
//	this->currentPageData = (char*)calloc(PAGE_SIZE,1);
	this->ixfileHandle = 0;
	this->offset = 0;

	this->lowKeyInt = 0;
	this->highKeyInt = 0;;

	this->lowKeyFloat = 0;
	this->highKeyFloat = 0;

	this->lowKeyString = "";
	this->highKeyString = "";

	this->lowKeyInclusive = false;
	this->highKeyInclusive = false;

	this->occupationSize = 0;
	this->reachedSearchRange = false;
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator:: initialize(IXFileHandle &ixfileHandle,
		  const Attribute &attribute,
		  const void *lowKey,
		  const void *highKey,
		  bool lowKeyInclusive,
		  bool highKeyInclusive,
		  int startPageNumber,
		  int searchOffset) {

	// initialize rid to 0,0
	this->offset = searchOffset;

	// store all parameters
	this->ixfileHandle = &ixfileHandle;
	this->attribute = attribute;

	this->lowKey = (void*) lowKey;
	this->highKey = (void*) highKey;

	if(attribute.type == TypeInt){
		if(lowKey != NULL){
			memcpy(&(this->lowKeyInt), lowKey, sizeof(int));
		}else{
			this->lowKeyInt = INT_MIN;
		}
		if(highKey != NULL){
			memcpy(&(this->highKeyInt), highKey, sizeof(int));
		}else{
			this->highKeyInt = INT_MAX;
		}
	}else if(attribute.type == TypeReal){
		if(lowKey != NULL){
			memcpy(&(this->lowKeyFloat), lowKey, sizeof(float));
		}else{
			this->lowKeyFloat = INT_MIN;
		}
		if(highKey != NULL){
			memcpy(&(this->highKeyFloat), highKey, sizeof(float));
		}else{
			this->highKeyFloat =  INT_MAX;
		}
	}else{
		int keySize = 0;
		if(lowKey != NULL){
			memcpy(&keySize, lowKey, sizeof(int));
			char* input = (char*)calloc(keySize,1);
			memcpy(input, (char*)lowKey+ sizeof(int), keySize);

			this->lowKeyString = string(input,keySize);
			free(input);
		}else{
			this->lowKeyString = "";
		}
		if(highKey != NULL){
			memcpy(&keySize, highKey, sizeof(int));
			char* input = (char*)calloc(keySize,1);
			memcpy(input, (char*)highKey+ sizeof(int), keySize);

			this->highKeyString = string(input,keySize);
			free(input);
		}else{
			// change this
			this->highKeyString = "";
		}
	}


	this-> lowKeyInclusive = lowKeyInclusive;
	this-> highKeyInclusive = highKeyInclusive;

	this->currentPageNum = startPageNumber;
	this->ixfileHandle->fileHandle.readPage(startPageNumber, this->currentPageData);
	this->occupationSize = PAGE_SIZE-sizeOfInt-sizeOfChar-sizeOfInt-getFreeSpace(this->currentPageData);
	this->isOpen = true;

	RC rc = 0;
	if(this->occupationSize == 0){
		rc =  setNextEntryOffset(1);
	}
	if(rc == -1){
		this->offset = -1;
	}else{
		getIntoSearchRange();
	}

	return 0;
}

RC IX_ScanIterator::getIntoSearchRange(){

	this->offset = 0;
	if(this->lowKey == NULL){
//		cout<<"Found the range!"<<endl;
		this->reachedSearchRange = true;
		return 0;
	}

	int currentEntrySize = sizeof(int)+sizeof(RID);
	int varcharKeySize = 0;

	if(this->lowKeyInclusive){
		int i=0;
		while(compareValues(this->currentPageData+this->offset, this->attribute, (char*)this->lowKey) != 0){
			if(this->attribute.type == AttrType::TypeVarChar){
				memcpy(&varcharKeySize,this->currentPageData+this->offset,sizeof(int));
				currentEntrySize = sizeof(int)+varcharKeySize+sizeof(RID);
			}
			i++;
			if(setNextEntryOffset(currentEntrySize) == -1 || this->offset == -1){
//				cout<<"Range not found!"<<endl;
				return -1;
			}
		}
	}else{
		while(compareValues(this->currentPageData+this->offset, this->attribute, (char*)this->lowKey) != 1){
			if(this->attribute.type == AttrType::TypeVarChar){
				memcpy(&varcharKeySize,this->currentPageData+this->offset,sizeof(int));
				currentEntrySize = sizeof(int)+varcharKeySize+sizeof(RID);
			}
			if(setNextEntryOffset(currentEntrySize) == -1 || this->offset == -1){
//				cout<<"Range not found!"<<endl;
				return -1;
			}
		}
	}
//	cout<<"Found the range!"<<endl;
	this->reachedSearchRange = true;
	return 0;
}

RC IX_ScanIterator::setNextEntryOffset(int moveOffsetBy) {

	if((this->offset + moveOffsetBy) < this->occupationSize){
		this->offset += moveOffsetBy;
		return 0;
	}else{
		this->offset = 0;

		int nextPageNumber;
		memcpy(&nextPageNumber, this->currentPageData+PAGE_SIZE-sizeOfInt-sizeOfChar-sizeOfInt,sizeof(int));
		if(nextPageNumber == -1 || nextPageNumber == 0){
			this->offset = -1;
			return -1;
		}

		this->currentPageNum = nextPageNumber;
		this->ixfileHandle->fileHandle.readPage(this->currentPageNum, this->currentPageData);
		this->occupationSize = PAGE_SIZE-sizeOfInt-sizeOfChar-sizeOfInt-getFreeSpace(this->currentPageData);

		while(this->occupationSize == 0){
//			cout<<"emty page skipped"<<endl;
			RC rc = setNextEntryOffset(1);
			if(rc == -1) return -1;
		}
	}
}

template <class myType>
bool IX_ScanIterator::compare(myType a, CompOp compOp, myType b) {
	switch(compOp){
		case EQ_OP :
			return (a == b);
		case LT_OP :
			return (a < b);
		case LE_OP :
			return (a <= b);
		case GT_OP :
			return (a > b);
		case GE_OP :
			return (a >= b);
		case NE_OP :
			return (a != b);
		case NO_OP :
			return true;
	}
	return false;
}

bool IX_ScanIterator::compareStrings(string a, CompOp compOp, string b){
	switch(compOp){
		case EQ_OP :
			return (a == b);
		case LT_OP :
			return (a.compare(b) < 0);
		case LE_OP :
			return (a.compare(b) <= 0);
		case GT_OP :
			return (a.compare(b) > 0);
		case GE_OP :
			return (a.compare(b) >= 0);
		case NE_OP :
			return (a != b);
		case NO_OP :
			return true;
	}
	return false;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

	if(setNextEntryOffset(0) != SUCCESS || this->offset == -1){
//		cout<<"EOF reached!"<<endl;
		return -1;
	}

	if(this->reachedSearchRange == true ){
		// check only higher key
		// set offset to read key + rid size
		CompOp compOp = GE_OP;

		if(this->highKeyInclusive == false){
			compOp = GT_OP;
		}

		bool comparisionResultHigh = false;
		if(this->highKey == NULL){
			comparisionResultHigh = true;
		}

		if(this->attribute.type != TypeVarChar){

			if(this->attribute.type == TypeInt){
				int compKey;
				memcpy(&compKey, this->currentPageData + this->offset, sizeof(int));
				if(this->highKey != NULL)comparisionResultHigh = compare<int>(this->highKeyInt, compOp, compKey);
			}else{
				float compKey;
				memcpy(&compKey, this->currentPageData + this->offset, sizeof(float));
				if(this->highKey != NULL)comparisionResultHigh = compare<float>(this->highKeyFloat, compOp, compKey);
			}

			if(comparisionResultHigh){
				memcpy(key, this->currentPageData + this->offset, sizeof(int));
				memcpy(&rid, this->currentPageData + this->offset + sizeof(int), sizeof(RID));

				setNextEntryOffset(sizeof(int)+sizeof(RID));
				return 0;
			}
		}else{
			int compKeySize;

			memcpy(&compKeySize, this->currentPageData + this->offset, sizeof(int));
			char* compKeyChars = (char*) calloc(compKeySize,1);
			memcpy(compKeyChars, this->currentPageData + this->offset + sizeof(int), compKeySize);

			string compKey(compKeyChars,compKeySize);

			if(this->highKey != NULL)comparisionResultHigh = compare<string>(this->highKeyString, compOp, compKey);

			if(comparisionResultHigh){
				memcpy(key, this->currentPageData + this->offset, sizeof(int) + compKeySize);
				memcpy(&rid, this->currentPageData + this->offset + sizeof(int) + compKeySize, sizeof(RID));

				setNextEntryOffset(sizeof(int) + compKeySize + sizeof(RID));
				return 0;
			}
		}

		this->offset = -1;
		return -1;

	}else{
		// tree traversal did not happen properly
		cout<<"tree traversal did not happen properly"<<endl;
	}

}

RC IX_ScanIterator::close() {
	if(this->isOpen){
		this->isOpen = false;
		free(this->currentPageData);
		return 0;
	}
	cout<< "Iterator is not available to close!"<<endl;
	return -1;
}
