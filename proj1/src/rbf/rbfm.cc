#include "rbfm.h"

#define slotOffsetSize 2 //size of- Corresponds to the offset of the record in the page
#define slotNumberSize 2 //size of- Corresponds to the length of the record in the page
#define freeSpaceSize 2
#define numSlotsSpace 2
#define varcharSize 4

#include <iostream>
#include "rbfm_util.h"
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager* RecordBasedFileManager::_pbf_manager = 0;


RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if(!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	_pbf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	//------------------------------------------------------------------------------------------------------------------------------------
	//																DESTROY STUFF
	//------------------------------------------------------------------------------------------------------------------------------------
	//todo jhjhj
}

RC RecordBasedFileManager::createFile(const string &fileName) {

	_pbf_manager->getAccess(true,SHARED_KEY);
	if(_pbf_manager->createFile(fileName) == 0){
		_pbf_manager->getAccess(false,SHARED_KEY);
		return 0;
	}
	_pbf_manager->getAccess(false,SHARED_KEY);
	return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {

	if(_pbf_manager->destroyFile(fileName) == 0){
		return 0;
	}
	return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {

	_pbf_manager->getAccess(true,SHARED_KEY);
	if(_pbf_manager->openFile(fileName,fileHandle) == 0){
		_pbf_manager->getAccess(false,SHARED_KEY);
		return 0;
	}
	_pbf_manager->getAccess(false,SHARED_KEY);
	return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {

	_pbf_manager->getAccess(true,SHARED_KEY);
	if(_pbf_manager->closeFile(fileHandle) == 0){
		_pbf_manager->getAccess(false,SHARED_KEY);
		return 0;
	}
	return -1;
}


short RecordBasedFileManager::encodeRecord(void* record,const void* data, const vector<Attribute> &recordDescriptor,char flagByte, RID recordActualRID, bool debugMode){

	char* inputData = (char* )data;

	short numFields = recordDescriptor.size();
	int numNullBites = ceil((float)numFields / 8.0);

	//-------SET flag byte----------------------------------


	if(flagByte == TOMBSTONE_FLAG){
		short sizeOfRecord = sizeof(char)+sizeof(RID);
		char* recordCopy = (char*)calloc(sizeOfRecord,1);

		memcpy(recordCopy,&flagByte,sizeof(char));

		// Data shd consist only new RID generated
		memcpy(recordCopy+sizeof(char),data,sizeof(RID));


		memcpy(record,recordCopy,sizeOfRecord);
		free(recordCopy);
		return sizeOfRecord;
	}
	//------------------------------------------------------


	//------------------------------------------------------

	char* recordNullBites = (char*) calloc(numNullBites,1);
	memcpy(recordNullBites, data, numNullBites);

	//------------------------------------------------------


	short offsetArray[numFields];

	char* recordData = (char*) calloc(PAGE_SIZE,1);
	int fieldsTotalLength = 0;
	int lastNonZeroOffset = sizeof(char)+ sizeof(RID) + numNullBites +sizeof(offsetArray);

	inputData+=numNullBites;

	for(int i=0;i<numFields;i++){

		////////// DEBUG MODE////////
		if(debugMode){
			cout<<recordDescriptor[i].name<<": ";
		}
		////////// DEBUG MODE////////

		if(getBit(recordNullBites[(i)/8],((i)%8)) == true){
			offsetArray[i] = lastNonZeroOffset;
			////////// DEBUG MODE////////
			if(debugMode){
				cout<<"NULL ";
			}
			////////// DEBUG MODE////////
		}else{
			if(recordDescriptor[i].type != AttrType::TypeVarChar){

				offsetArray[i] = lastNonZeroOffset + recordDescriptor[i].length;

				memcpy(recordData, inputData,recordDescriptor[i].length);

				////////// DEBUG MODE////////
				if(debugMode){
					if(recordDescriptor[i].type == AttrType::TypeInt){
						int tempValue=0;
						memcpy(&tempValue, inputData,recordDescriptor[i].length);

						cout<<tempValue<<" ";
					}
					else{
						float tempValue=0.0;
						memcpy(&tempValue, inputData,recordDescriptor[i].length);

						cout<<tempValue<<" ";
					}
				}
				////////// DEBUG MODE////////

				inputData+= recordDescriptor[i].length;
				recordData+= recordDescriptor[i].length;
				fieldsTotalLength+= recordDescriptor[i].length;

			}else{

				unsigned varcharActualSize = 0;
				memcpy(&varcharActualSize,inputData, varcharSize);
				inputData+= varcharSize;

				memcpy(recordData,inputData,varcharActualSize);

				////////// DEBUG MODE////////
				if(debugMode){
					char* tempValue = (char*)calloc(varcharActualSize,1);
					memcpy(tempValue, inputData,varcharActualSize);

					string varcharValue;
					varcharValue.append(tempValue);

					cout<< varcharValue<<" ";
					free(tempValue);
				}
				////////// DEBUG MODE////////


				offsetArray[i] = lastNonZeroOffset + varcharActualSize;
				inputData+= varcharActualSize;
				recordData+= varcharActualSize;
				fieldsTotalLength+= varcharActualSize;

			}
			lastNonZeroOffset = offsetArray[i];
		}

	}

	//------------------------------------------------------
	// append offsetArea NULL Bits area Data Area

	char* recordCopy = (char*)calloc(PAGE_SIZE,1);

	memcpy(recordCopy,&flagByte,sizeof(char));
	recordCopy += sizeof(char);

	memcpy(recordCopy,&recordActualRID,sizeof(recordActualRID));
	recordCopy += sizeof(recordActualRID);

	memcpy(recordCopy,recordNullBites,numNullBites);
	recordCopy += numNullBites;

	memcpy(recordCopy,offsetArray,sizeof(offsetArray));
	recordCopy += sizeof(offsetArray);

	recordData-= fieldsTotalLength;
	memcpy(recordCopy,recordData,fieldsTotalLength);
	recordCopy += fieldsTotalLength;



	short sizeOfRecord = (sizeof(char) + sizeof(recordActualRID) + sizeof(offsetArray) + numNullBites+ fieldsTotalLength);
	recordCopy -= sizeOfRecord;



	memcpy(record,recordCopy,sizeOfRecord);
	free(recordCopy);
	free(recordNullBites);
	free(recordData);
	return sizeOfRecord;
}

bool isTombStone(FileHandle &fileHandle, const RID &rid, RID &pointedRID){

	void* page = calloc(PAGE_SIZE,1);
	if(fileHandle.readPage(rid.pageNum, page) != 0){
		//cout<<"isTombStone - Reading page issue"<<endl;
		free(page);
		return -10;
	}

	short offset;
	memcpy(&offset, (char *) page + (PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
			((rid.slotNum + 1) * (slotOffsetSize + slotNumberSize)) + slotNumberSize, sizeof(short));
	if(offset == OFFSET_FOR_DELETED) {
		free(page);
		return false;
	}

	char flag;
	memcpy(&flag, (char*)page+offset, sizeof(char));

	if(flag == TOMBSTONE_FLAG){
		memcpy(&pointedRID,(char*)page+offset+sizeof(char),sizeof(RID));
		free(page);
		return true;
	}

	free(page);
	return false;
}

RC findActualRID(FileHandle &fileHandle,const RID &inputRid, RID &outputRid, void* page){

//	//cout << "findActualRID: " << inputRid.pageNum << " " << inputRid.slotNum << endl;
	if(!fileHandle.isOpen()){
		//cout<<"findActualRID - file not open!"<<endl;
		return -1;
	}

	if(fileHandle.readPage(inputRid.pageNum, page) != 0){
		//cout<<"findActualRID - Reading page issue"<<inputRid.pageNum<<"  "<<inputRid.slotNum<<endl;
		//cout<<"pages: "<<fileHandle.getNumberOfPages()<<endl;
		return -10;
	}

	short offset;
	memcpy(&offset, (char *) page + (PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
			((inputRid.slotNum + 1) * (slotOffsetSize + slotNumberSize)) + slotNumberSize, sizeof(short));
	if(offset == OFFSET_FOR_DELETED) {
//		//cout<<" Deleted RID as input for findActualRID"<<endl;
		return -1;
	}

	char flag;
	memcpy(&flag, (char*)page+offset, sizeof(char));

	if(flag == TOMBSTONE_FLAG){
		RID ridPointed;
		memcpy(&ridPointed,(char*)page+offset+sizeof(char),sizeof(RID));
		return findActualRID(fileHandle,ridPointed, outputRid, page);
	}else{
		outputRid = inputRid;
		return 0;
	}
	return -1;
}

template <class myType>
bool compare(myType a, CompOp compOp, myType b) {
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

// CHANGE! //
bool compareStrings(string a, CompOp compOp, string b){
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

//==============================================//==============================================//==============================================

short RecordBasedFileManager::decodeRecord(void* record, void* data, const vector<Attribute> &recordDescriptor, FileHandle &fileHandle){

	char* inputData = (char* )record;

	unsigned short numFields = recordDescriptor.size();

	//------READ flag ---------------------------------

	char flag;
	memcpy(&flag, inputData, sizeof(char));
	inputData+=sizeof(char);

	if(flag != NEAT_RECORD_FLAG && flag != REDIRECTED_RECORD_FLAG){
		//cout<< " decodeRecord - Wrong flag as input"<<endl;
		//cout<<"flag is: "<< (int)flag<<endl;
		if(flag == TOMBSTONE_FLAG){
			//cout<<" decodeRecord - Flag corresponds to tombstone record"<<endl;
		}
	}

	//----------- READ RID-------------------------

	RID dummyRID;
	memcpy(&dummyRID, inputData, sizeof(RID));
	inputData+=sizeof(RID);

	//------------------------------------------------------

	unsigned int numNullBites = ceil((float)numFields / 8.0);

	char* recordNullBites = (char*) calloc(numNullBites,1);     // free this memory
	memcpy(recordNullBites, inputData, numNullBites);
	inputData+=numNullBites;

	//------------------------------------------------------

	short offsetArray[numFields];
	memcpy(offsetArray, inputData, sizeof(offsetArray));
	inputData+=sizeof(offsetArray);

	//------------------------------------------------------

	//formatted data part
	char* formattedData = (char*) calloc(PAGE_SIZE,1);		// free this memory
	int formattedData_length = 0;

	for(int i=0;i<numFields;i++){
		if(getBit(recordNullBites[(i)/8],((i)%8)) == true){
			// Null value field

		}else{

			unsigned lengthField = 0; // must be unsigned
			if(i != 0){
				lengthField = offsetArray[i] - offsetArray[i-1];
			}else{
				lengthField = offsetArray[i] - (sizeof(char)+ sizeof(RID) + numNullBites +sizeof(offsetArray));
			}

			// add length of field if type is varchar
			if(recordDescriptor[i].type == AttrType::TypeVarChar){

				memcpy(formattedData,&lengthField,4);					// 4 hardcoded for num of bites for length of varchar representation
				formattedData+=4;										// 4 hardcoded for num of bites for length of varchar representation
				formattedData_length+=4;
			}

			// add value of the field - read from input data - lengthField bytes
			memcpy(formattedData,inputData,lengthField);
			formattedData+=lengthField;
			formattedData_length+=lengthField;
			inputData+=lengthField;
		}
	}

	// pull back pointers to initial positions

	formattedData -= formattedData_length;

	//full data part
	char* formattedRecord = (char*) calloc(PAGE_SIZE,1);		// free this memory

	//copy null bytes
	memcpy(formattedRecord, recordNullBites, numNullBites);

	//copy data bytes
	memcpy(formattedRecord+numNullBites, formattedData, formattedData_length);

	int totalSize = sizeof(char)+ numNullBites+formattedData_length;

	// handover formattedRecord - copy to the given handle
	memcpy(data, formattedRecord, totalSize);

	free(formattedRecord);
	free(formattedData);
	free(recordNullBites);
	return totalSize;
}

RC RecordBasedFileManager::freeSpaceDecrease(void* pageData,unsigned sizeOfRecord){
	short initialFreeSpace;
	memcpy(&initialFreeSpace, ((char*)pageData)+PAGE_SIZE -freeSpaceSize, freeSpaceSize);
	initialFreeSpace -= (sizeOfRecord+ slotNumberSize + slotOffsetSize);
	memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize, &initialFreeSpace, freeSpaceSize);

	return 0;
}

RC RecordBasedFileManager::findDeletedSlot(short* slotNum, const void* pageData, const short numSlots){

	short slotOffset;
	for(int i=0; i<numSlots; i++){

		memcpy(&slotOffset, ((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace - ((i+1) *(slotNumberSize + slotOffsetSize)) + slotNumberSize,slotOffsetSize);

		if(slotOffset == OFFSET_FOR_DELETED){
			*slotNum = i;
			return 0;
		}
	}

	return 0;

}

short RecordBasedFileManager::addSlot(void* pageData, short* slotNum, void* record, unsigned sizeOfRecord, bool newPage){
	short initialNumSlots;
	memcpy(&initialNumSlots, ((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace, numSlotsSpace);

	short slotOffset;
	if(newPage){
		slotOffset = 0;
	}else{
		slotOffset = 0;

		short tempPrevSlotOffset;
		short tempPrevRecordSize;
		for(int i=0; i<initialNumSlots; i++){
			memcpy(&tempPrevSlotOffset, ((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace - ((i+1) *(slotNumberSize + slotOffsetSize)) + slotNumberSize, slotOffsetSize);
			memcpy(&tempPrevRecordSize, ((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace - ((i+1) *(slotNumberSize + slotOffsetSize)), slotNumberSize);
			if(tempPrevSlotOffset!=OFFSET_FOR_DELETED &&  (tempPrevSlotOffset + tempPrevRecordSize)>slotOffset){
				slotOffset = tempPrevSlotOffset + tempPrevRecordSize;
			}
		}
	}

	// Will be done at later stage
//	//insert record
//	memcpy(((char*)pageData)+slotOffset,record,sizeOfRecord);


	// Update directory

	// finalize slot
	memcpy(slotNum,&initialNumSlots,sizeof(short));
	if(!newPage){
		if(findDeletedSlot(slotNum, pageData, initialNumSlots) !=SUCCESS){
			//cout<<"addSlotInsertRecord - finding Deleted Slot issue"<<endl;
			return -1;
		}
	}

	// increase slot count
	if(*slotNum == initialNumSlots){
		initialNumSlots++;
		memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace, &initialNumSlots, numSlotsSpace);
	}

	// insert record size at slot num
	memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace - ((*slotNum+1) *(slotNumberSize + slotOffsetSize)), &sizeOfRecord, slotNumberSize);
	// insert record offset at slot num
	memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace - ((*slotNum+1) *(slotNumberSize + slotOffsetSize)) + slotNumberSize, &slotOffset, slotOffsetSize);

	return slotOffset;
}

int RecordBasedFileManager::searchFreePage(FileHandle &fileHandle, short spaceRequired){

	short tempFreeSpace = 0;
	for(int i=fileHandle.getNumberOfPages()-1; i>=0 ;i--){
		tempFreeSpace = getFreeSpace(fileHandle,i);
		if(tempFreeSpace>=spaceRequired){
			return i;
		}
	}
	return -1;
}

short RecordBasedFileManager::getFreeSpace(FileHandle &fileHandle,int pageNum){
	void* pageData = calloc(PAGE_SIZE,1);
	fileHandle.readPage(pageNum, pageData);

	short initialFreeSpace;
	memcpy(&initialFreeSpace, ((char*)pageData)+PAGE_SIZE -freeSpaceSize, freeSpaceSize);

	free(pageData);
	return initialFreeSpace;
}

RC RecordBasedFileManager::createAppendPage(FileHandle &fileHandle,void* record,short sizeOfRecord, RID &rid , char neatRecord, const void* data, const vector<Attribute> recordDescriptor, bool debug){
	void* pageData = calloc(PAGE_SIZE,1);
	short freeSpace = PAGE_SIZE- sizeof(short) -sizeof(numSlotsSpace);
	short numSlots = 0;
	memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize, &freeSpace, freeSpaceSize);
	memcpy(((char*)pageData)+PAGE_SIZE -freeSpaceSize -numSlotsSpace, &numSlots, numSlotsSpace);

	// free space decrease
	freeSpaceDecrease(pageData, sizeOfRecord);

	// add slot to directory
	short slotNum = 0;
	short slotOffset = addSlot(pageData, &slotNum,record,sizeOfRecord,true);

	// populate rid
	rid.pageNum = fileHandle.getNumberOfPages();
	rid.slotNum = slotNum;

	// insert record
	if(neatRecord == NEAT_RECORD_FLAG){
		// re encode the data in case of insert record to include newly found rid
		sizeOfRecord = encodeRecord(record, data, recordDescriptor, neatRecord, rid, debug);
	}
	memcpy(((char*)pageData)+slotOffset,record,sizeOfRecord);

	// append page
	if(fileHandle.appendPage(pageData)!=SUCCESS){
		free(pageData);
		return -1;
	}

	free(pageData);
	return 0;
}

RC RecordBasedFileManager::insertFormattedRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid, char flag, RID ridToInsert){
	if(!fileHandle.isOpen() || flag == TOMBSTONE_FLAG){
		return -1;
	}

	// formatDataToRecord -------------------------------------------
	bool debug = false;
	void* record = calloc(PAGE_SIZE,1);
	short sizeOfRecord = encodeRecord(record, data, recordDescriptor, flag, ridToInsert, debug);


	// find free page (contiguous page) size + 4bytes -----------------

	int numPages = fileHandle.getNumberOfPages();
	if(numPages == 0){

		// create page
		createAppendPage(fileHandle, record, sizeOfRecord, rid, flag, data, recordDescriptor, debug);

	}else{

		int pageNum = searchFreePage(fileHandle,sizeOfRecord+slotNumberSize+slotOffsetSize);
		if(pageNum > -1){
			// open page
			void* pageData = calloc(PAGE_SIZE,1);
			fileHandle.readPage(pageNum, pageData);

			// free space decrease
			freeSpaceDecrease(pageData, sizeOfRecord);

			//add slot to directory
			short slotNum = 0;
			short slotOffset = addSlot(pageData, &slotNum,record,sizeOfRecord,false);

			rid.pageNum = pageNum;
			rid.slotNum = slotNum;

			// insert record
			if(flag == NEAT_RECORD_FLAG){
				// re encode the data in case of insert record to include newly found rid
				sizeOfRecord = encodeRecord(record, data, recordDescriptor, flag, rid, debug);
			}
			memcpy(((char*)pageData)+slotOffset,record,sizeOfRecord);

			fileHandle.writePage(pageNum,pageData);
			free(pageData);
		}else{
			// append page
			createAppendPage(fileHandle, record, sizeOfRecord, rid, flag, data, recordDescriptor, debug);
		}

	}

	free(record);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	RID dummyRID;
	return insertFormattedRecord(fileHandle, recordDescriptor, data, rid, NEAT_RECORD_FLAG, dummyRID);
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	if(!fileHandle.isOpen()){
		return -1;
	}

	void* pageData = calloc(PAGE_SIZE,1);
	RID trueRID;

	if(findActualRID(fileHandle, rid, trueRID,pageData) != SUCCESS){
		free(pageData);
		return -1;
	}

//	//cout<< rid.slotNum << endl;

	unsigned short offset;
	memcpy(&offset,(char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((trueRID.slotNum+1)*(slotOffsetSize+slotNumberSize))+slotNumberSize,sizeof(short));

	void* recordPtr = (char*)pageData+offset;

	int dataSize = decodeRecord(recordPtr, data, recordDescriptor, fileHandle);
	if(dataSize <0){
		//cout<<"readRecord- No data found"<<endl;
		free(pageData);
		return -1;
	}

	free(pageData);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

	bool debug = true;
	void* record = calloc(PAGE_SIZE,1);
	RID dummyRID;
	unsigned short sizeOfRecord = encodeRecord(record, data, recordDescriptor, NEAT_RECORD_FLAG, dummyRID, debug);

	if(sizeOfRecord > 0){
		//DO NOT REMOVE THIS //cout
		cout<<endl;
		free(record);
		return 0;
	}

	free(record);
	return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
										const RID &rid) {
	if(!fileHandle.isOpen()) {
		//cout << "deleteRecord: file not open" << endl;
		return -1;
	}

	void *page = calloc(PAGE_SIZE, 1);
	RID currRID = rid;

	bool toContinue = true;
	while(toContinue) {

// 		//cout << "currRID: " << currRID.pageNum << " " << currRID.slotNum << endl;
		if (fileHandle.readPage(currRID.pageNum, page) != 0) {
			//cout << "deleteRecord - Reading page issue" << endl;
			free(page);
			return -1;
		}
// 		PrintBytes((char *)page, 150, 0);
// 		//cout << endl;
// 		PrintBytes((char *)page, 4096, 4050);

		short offset;
		memcpy(&offset, (char *) page + (PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
				((currRID.slotNum + 1) * (slotOffsetSize + slotNumberSize)) + slotNumberSize, sizeof(short));

		char flag;
		memcpy(&flag, (char *) page + offset, sizeof(char));

		RID ridPointed = currRID;

		if (flag == TOMBSTONE_FLAG) {
			memcpy(&ridPointed, (char *) page + offset + sizeof(char), sizeof(RID));
			defragment(currRID, sizeof(RID) + sizeof(char), page);
		} else if (flag != TOMBSTONE_FLAG) {
			short recordSize;
			memcpy(&recordSize, (char *) page + (PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
					((currRID.slotNum + 1) * (slotOffsetSize + slotNumberSize)),
					sizeof(short));
			defragment(currRID, recordSize, page);
			toContinue = false;
		}


		offset = OFFSET_FOR_DELETED;
//		//cout <<endl << "offset pos: " << PAGE_SIZE - ((PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
//				   ((currRID.slotNum + 1) * (slotOffsetSize + slotNumberSize)) + slotNumberSize)<<endl;
		// Updating offset in directory entry
		memcpy((char *) page + (PAGE_SIZE - freeSpaceSize - numSlotsSpace) -
			   ((currRID.slotNum + 1) * (slotOffsetSize + slotNumberSize)) + slotNumberSize, &offset,
			   sizeof(short));
// 		//cout << endl << "After defrag" << endl;
// 		PrintBytes((char *)page, 150, 0);
// 		//cout << endl;
// 		PrintBytes((char *)page, 4096, 4050);
		fileHandle.writePage(currRID.pageNum, page);
		currRID = ridPointed;
	}
	free(page);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	if(!fileHandle.isOpen()){
		return -1;
	}

	// formatDataToRecord -------------------------------------------
	bool debug = false;
	void* record = calloc(PAGE_SIZE,1);
	short newSizeOfRecord = encodeRecord(record, data, recordDescriptor, REDIRECTED_RECORD_FLAG ,rid, debug);

	// Read page of the original record
	void* pageData = calloc(PAGE_SIZE,1);
	if(fileHandle.readPage(rid.pageNum, pageData) != 0){
		free(record);
		return -10;
	}

	// Read free space in the page
	unsigned short freeSpaceInPage;
	memcpy(&freeSpaceInPage,(char*)pageData + (PAGE_SIZE-freeSpaceSize),sizeof(short));

	// Read original size of record
	unsigned short prevRecordSize;
	memcpy(&prevRecordSize,(char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((rid.slotNum+1)*(slotOffsetSize+slotNumberSize)),sizeof(short));

	// read offset of original record in page
	unsigned short prevRecordOffset;
	memcpy(&prevRecordOffset,(char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((rid.slotNum+1)*(slotOffsetSize+slotNumberSize))+slotNumberSize,sizeof(short));

	// control flow based on size of record update -----------------

	// Before update, delete any pointed data, if the rid being updated is a tombstone

	RID pointedRID;
	if(isTombStone(fileHandle, rid, pointedRID)){
		if(deleteRecord(fileHandle, recordDescriptor, pointedRID) != SUCCESS){
			free(record);
			free(pageData);
			//cout<<"updateRecord- deleteRecord failed"<<endl;
			return -1;
		}
	}

	// Update

	if(newSizeOfRecord == prevRecordSize){
		// insert at the same location
		memcpy((char*)pageData+prevRecordOffset,record,newSizeOfRecord);

	}else if(newSizeOfRecord > prevRecordSize){

		// if page has enough space for updates record
		if(newSizeOfRecord-prevRecordSize <= freeSpaceInPage){
			// call defragmentation
			if(defragment(rid, -(newSizeOfRecord-prevRecordSize) ,pageData) != SUCCESS){
				//cout<<"Defragmentation error during updateRecord process"<<endl;
				free(record);
				free(pageData);
				return -1;
			}

			// insert at the same location
			memcpy((char*)pageData+prevRecordOffset,record,newSizeOfRecord);

			// change record size in the directory
			memcpy((char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((rid.slotNum+1)*(slotOffsetSize+slotNumberSize)), &newSizeOfRecord, sizeof(short));

		}else{
			// if page does not have enough space for updates record

			// insert new record
			RID newRid;
			if(insertFormattedRecord(fileHandle, recordDescriptor, data, newRid, REDIRECTED_RECORD_FLAG, rid) != SUCCESS){
				//cout<<"Failure inserting record during updateRecord process"<<endl;
				free(record);
				free(pageData);
				return -1;
			}

			// create tombstone record
			void* tombStoneRecord = calloc(sizeof(char)+sizeof(RID),1);
			RID dummyRID;
			short sizeOfTombStoneRecord = encodeRecord(tombStoneRecord, &newRid, recordDescriptor, TOMBSTONE_FLAG, dummyRID, debug);
			if(sizeOfTombStoneRecord != (sizeof(char)+sizeof(RID))){
				//cout<<"Size is: "<<sizeOfTombStoneRecord<<endl;
				//cout<<"Tombstone record creation error"<<endl;
				free(record);
				free(pageData);
				free(tombStoneRecord);
				return -1;
			}

			// update with tombstone record
			memcpy((char*)pageData+prevRecordOffset,tombStoneRecord,sizeOfTombStoneRecord);

			// call defragmentation
			if(defragment(rid, prevRecordSize-sizeOfTombStoneRecord,pageData) != SUCCESS){
				//cout<<"Defragmentation error during updateRecord process"<<endl;
				free(record);
				free(pageData);
				free(tombStoneRecord);
				return -1;
			}

			// change record size in the directory
			memcpy((char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((rid.slotNum+1)*(slotOffsetSize+slotNumberSize)), &sizeOfTombStoneRecord, sizeof(short));

			free(tombStoneRecord);
		}

	}else{
		// insert at the same location
		memcpy((char*)pageData+prevRecordOffset,record,newSizeOfRecord);

		// call defragmentation
//		RID nextRid = rid;
//		nextRid.slotNum+=1;
		if(defragment(rid, prevRecordSize-newSizeOfRecord, pageData) != SUCCESS){
			//cout<<"Defragmentation error during updateRecord process"<<endl;
			free(record);
			return -1;
		}

		// change record size in the directory
		memcpy((char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((rid.slotNum+1)*(slotOffsetSize+slotNumberSize)), &newSizeOfRecord, sizeof(short));
	}

	// write page
	fileHandle.writePage(rid.pageNum,pageData);
	free(pageData);
	free(record);
	return 0;
}

// readAttributesFromRecord reads data only from actual record
RC readAttributesFromRecord(const void* record, const vector<Attribute> & recordDescriptor, const vector<string> &attributeNames, void *data){

	char* input = (char*) record;

	//------------------------------------------------------

	char flag;
	memcpy(&flag, input, sizeof(char));
	input+=sizeof(char);

	if(flag != NEAT_RECORD_FLAG && flag != REDIRECTED_RECORD_FLAG){
		//cout<< " readAttributesFromRecord- Wrong flag as input"<<endl;
		if(flag == TOMBSTONE_FLAG){
			//cout<<" readAttributesFromRecord - Flag corresponds to tombstone record"<<endl;
		}
		return -1;
	}


	//------------------------------------------------------

	// Get index of required attributes in recordDescriptor to index_array
	int ind = -1;
	int i = 0;
	int j = 0;
	int index_array[attributeNames.size()];

	for (string attributeName: attributeNames){
		i=0;
		for(Attribute attribute:recordDescriptor){
			if(attribute.name == attributeName){
				ind = i;
				break;
			}
			i++;
		}

		if(ind < 0){
			//cout<<"readAttributeFromRecord: Attribute not found"<<endl;
			return -1;
		}

		index_array[j] = ind;
		j++;
	}

//	//cout << "size:" << recordDescriptor.s



	//------------------------------------------------------


	RID dummy;
	memcpy(&dummy, input, sizeof(RID));
	input+= sizeof(RID);


	unsigned short numFields = recordDescriptor.size();
	unsigned int numNullBites = ceil((float)numFields / 8.0);


	//------------------------------------------------------

	char* recordNullBites = (char*) calloc(numNullBites,1);     // free this memory
	memcpy(recordNullBites, input, numNullBites);
	input+=numNullBites;

	//------------------------------------------------------

	short offsetArray[numFields];
	memcpy(offsetArray, input, sizeof(offsetArray));
	input+=sizeof(offsetArray);

	//------------------------------------------------------


	int outputNumNullBites = ceil((float)(attributeNames.size()) / 8.0);

	char* outputNullBites = (char*) calloc(outputNumNullBites,1);
	char* outputBites = (char*) calloc(PAGE_SIZE,1);
	int sizeOfOutput = 0;

	input -= (sizeof(char) + sizeof(RID) + numNullBites +sizeof(offsetArray));

	for(int i = 0; i<attributeNames.size(); i++){
		ind = index_array[i];

		setBit(outputNullBites[(i/8)], i ,getBit(recordNullBites[(ind)/8],((ind)%8)));

		if(getBit(recordNullBites[(ind)/8],((ind)%8)) == false){

			int sizeOfAttribute;

			if(ind == 0){
				sizeOfAttribute = offsetArray[0]- (sizeof(char) + sizeof(RID) + numNullBites +sizeof(offsetArray));

				if(recordDescriptor[ind].type == AttrType::TypeVarChar){
					memcpy(outputBites, &sizeOfAttribute, sizeof(sizeOfAttribute));
					outputBites += sizeof(sizeOfAttribute);
					sizeOfOutput += sizeof(sizeOfAttribute);
				}

				memcpy(outputBites, input + (sizeof(char) + sizeof(RID) + numNullBites +sizeof(offsetArray)), sizeOfAttribute);
				outputBites += sizeOfAttribute;
				sizeOfOutput += sizeOfAttribute;

			}else{
				sizeOfAttribute = offsetArray[ind]-offsetArray[ind-1];

				if(recordDescriptor[ind].type == AttrType::TypeVarChar){
					memcpy(outputBites, &sizeOfAttribute, sizeof(sizeOfAttribute));
					outputBites += sizeof(sizeOfAttribute);
					sizeOfOutput += sizeof(sizeOfAttribute);
				}

				memcpy(outputBites, input+offsetArray[ind-1], sizeOfAttribute);
				outputBites += sizeOfAttribute;
				sizeOfOutput += sizeOfAttribute;
			}
		}
	}

	outputBites -= sizeOfOutput;

	// Form the return format

	memcpy(data, outputNullBites, outputNumNullBites);
	memcpy((char*)data+outputNumNullBites, outputBites, sizeOfOutput);

	free(recordNullBites);
	free(outputBites);
	free(outputNullBites);
	return 0;
}

RC RecordBasedFileManager::readAttributesFromRecordFormat(void* record, const vector<Attribute> & recordDescriptor, const vector<string> &attributeNames, void *data){
	return readAttributesFromRecord(record, recordDescriptor, attributeNames, data);
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){

	if(!fileHandle.isOpen()){
		return -1;
	}

	void* pageData = calloc(PAGE_SIZE,1);
	RID trueRID;

	if(findActualRID(fileHandle, rid, trueRID,pageData) != SUCCESS){
		//cout<<"Actual RID not found"<<endl;
		free(pageData);
		return -1;
	}

	unsigned short offset;
	memcpy(&offset,(char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((trueRID.slotNum+1)*(slotOffsetSize+slotNumberSize))+slotNumberSize,sizeof(short));

	vector<string> attributeNames;
	attributeNames.push_back(attributeName);

	if(readAttributesFromRecordFormat((char*)pageData+offset, recordDescriptor, attributeNames, data) != SUCCESS){
		//cout<<"readAttribute - readAttributesFromRecordFormat failed! "<<endl;
		free(pageData);
		return -1;
	}

	free(pageData);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator){

	if(!fileHandle.isOpen()){
		return -1;
	}

	rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);

	return 0;
}

RC RBFM_ScanIterator:: close() {
	if(this->isOpen){
		this->isOpen = false;

		free(this->currentPageData);

		return 0;
	}
	//cout<< "Iterator is not available to close!"<<endl;
	return -1;
}

void RBFM_ScanIterator:: initialize(FileHandle &fileHandle, const vector<Attribute> recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames) {

	// initialize rid to 0,0
	this->currentRID.pageNum = 0;
	this->currentRID.slotNum = -1;

	// store all parameters
	this->fileHandle = &fileHandle;
	this->recordDescriptor = recordDescriptor;
	this-> conditionAttribute = conditionAttribute;
	this->compOp = compOp;
	this-> value = (void *)value;
	this->attributeNames = attributeNames;

	this->fileHandle->readPage(this->currentRID.pageNum, this->currentPageData);

	this->isOpen = true;

}

RC RBFM_ScanIterator:: setNextRID() {
	this->currentRID.slotNum += 1;

	// Check if the slot exists - if not update the pageNum to next and read next page and slotNum to -1

	unsigned short numOfSlots = 0;
	memcpy(&numOfSlots, (char*)this->currentPageData + PAGE_SIZE - freeSpaceSize - numSlotsSpace, sizeof(numOfSlots));

	if(this->currentRID.slotNum >= numOfSlots){
		if((this->currentRID.pageNum+1) < fileHandle->getNumberOfPages()){
			this->currentRID.pageNum += 1;
			this->currentRID.slotNum = -1;
			if(this->fileHandle->readPage(this->currentRID.pageNum, this->currentPageData) != SUCCESS){
				//cout<<"setNextRID - readPage issue!"<<endl;
				return -1;
			}
		}else{
//			//cout<<"setNextRID - RBFM_EOF reached"<<endl;
			return RBFM_EOF;
		}
	}else{
		// If exists - check if it is deleted slot (unused slot)
		short offset;
		memcpy(&offset,(char*)this->currentPageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((this->currentRID.slotNum+1)*(slotOffsetSize+slotNumberSize))+slotNumberSize, sizeof(offset));

		if(offset != OFFSET_FOR_DELETED){
			// If exists - check if it is tombstone record
			char flag;
			memcpy(&flag,(char*)this->currentPageData+offset,sizeof(char));
//			//cout<<"flag is: "<<(int)flag<<endl;
			if(flag != TOMBSTONE_FLAG){
				// If exists - read actual RID
//				memcpy(&(this->currentRID), (char*)this->currentPageData+offset+sizeof(char),sizeof(RID));

				return 0;
			}
		}
	}

	// ELSE for all scenarios call itself
	return setNextRID();
}

RC RBFM_ScanIterator:: getNextRecord(RID &rid, void *data) {

	bool recordFound = false;
	if(this->isOpen == false){
		//cout<< "Iterator is not open!"<<endl;
		return -1;
	}


	// Get current RID and get the required attribute for comparison
	if(setNextRID()!=SUCCESS){

//		//cout<<"Next record: Page: "<<this->currentRID.pageNum<<" Slot: "<<this->currentRID.slotNum<<endl;
//		//cout<<"getNextRecord reached RBFM_EOF - 1"<<endl;
		return RBFM_EOF;
	}

	while(recordFound == false){
		void* pageData = this->currentPageData;

		unsigned short offset;
		memcpy(&offset,(char*)pageData + (PAGE_SIZE-freeSpaceSize-numSlotsSpace) - ((this->currentRID.slotNum+1)*(slotOffsetSize+slotNumberSize))+slotNumberSize,sizeof(short));

		void* recordData = calloc(PAGE_SIZE,1);

		vector<string> conditionAttributes;
		conditionAttributes.push_back(this-> conditionAttribute);

		// Should be done only for real records!!!!
		if(this->value != NULL && readAttributesFromRecord((char*)pageData+offset, this->recordDescriptor, conditionAttributes, recordData)!=SUCCESS){
			//cout<<"getNextRecord - readAttributesFromRecord failed"<<endl;
			free(recordData);
			return -1;
		}else{
			recordFound = readRecordDataAndCompareValue(recordData, pageData, offset, rid, data);
			// move rid to next
			free(recordData);
			if(recordFound == false){
				if(setNextRID()!=SUCCESS){
//					//cout<<"Next record: Page: "<<this->currentRID.pageNum<<" Slot: "<<this->currentRID.slotNum<<endl;
//					//cout<<"getNextRecord reached RBFM_EOF - 2"<<endl;
					return RBFM_EOF;
				}
			}else{
				// If record is finally found
				// data is already populated in the previous call
				// update the returned rid to the rid passed in the data

				memcpy(&rid, (char*)pageData+offset + sizeof(char),sizeof(RID));
			}
		}
	}

	return 0;
}

bool RBFM_ScanIterator::readRecordDataAndCompareValue(const void* record, const void* pageData, const unsigned short offset, RID &rid, void *data){
	char* recordData = (char*)record;

	// If comp operator is NO_OP - all are valid records
	if(this->compOp == NO_OP){
		if(readAttributesFromRecord((char*)pageData+offset, this->recordDescriptor, this->attributeNames, data)!=SUCCESS){
			//cout<<"getNextRecord - readAttributesFromRecord failed"<<endl;
		}
		return true;
	}

	int i=0;
	while(this->recordDescriptor[i].name != this->conditionAttribute){
		i++;
		if(i>= this->recordDescriptor.size()){
			//cout<<"conditionAttribute not found in recordDescriptor"<<endl;
			return false;
		}
	}

	if(this->recordDescriptor[i].type == TypeVarChar){
		char* recordAttributeValue = (char*) calloc(PAGE_SIZE,1);
		unsigned varcharAttributeSize;
		char nullByte;
		memcpy(&nullByte, recordData, sizeof(char));
		recordData+=sizeof(char);

		if(getBit(nullByte,0) == true){
			return false;
		}else{
			memcpy(&varcharAttributeSize, recordData, sizeof(unsigned));
			recordData+=sizeof(unsigned);

			memcpy(recordAttributeValue, recordData, varcharAttributeSize);
			recordData+=varcharAttributeSize;
		}


		int compSize;
		memcpy(&compSize, this->value, sizeof(int));
		char* compValue = (char*) calloc(compSize,1);
		memcpy(compValue, (char*)(this->value)+sizeof(int), compSize);

		string compVal(compValue, compSize);
		string recordAttVal(recordAttributeValue, varcharAttributeSize);


		if(compareStrings(recordAttVal, this->compOp, compVal)){
			rid = this->currentRID;
			if(readAttributesFromRecord((char*)pageData+offset, this->recordDescriptor, this->attributeNames, data)!=SUCCESS){
				//cout<<"getNextRecord - readAttributesFromRecord failed"<<endl;
			}

			free(recordAttributeValue);
			free(compValue);
			return true;
		}

		free(recordAttributeValue);
		free(compValue);

	}
	if(this->recordDescriptor[i].type == TypeInt){

		int recordAttributeValue;

		char nullByte;
		memcpy(&nullByte, recordData, sizeof(char));
		recordData+=sizeof(char);

		if(getBit(nullByte,0) == true){
			return false;
		}else{
			memcpy(&recordAttributeValue, recordData, 4);
			recordData+=4;
		}

		int compValue;
		memcpy(&compValue, this->value, 4);

//		//cout<<" Value is: "<<compValue<<endl;

		if(compare(recordAttributeValue,this->compOp, compValue)){
			rid = this->currentRID;
			if(readAttributesFromRecord((char*)pageData+offset, this->recordDescriptor, this->attributeNames, data)!=SUCCESS){
				//cout<<"getNextRecord - readAttributesFromRecord failed"<<endl;
			}
			return true;
		}

	}

	if(this->recordDescriptor[i].type == TypeReal){

		float recordAttributeValue;

		char nullByte;
		memcpy(&nullByte, recordData, sizeof(char));
		recordData+=sizeof(char);

		if(getBit(nullByte,0) == true){
			return false;
		}else{
			memcpy(&recordAttributeValue, recordData, 4);
			recordData+=4;
		}

		float compValue;
		memcpy(&compValue, this->value, 4);

		if(compare(recordAttributeValue,this->compOp, compValue)){
			rid = this->currentRID;
			if(readAttributesFromRecord((char*)pageData+offset, this->recordDescriptor, this->attributeNames, data)!=SUCCESS){
				//cout<<"getNextRecord - readAttributesFromRecord failed"<<endl;
			}
			return true;
		}
	}
	return false;
}


