
#include <sstream>
#include <iomanip>
#include "rm.h"
#include "../ix/ix.h"
#include "math.h"
#include "../rbf/rbfm.h"


RelationManager RelationManager::rm;

RelationManager *RelationManager::instance() {
	return &rm;
}

RelationManager::RelationManager() {
	rbfm = RecordBasedFileManager::instance();
	if (rbfm->openFile(CATALOG_TABLE_FILE, tablesFileHandle) != SUCCESS ||
		rbfm->openFile(CATALOG_COLUMN_FILE, columnsFileHandle) != SUCCESS) {
		//cout << "Could not open catalog files" << endl;
	}

	im = IndexManager::instance();
}

RelationManager::~RelationManager() {

	if (rbfm->closeFile(tablesFileHandle) != SUCCESS || rbfm->closeFile(columnsFileHandle) != SUCCESS) {
		//cout << "Could not close catalog files" << endl;
	}
}

RC RelationManager::createCatalog() {

	FileHandle fileHandle;

	// Catalog files should not exist
	if (rbfm->openFile(CATALOG_TABLE_FILE, fileHandle) == SUCCESS ||
		rbfm->openFile(CATALOG_COLUMN_FILE, fileHandle) == SUCCESS) {
		return -1;
	}

	if (rbfm->createFile(CATALOG_TABLE_FILE) != SUCCESS || rbfm->createFile(CATALOG_COLUMN_FILE) != SUCCESS) {
		return -1;
	}

	if (rbfm->openFile(CATALOG_TABLE_FILE, tablesFileHandle) != SUCCESS ||
		rbfm->openFile(CATALOG_COLUMN_FILE, columnsFileHandle) != SUCCESS) {
		return -1;
	}

	if (createTable(CATALOG_TABLE_FILE, getAttributesOfMeta(true)) != SUCCESS ||
		createTable(CATALOG_COLUMN_FILE, getAttributesOfMeta(false)) != SUCCESS) {
		return -1;
	}

	return 0;
}

vector<string> RelationManager::getAttributeNamesOfMeta(bool forTable) {

	vector<string> attributeNames;
	if (forTable) {
		attributeNames.push_back("table-id");
		attributeNames.push_back("table-name");
		attributeNames.push_back("file-name");
		attributeNames.push_back("table-type");
	} else {
		attributeNames.push_back("table-id");
		attributeNames.push_back("column-name");
		attributeNames.push_back("column-type");
		attributeNames.push_back("column-length");
		attributeNames.push_back("column-position");
	}
	return attributeNames;
}

vector<Attribute> RelationManager::getAttributesOfMeta(bool forTable) {

	vector<Attribute> recordDescriptor;
	vector<string> attributeNames = getAttributeNamesOfMeta(forTable);

	if (forTable) {

		Attribute tableIdAttribute;
		tableIdAttribute.name = attributeNames.at(0);
		tableIdAttribute.length = 4;
		tableIdAttribute.type = TypeInt;
		recordDescriptor.push_back(tableIdAttribute);

		Attribute tableNameAttribute;
		tableNameAttribute.name = attributeNames.at(1);
		tableNameAttribute.length = 50;
		tableNameAttribute.type = TypeVarChar;
		recordDescriptor.push_back(tableNameAttribute);

		Attribute fileNameAttribute;
		fileNameAttribute.name = attributeNames.at(2);
		fileNameAttribute.length = 50;
		fileNameAttribute.type = TypeVarChar;
		recordDescriptor.push_back(fileNameAttribute);

		Attribute tableTypeAttribute;
		tableTypeAttribute.name = attributeNames.at(3);
		tableTypeAttribute.length = 50;
		tableTypeAttribute.type = TypeVarChar;
		recordDescriptor.push_back(tableTypeAttribute);

	} else {

		Attribute tableIdAttribute;
		tableIdAttribute.name = attributeNames.at(0);
		tableIdAttribute.length = 4;
		tableIdAttribute.type = TypeInt;
		recordDescriptor.push_back(tableIdAttribute);

		Attribute columnNameAttribute;
		columnNameAttribute.name = attributeNames.at(1);
		columnNameAttribute.length = 50;
		columnNameAttribute.type = TypeVarChar;
		recordDescriptor.push_back(columnNameAttribute);

		Attribute columnTypeAttribute;
		columnTypeAttribute.name = attributeNames.at(2);
		columnTypeAttribute.length = 4;
		columnTypeAttribute.type = TypeInt;
		recordDescriptor.push_back(columnTypeAttribute);

		Attribute columnLengthAttribute;
		columnLengthAttribute.name = attributeNames.at(3);
		columnLengthAttribute.length = 4;
		columnLengthAttribute.type = TypeInt;
		recordDescriptor.push_back(columnLengthAttribute);

		Attribute columnPositionAttribute;
		columnPositionAttribute.name = attributeNames.at(4);
		columnPositionAttribute.length = 4;
		columnPositionAttribute.type = TypeInt;
		recordDescriptor.push_back(columnPositionAttribute);
	}
	return recordDescriptor;
}

RC RelationManager::deleteCatalog() {

	if (rbfm->closeFile(tablesFileHandle) != SUCCESS ||
		rbfm->closeFile(columnsFileHandle) != SUCCESS ||
		rbfm->destroyFile(CATALOG_TABLE_FILE) != SUCCESS ||
		rbfm->destroyFile(CATALOG_COLUMN_FILE) != SUCCESS) {
		//cout << "Could not delete catalog files" << endl;
		return -1;
	}
	return 0;
}

int RelationManager::getNewTableId(const string &tableName) {

	if (tableName == CATALOG_TABLE_FILE) {
		return 1;
	}

	if (tableName == CATALOG_COLUMN_FILE) {
		return 2;
	}

	int newTableId = 3;
	RBFM_ScanIterator rbfm_scanIterator;
	vector<string> selectedAttributes;
	selectedAttributes.push_back((RelationManager::getAttributeNamesOfMeta(true).at(0)));
	stringstream tableNameValue;
	putNumberInStringStream(tableNameValue, tableName.length());
	tableNameValue << tableName;
	string s = tableNameValue.str();
	// Getting table-id
	if (rbfm->scan(tablesFileHandle, getAttributesOfMeta(true), getAttributeNamesOfMeta(true).at(1),
				   CompOp::NO_OP, s.c_str(),
				   selectedAttributes, rbfm_scanIterator) != SUCCESS) {
		//cout << "getNewTableId :Tables: Scan failed" << endl;
		rbfm_scanIterator.close();
		return -1;
	}
//	//cout << "getNewTableId: scan done" << endl;
	void *tableIdWithNullBytes = calloc(sizeOfInt + 1, 1);

	RID rid;
	while (rbfm_scanIterator.getNextRecord(rid, tableIdWithNullBytes) == SUCCESS) {
		char *detailsPtr = (char *) tableIdWithNullBytes;
		char nullBits;
		memcpy(&nullBits, detailsPtr, 1);
		detailsPtr++;
		if (nullBits != (char) 0) {
			//cout << "Got nullBits = " << nullBits;
			return -1;
		}

		memcpy(&newTableId, detailsPtr, sizeOfInt);
//		//cout << "getNewTableId: newTableId - " << newTableId << endl;
	}

	rbfm_scanIterator.close();
	free(tableIdWithNullBytes);
	return newTableId + 1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {

	if (tableName != CATALOG_TABLE_FILE && tableName != CATALOG_COLUMN_FILE && rbfm->createFile(tableName) != SUCCESS) {
		return -1;
	}

	RID tableRID;
	int tableId = getNewTableId(tableName);
	//cout << "createTable-new table id:" << tableId << endl;
	string recordInMetaTable = (tableName == CATALOG_TABLE_FILE || tableName == CATALOG_COLUMN_FILE)
							   ? getRecordForTables(tableId, tableName, false)
							   : getRecordForTables(tableId, tableName, true);
//	PrintBytesWithRange(recordInMetaTable.c_str(), 50, 0);
	if (rbfm->insertRecord(tablesFileHandle, getAttributesOfMeta(true), recordInMetaTable.c_str(), tableRID) !=
		SUCCESS) {
		return -1;
	}

	int columnPosition = 1;
	for (Attribute attribute: attrs) {
		string recordInMetaColumn = getRecordForColumns(tableId, attribute.name, attribute.type, attribute.length,
														columnPosition);
		if (rbfm->insertRecord(columnsFileHandle, getAttributesOfMeta(false), recordInMetaColumn.c_str(), tableRID) !=
			SUCCESS) {
			//cout << "Insert into columns failed" << endl;
			return -1;
		}
//		cout << endl << "createTable - columns rids: " << tableRID.pageNum << tableRID.slotNum << endl;
		columnPosition++;
	}
	return 0;
}

void RelationManager::PrintBytesWithRange(const char *pBytes, const uint32_t end, uint32_t start) {
	//cout << "-------------------" << endl;
	for (uint32_t i = start; i != end; i++) {
		std::cout <<
				  std::hex <<           // output in hex
				  std::setw(2) <<       // each byte prints as two characters
				  std::setfill('0') <<  // fill with 0 if not enough characters
				  static_cast<unsigned int>(pBytes[i]);
		if (i % 10 == 0) cout << std::endl;
		else cout << "  ";
	}
	//cout << std::dec;
	//cout << endl << "-------------------" << endl;
}

void RelationManager::putNumberInStringStream(stringstream &stringstream, int number) {

	char numberInBytes[4];
	memcpy(numberInBytes, &number, sizeOfInt);
	for (int i = 0; i < 4; i++) {
		stringstream << numberInBytes[i];
	}
}

string RelationManager::getRecordForTables(int tableId, string tableName, bool isUserTable) {

	stringstream record;
	char nullBits = 0;
	int tableNameLength = tableName.length();
	record << nullBits;
	putNumberInStringStream(record, tableId);
	putNumberInStringStream(record, tableNameLength);
	record << tableName;
	putNumberInStringStream(record, tableNameLength);
	record << tableName;

	if (isUserTable) {
		putNumberInStringStream(record, USER_TYPE_LENGTH);
		record << USER_TYPE;
	} else {
		putNumberInStringStream(record, SYSTEM_TYPE_LENGTH);
		record << SYSTEM_TYPE;
	}
//	PrintBytesWithRange(record.str().c_str(), record.str().length(), 0);
	return record.str();
}

string RelationManager::getRecordForColumns(int tableId, string columnName, int columnType, int columnLength,
											int columnPosition) {

	stringstream record;
	char nullBits = 0;
	record << nullBits;
	putNumberInStringStream(record, tableId);
	putNumberInStringStream(record, columnName.length());
	record << columnName;
	putNumberInStringStream(record, columnType);
//	record << columnType;
	putNumberInStringStream(record, columnLength);
	putNumberInStringStream(record, columnPosition);
//	PrintBytesWithRange(record.str().c_str(), record.str().length(), 0);
	return record.str();
}

RC RelationManager::deleteTable(const string &tableName) {

	if (tableName == CATALOG_TABLE_FILE || tableName == CATALOG_COLUMN_FILE) {
		//cout << "cant delete catalog files";
		return -1;
	}
	vector<Attribute> vec;
	if (rbfm->destroyFile(tableName) != SUCCESS) {
		//cout << "Deleting table failed" << endl;
	}
// 	//cout << "destroyed" << endl;
	RC rc =  getOrDeleteAttributes(tableName, vec, true);
	if(rc!=SUCCESS){
		cout<<"deleteTable::getOrDeleteAttributes failed!"<<endl;
		return -1;
	}

	// Delete Index files
	vector<string> indexNames;
	vector<string> indexAttributeNames = getIndexAttributeNames(tableName, indexNames);

	for(int i=0;i<indexNames.size();i++){
		rc = this->destroyIndex(tableName, indexAttributeNames[i]);
		if(rc!=SUCCESS){
			cout<<"deleteTable::destroyIndex failed!"<<endl;
			return -1;
		}
	}

	return 0;
}

RC RelationManager::getOrDeleteAttributes(const string &tableName, vector<Attribute> &attrs, bool toDelete) {

	RBFM_ScanIterator rbfm_scanIterator;
	vector<string> selectedAttributes;
	selectedAttributes.push_back((RelationManager::getAttributeNamesOfMeta(true).at(0)));
	stringstream tableNameValue;
	putNumberInStringStream(tableNameValue, tableName.length());
	tableNameValue << tableName;
	//cout.flush();
	string s = tableNameValue.str();
	// Getting table-id
	if (rbfm->scan(tablesFileHandle, getAttributesOfMeta(true), getAttributeNamesOfMeta(true).at(1),
				   CompOp::EQ_OP, s.c_str(),
				   selectedAttributes, rbfm_scanIterator) != SUCCESS) {
		//cout << "Tables: Scan failed" << endl;
		//cout.flush();
		rbfm_scanIterator.close();
		return -1;
	}

	void *tableIdWithNullBytes = calloc(sizeOfInt + 1, 1);

	RID rid;
	if (rbfm_scanIterator.getNextRecord(rid, tableIdWithNullBytes) == RM_EOF) {
		//cout << "getOrDeleteAttributes: getNextRecord failed" << endl;
		//cout.flush();
		rbfm_scanIterator.close();
		return -1;
	}

	rbfm_scanIterator.close();

	if (toDelete && rbfm->deleteRecord(tablesFileHandle, getAttributesOfMeta(true), rid) != SUCCESS) {
		//cout << "Del from 'Tables' failed" << endl;
		return -1;
	}

//	if (toDelete)
		//cout << "table rid deleted: " << rid.pageNum << rid.slotNum << endl;

	char *detailsPtr = (char *) tableIdWithNullBytes;
	char nullBits;
	memcpy(&nullBits, detailsPtr, 1);
	detailsPtr++;
	if (nullBits != (char) 0) {
		//cout << "Got nullBits = " << nullBits;
		return -1;
	}

	int tableId;
	memcpy(&tableId, detailsPtr, sizeOfInt);
	free(tableIdWithNullBytes);
	RBFM_ScanIterator rbfm_column_scanIterator;
	stringstream tableIdValue;
	putNumberInStringStream(tableIdValue, tableId);
	//cout << tableIdValue.str();
	//cout.flush();
	s = tableIdValue.str();
	// Getting columns from table-id
	if (rbfm->scan(columnsFileHandle, getAttributesOfMeta(false), getAttributeNamesOfMeta(false).at(0),
				   CompOp::EQ_OP, s.c_str(), getAttributeNamesOfMeta(false),
				   rbfm_column_scanIterator) !=
		SUCCESS) {
		//cout << "Columns: Scan failed" << endl;
		rbfm_column_scanIterator.close();
		return -1;
	}
	void *columnDetails = calloc(75, 1);
	vector<RID> ridsToDelete;
	while (rbfm_column_scanIterator.getNextRecord(rid, columnDetails) != RM_EOF) {

		if (toDelete) {
			ridsToDelete.push_back(rid);
		}
		Attribute attribute;
		detailsPtr = (char *) columnDetails;
		char nullBits;
		memcpy(&nullBits, detailsPtr, 1);
		detailsPtr++;

		int tableId;
		memcpy(&tableId, detailsPtr, sizeOfInt);
		detailsPtr += sizeOfInt;

		unsigned columnNameLength = 0;
		memcpy(&columnNameLength, detailsPtr, sizeOfInt);
		detailsPtr += sizeOfInt;

//		//cout << "HERE: " << columnNameLength << endl;

		char *attributeName = (char *) calloc(columnNameLength + 1, 1);
		memcpy(attributeName, detailsPtr, columnNameLength);
		detailsPtr += columnNameLength;
		attribute.name = string(attributeName);

		int columnType;
		memcpy(&columnType, detailsPtr, sizeOfInt);
		detailsPtr += sizeOfInt;

		switch (columnType) {
			case 0:
				attribute.type = AttrType::TypeInt;
				break;
			case 1:
				attribute.type = AttrType::TypeReal;
				break;
			case 2:
				attribute.type = AttrType::TypeVarChar;
				break;
			default:
				return -1;
		}


		int columnLength;
		memcpy(&columnLength, detailsPtr, sizeOfInt);
		detailsPtr += sizeOfInt;
		attribute.length = columnLength;

		int columnPosition;
		memcpy(&columnPosition, detailsPtr, sizeOfInt);

		attrs.push_back(attribute);

		free(attributeName);

	}
	free(columnDetails);
	rbfm_column_scanIterator.close();

	for (RID ridToDelete : ridsToDelete) {
		if (toDelete && rbfm->deleteRecord(columnsFileHandle, getAttributesOfMeta(false), ridToDelete) != SUCCESS) {
			//cout << "Del from 'Columns' failed" << endl;
			return -1;
		}
	}
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {

	RC rc = getOrDeleteAttributes(tableName, attrs, false);
	return rc;
}

void splitIndexName(const string &indexName, string &index_tableName, string &index_attributeName){
	string delimiter = "_";
	int pos = indexName.find(delimiter);

	index_tableName = indexName.substr(0, pos);
	index_attributeName = indexName.substr(pos+1, indexName.length()-1);
}

vector<string> RelationManager::getIndexAttributeNames(const string &tableName, vector<string> &indexNames){
	vector<string> indexAttributeNames;
	string indexName;
	string index_tableName;
	string index_attributeName;

	for(std::map<string,int>::iterator it=im->indexRootPageMap.begin(); it!=im->indexRootPageMap.end(); ++it){
		indexName = it->first;
		splitIndexName(indexName, index_tableName, index_attributeName);
		if(index_tableName == tableName){
			indexNames.push_back(indexName);
			indexAttributeNames.push_back(index_attributeName);
		}
	}
	return indexAttributeNames;
}

RC RelationManager::insertDeleteIndexEntry(const string &tableName, vector<Attribute> tableAttributes, const void *data,const  RID &rid, bool op){
	vector<string> indexNames;
		vector<string> indexAttributeNames = getIndexAttributeNames(tableName, indexNames);

		IXFileHandle ixFileHandle;
		Attribute index_attribute;
		string indexName;
		void* record = calloc(PAGE_SIZE,1);
		void* indexData = calloc(PAGE_SIZE,1);
		RID dummy;
		rbfm->encodeRecord(record, data, tableAttributes, 0 , dummy, false);
		vector<string>indexAttributeName;

		for(int i=0; i<indexNames.size();i++){
			indexName = indexNames[i];
			im->openFile(indexName, ixFileHandle);
			index_attribute = mapAttributeName(tableName, indexAttributeNames[i]);

			indexAttributeName.push_back(indexAttributeNames[i]);
			rbfm->readAttributesFromRecordFormat(record, tableAttributes, indexAttributeName, indexData);
			indexAttributeName.pop_back();

			RC rc;
			if(*(char*)indexData != 128){
				if(op){
					rc = im->insertEntry(ixFileHandle, index_attribute, (char *)indexData + 1, rid);
				}else{
					rc = im->deleteEntry(ixFileHandle, index_attribute, (char *)indexData + 1, rid);
				}
				if(rc!=SUCCESS){
					cout<<"RelationManager::createIndex:: insertEntry in index file not successful!"<<endl;
					return -1;
				}
			}
//			cout << "After inserting one-by-one" << endl;
//			cout << "index-name: " << index_attribute.name << " " << "index-type: " << index_attribute.type << " ";
//			im->printBtree(ixFileHandle, index_attribute);	// By Anirudh
			im->closeFile(ixFileHandle);
		}

		return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {

	if (tableName == CATALOG_TABLE_FILE || tableName == CATALOG_COLUMN_FILE) {
		//cout << "cant insert into catalog files";
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != SUCCESS) {
		return -1;
	}
	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		return -1;
	}

//	cout<<"data inserted in table: "<<*((float*)((char*)data+1)+1)<<endl;
	if (rbfm->insertRecord(fileHandle, tableAttributes, data, rid) != SUCCESS) {
		//cout << "Insert tuple failed" << endl;
		return -1;
	}

	rbfm->closeFile(fileHandle);

	// INSERT INTO INDEX FILES
	this->insertDeleteIndexEntry(tableName, tableAttributes, data, rid, true);

	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {

	if (tableName == CATALOG_TABLE_FILE || tableName == CATALOG_COLUMN_FILE) {
		//cout << "cant delete from catalog files";
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != SUCCESS) {
		return -1;
	}
	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		return -1;
	}

	void* data = calloc(PAGE_SIZE,1);
	rbfm->readRecord(fileHandle, tableAttributes, rid, data);
	if (rbfm->deleteRecord(fileHandle, tableAttributes, rid) != SUCCESS) {
		//cout << "deleteRecord tuple failed" << endl;
		return -1;
	}


	rbfm->closeFile(fileHandle);

	// DELETE FROM INDEX FILES
	this->insertDeleteIndexEntry(tableName, tableAttributes, data, rid, false);

	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {

	if (tableName == CATALOG_TABLE_FILE || tableName == CATALOG_COLUMN_FILE) {
		//cout << "cant update into catalog files";
		return -1;
	}

	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != SUCCESS) {
		return -1;
	}
	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		return -1;
	}

	void* oldData = calloc(PAGE_SIZE,1);
	rbfm->readRecord(fileHandle, tableAttributes, rid, oldData);
	if (rbfm->updateRecord(fileHandle, tableAttributes, data, rid) != SUCCESS) {
		//cout << "updateRecord tuple failed" << endl;
		return -1;
	}

	rbfm->closeFile(fileHandle);

	// Update in index file
	// delete
	this->insertDeleteIndexEntry(tableName, tableAttributes, oldData, rid, false);

	// insert
	this->insertDeleteIndexEntry(tableName, tableAttributes, data, rid, true);

	return 0;
}


RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {

	if (tableName == CATALOG_TABLE_FILE) {
		if (rbfm->readRecord(tablesFileHandle, getAttributesOfMeta(true), rid, data) != SUCCESS) {
//			//cout << "readRecord tuple failed" << endl;
			return -1;
		} else {
			return 0;
		}
	}
	if (tableName == CATALOG_COLUMN_FILE) {
		if (rbfm->readRecord(columnsFileHandle, getAttributesOfMeta(false), rid, data) != SUCCESS) {
//			//cout << "readRecord tuple failed" << endl;
			return -1;
		} else {
			return 0;
		}
	}

	FileHandle fileHandle;

	if (rbfm->openFile(tableName, fileHandle) != SUCCESS) {
		return -1;
	}
	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		return -1;
	}

	if (rbfm->readRecord(fileHandle, tableAttributes, rid, data) != SUCCESS) {
//		//cout << "readRecord tuple failed" << endl;
		return -1;
	}

	rbfm->closeFile(fileHandle);
	return 0;
}


RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
	return rbfm->printRecord(attrs, data);
}


RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {

	if (tableName == CATALOG_TABLE_FILE) {
		if (rbfm->readAttribute(tablesFileHandle, getAttributesOfMeta(true), rid, attributeName, data) != SUCCESS) {
			//cout << "readAttribute tuple failed" << endl;
			return -1;
		} else {
			return 0;
		}
	}
	if (tableName == CATALOG_COLUMN_FILE) {
		if (rbfm->readAttribute(columnsFileHandle, getAttributesOfMeta(false), rid, attributeName, data) != SUCCESS) {
			//cout << "readAttribute tuple failed" << endl;
			return -1;
		} else {
			return 0;
		}
	}
	FileHandle fileHandle;
	if (rbfm->openFile(tableName, fileHandle) != SUCCESS) {
		return -1;
	}
	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		return -1;
	}


	if (rbfm->readAttribute(fileHandle, tableAttributes, rid, attributeName, data) != SUCCESS) {
		//cout << "readAttribute tuple failed" << endl;
		return -1;
	}

	rbfm->closeFile(fileHandle);
	return 0;
}



Attribute RelationManager::mapAttributeName(const string &tableName, const string &attributeName){
	vector<Attribute> allAttributes;
	Attribute attribute;
	attribute.name = "nullName";

	RC rc = this->getAttributes(tableName,allAttributes);
	if(rc!=SUCCESS){
		cout<<"RelationManager::mapAttributeName: failed!"<<endl;
		return attribute;
	}

	for(Attribute attr:allAttributes){
		if(attr.name == attributeName) return attr;
	}

	return attribute;
}

// Index files

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	// Create index file name

	string underscore = "_";
	string indexName = "";

	indexName.append(tableName);
	indexName.append(underscore);
	indexName.append(attributeName);

	// Create index file

	RC rc = im->createFile(indexName);
	if(rc!=SUCCESS){
		cout<<"RelationManager::createIndex:: Creating index file not successful!"<<endl;
		return -1;
	}

	// Open index file

	IXFileHandle ixFileHandle;
	rc = im->openFile(indexName,ixFileHandle);
	if(rc!=SUCCESS){
		cout<<"RelationManager::createIndex:: Opening index file not successful!"<<endl;
		return -1;
	}

	// If the table already consist records - create corresponding index
	int i = 0;
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);

	RM_ScanIterator rm_ScanIterator;
	rc = this->scan(tableName, "", NO_OP, NULL, attributeNames, rm_ScanIterator);
	if(rc!=SUCCESS){
		cout<<"RelationManager::createIndex:: Scan on table data file not successful!"<<endl;
		return -1;
	}

	RID rid;
	void *returnedData = calloc(PAGE_SIZE,1);

	const Attribute index_attribute = mapAttributeName(tableName, attributeName);
	if(index_attribute.name == "nullName"){
		cout<<"RelationManager::createIndex:: mapAttributeName is not successful!"<<endl;
		return -1;
	}

	while(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){

//		cout<<"lazy indexing: "<< index_attribute.name<< " is "<<*(float*)((char*)returnedData+1) << endl;
		rc = im->insertEntry(ixFileHandle, index_attribute, (char *)returnedData + 1, rid);
//		cout << " rid after insertion: "<< rid.slotNum<<endl;	// By Anirudh
		if(rc!=SUCCESS){
			cout<<"RelationManager::createIndex:: insertEntry in index file not successful!"<<endl;
			return -1;
		}
//		im->printBtree(ixFileHandle, index_attribute);	// By Anirudh
	}

	rc = rm_ScanIterator.close();
	if(rc!=SUCCESS){
		cout << "here" << endl;
		return -1;
	}

	// Print b-tree

//	im->printBtree(ixFileHandle, index_attribute);
//	cout<<"-----------------------------------------";

	// Close index file

	rc = im->closeFile(ixFileHandle);
	if(rc!=SUCCESS){
		cout<<"RelationManager::createIndex:: Closing index file not successful!"<<endl;
		return -1;
	}

	return 0;
}


RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string underscore = "_";
	string indexName = "";

	indexName.append(tableName);
	indexName.append(underscore);
	indexName.append(attributeName);

	for(std::map<string,int>::iterator it=im->indexRootPageMap.begin(); it!=im->indexRootPageMap.end(); ++it){
		if(indexName == it->first){
			im->indexRootPageMap.erase (it);
			RC rc = im->destroyFile(indexName);
			if(rc!=SUCCESS){
				cout<<"RelationManager::destroyIndex: index file destroyFile was unsuccessful!"<<endl;
				return -1;
			}
			return 0;
		}
	}

	return -1;
}


// Index Files Scan

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	string underscore = "_";
	string indexName = "";

	indexName.append(tableName);
	indexName.append(underscore);
	indexName.append(attributeName);

	RC rc = im->openFile(indexName, *(rm_IndexScanIterator.ixfileHandle));

	if(rc!= SUCCESS){
		return -1;
	}
	Attribute attribute = this->mapAttributeName(tableName, attributeName);
	if(attribute.name == "nullName"){
		return -1;
	}
//	im->printBtree(*(rm_IndexScanIterator.ixfileHandle), attribute);
	IX_ScanIterator ix_ScanIterator;
	rm_IndexScanIterator.setIx_scanIterator(ix_ScanIterator);

	rc = im->scan(*(rm_IndexScanIterator).ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
	if(rc!= SUCCESS){
		return -1;
	}

	return 0;
}

RM_IndexScanIterator::RM_IndexScanIterator() {
	this->ixfileHandle = new IXFileHandle();
}

RM_IndexScanIterator::~RM_IndexScanIterator() {}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	return this->ix_ScanIterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close() {
	RC rc = this->ixfileHandle->fileHandle.close();
	if(rc!=SUCCESS){
		return -1;
	}
	rc = ix_ScanIterator.close();
	return rc;
}

void RM_IndexScanIterator::setIx_scanIterator(IX_ScanIterator &ix_ScanIterator) {
	this->ix_ScanIterator = ix_ScanIterator;
}

// Record Files Scan

RC RelationManager::scan(const string &tableName,
						 const string &conditionAttribute,
						 const CompOp compOp,
						 const void *value,
						 const vector<string> &attributeNames,
						 RM_ScanIterator &rm_ScanIterator) {

	if (tableName == CATALOG_TABLE_FILE) {
		if (rbfm->scan(tablesFileHandle, getAttributesOfMeta(true), conditionAttribute, compOp, value, attributeNames,
					   rm_ScanIterator.rbfm_scanIterator) != SUCCESS) {
			//cout << "RM scan - RBFM scan failed!" << endl;
			return -1;
		} else {
			return 0;
		}
	}

	if (tableName == CATALOG_COLUMN_FILE) {
		if (rbfm->scan(columnsFileHandle, getAttributesOfMeta(false), conditionAttribute, compOp, value, attributeNames,
					   rm_ScanIterator.rbfm_scanIterator) != SUCCESS) {
			//cout << "RM scan - RBFM scan failed!" << endl;
			return -1;
		} else {
			return 0;
		}
	}

	if (rbfm->openFile(tableName, *(rm_ScanIterator.currentFileHandle)) != SUCCESS) {
		cout<<"file open failure"<<endl;
		return -1;
	}

	vector<Attribute> tableAttributes;
	if (getAttributes(tableName, tableAttributes) != SUCCESS) {
		cout<<"RelationManager::scan :getAttributes failed!"<<endl;
		return -1;
	}

	RBFM_ScanIterator rbfm_scanIterator;
	rm_ScanIterator.setRbfm_scanIterator(rbfm_scanIterator);

	if (rbfm->scan(*(rm_ScanIterator.currentFileHandle), tableAttributes, conditionAttribute, compOp, value,
				   attributeNames, rm_ScanIterator.rbfm_scanIterator) != SUCCESS) {
		cout << "RM scan - RBFM scan failed!" << endl;
		return -1;
	}
	return 0;
}


RM_ScanIterator::RM_ScanIterator() {
	currentFileHandle = new FileHandle();
}


RM_ScanIterator::~RM_ScanIterator() {
//	rbfm_scanIterator.close();
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	return rbfm_scanIterator.getNextRecord(rid, data);
}


RC RM_ScanIterator::close() {

	currentFileHandle->close();
	delete (currentFileHandle);
	return (rbfm_scanIterator.close());
}


void RM_ScanIterator::setRbfm_scanIterator(RBFM_ScanIterator &rbfm_scanIterator) {
	this->rbfm_scanIterator = rbfm_scanIterator;
}

