
#include "qe.h"

#include "map"

// Helper Functions ---------------------------------------------------------------------------------------------------------------------------------

bool gBit(char byte, int position) // position in range 0-7
{
	position = 7 - position;
	return (byte >> position) & 0x1;
}

void sBit(char &outputBite, int ind, bool zeroOne) {

	ind = 7 - ind;

	if (zeroOne == true) {
		outputBite |= 1 << ind;
	} else {
		outputBite &= ~(1 << ind);
	}
}

short
encode(void *record, const void *data, const vector<Attribute> &recordDescriptor, char flagByte, RID recordActualRID,
	   bool debugMode) {

	char *inputData = (char *) data;

	short numFields = recordDescriptor.size();
	int numNullBites = ceil((float) numFields / 8.0);



	//------------------------------------------------------

	char *recordNullBites = (char *) calloc(numNullBites, 1);
	memcpy(recordNullBites, data, numNullBites);

	//------------------------------------------------------


	short offsetArray[numFields];

	char *recordData = (char *) calloc(PAGE_SIZE, 1);
	int fieldsTotalLength = 0;
	int lastNonZeroOffset = sizeof(char) + sizeof(RID) + numNullBites + sizeof(offsetArray);

	inputData += numNullBites;

	for (int i = 0; i < numFields; i++) {

		////////// DEBUG MODE////////
		if (debugMode) {
			cout << recordDescriptor[i].name << ": ";
		}
		////////// DEBUG MODE////////

		if (gBit(recordNullBites[(i) / 8], ((i) % 8)) == true) {
			offsetArray[i] = lastNonZeroOffset;
			////////// DEBUG MODE////////
			if (debugMode) {
				cout << "NULL ";
			}
			////////// DEBUG MODE////////
		} else {
			if (recordDescriptor[i].type != AttrType::TypeVarChar) {

				offsetArray[i] = lastNonZeroOffset + recordDescriptor[i].length;

				memcpy(recordData, inputData, recordDescriptor[i].length);

				////////// DEBUG MODE////////
				if (debugMode) {
					if (recordDescriptor[i].type == AttrType::TypeInt) {
						int tempValue = 0;
						memcpy(&tempValue, inputData, recordDescriptor[i].length);

						cout << tempValue << " ";
					} else {
						float tempValue = 0.0;
						memcpy(&tempValue, inputData, recordDescriptor[i].length);

						cout << tempValue << " ";
					}
				}
				////////// DEBUG MODE////////

				inputData += recordDescriptor[i].length;
				recordData += recordDescriptor[i].length;
				fieldsTotalLength += recordDescriptor[i].length;

			} else {

				unsigned varcharActualSize = 0;
				memcpy(&varcharActualSize, inputData, 4);
				inputData += 4;

				memcpy(recordData, inputData, varcharActualSize);

				////////// DEBUG MODE////////
				if (debugMode) {
					char *tempValue = (char *) calloc(varcharActualSize, 1);
					memcpy(tempValue, inputData, varcharActualSize);

					string varcharValue;
					varcharValue.append(tempValue);

					cout << varcharValue << " ";
					free(tempValue);
				}
				////////// DEBUG MODE////////


				offsetArray[i] = lastNonZeroOffset + varcharActualSize;
				inputData += varcharActualSize;
				recordData += varcharActualSize;
				fieldsTotalLength += varcharActualSize;

			}
			lastNonZeroOffset = offsetArray[i];
		}

	}

	//------------------------------------------------------
	// append offsetArea NULL Bits area Data Area

	char *recordCopy = (char *) calloc(PAGE_SIZE, 1);

	memcpy(recordCopy, &flagByte, sizeof(char));
	recordCopy += sizeof(char);

	memcpy(recordCopy, &recordActualRID, sizeof(recordActualRID));
	recordCopy += sizeof(recordActualRID);

	memcpy(recordCopy, recordNullBites, numNullBites);
	recordCopy += numNullBites;

	memcpy(recordCopy, offsetArray, sizeof(offsetArray));
	recordCopy += sizeof(offsetArray);

	recordData -= fieldsTotalLength;
	memcpy(recordCopy, recordData, fieldsTotalLength);
	recordCopy += fieldsTotalLength;


	short sizeOfRecord = (sizeof(char) + sizeof(recordActualRID) + sizeof(offsetArray) + numNullBites +
						  fieldsTotalLength);
	recordCopy -= sizeOfRecord;


	memcpy(record, recordCopy, sizeOfRecord);
	free(recordCopy);
	free(recordNullBites);
	free(recordData);
	return sizeOfRecord;
}

RC
readAttributeValues(const void *record, const vector<Attribute> &recordDescriptor, const vector<string> &attributeNames,
					void *data) {

	char *input = (char *) record;

	//------------------------------------------------------

	char flag;
	memcpy(&flag, input, sizeof(char));
	input += sizeof(char);

	//------------------------------------------------------

	// Get index of required attributes in recordDescriptor to index_array
	int ind = -1;
	int i = 0;
	int j = 0;
	int index_array[attributeNames.size()];

	for (string attributeName: attributeNames) {
		i = 0;
		for (Attribute attribute:recordDescriptor) {
			if (attribute.name == attributeName) {
				ind = i;
				break;
			}
			i++;
		}

		if (ind < 0) {
			//cout<<"readAttributeFromRecord: Attribute not found"<<endl;
			return -1;
		}

		index_array[j] = ind;
		j++;
	}

	//------------------------------------------------------

	RID dummy;
	memcpy(&dummy, input, sizeof(RID));
	input += sizeof(RID);


	unsigned short numFields = recordDescriptor.size();
	unsigned int numNullBites = ceil((float) numFields / 8.0);


	//------------------------------------------------------

	char *recordNullBites = (char *) calloc(numNullBites, 1);     // free this memory
	memcpy(recordNullBites, input, numNullBites);
	input += numNullBites;

	//------------------------------------------------------

	short offsetArray[numFields];
	memcpy(offsetArray, input, sizeof(offsetArray));
	input += sizeof(offsetArray);

	//------------------------------------------------------


	int outputNumNullBites = ceil((float) (attributeNames.size()) / 8.0);

	char *outputNullBites = (char *) calloc(outputNumNullBites, 1);
	char *outputBites = (char *) calloc(PAGE_SIZE, 1);
	int sizeOfOutput = 0;

	input -= (sizeof(char) + sizeof(RID) + numNullBites + sizeof(offsetArray));

	for (int i = 0; i < attributeNames.size(); i++) {
		ind = index_array[i];

		sBit(outputNullBites[(i / 8)], i, gBit(recordNullBites[(ind) / 8], ((ind) % 8)));

		if (gBit(recordNullBites[(ind) / 8], ((ind) % 8)) == false) {

			int sizeOfAttribute;

			if (ind == 0) {
				sizeOfAttribute = offsetArray[0] - (sizeof(char) + sizeof(RID) + numNullBites + sizeof(offsetArray));

				if (recordDescriptor[ind].type == AttrType::TypeVarChar) {
					memcpy(outputBites, &sizeOfAttribute, sizeof(sizeOfAttribute));
					outputBites += sizeof(sizeOfAttribute);
					sizeOfOutput += sizeof(sizeOfAttribute);
				}

				memcpy(outputBites, input + (sizeof(char) + sizeof(RID) + numNullBites + sizeof(offsetArray)),
					   sizeOfAttribute);
				outputBites += sizeOfAttribute;
				sizeOfOutput += sizeOfAttribute;

			} else {
				sizeOfAttribute = offsetArray[ind] - offsetArray[ind - 1];

				if (recordDescriptor[ind].type == AttrType::TypeVarChar) {
					memcpy(outputBites, &sizeOfAttribute, sizeof(sizeOfAttribute));
					outputBites += sizeof(sizeOfAttribute);
					sizeOfOutput += sizeof(sizeOfAttribute);
				}

				memcpy(outputBites, input + offsetArray[ind - 1], sizeOfAttribute);
				outputBites += sizeOfAttribute;
				sizeOfOutput += sizeOfAttribute;
			}
		}
	}

	outputBites -= sizeOfOutput;

	// Form the return format

	memcpy(data, outputNullBites, outputNumNullBites);
	memcpy((char *) data + outputNumNullBites, outputBites, sizeOfOutput);


	free(recordNullBites);
	free(outputBites);
	free(outputNullBites);
	return 0;
}

int compareValues(const void *currPageEntry, const AttrType attrType, const char *inputKey) {

	int compareResult = -1;
	char *currPageData = (char *) currPageEntry;
	switch (attrType) {
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

bool checkCondition(const void *record, const vector<Attribute> &attrs, const Condition condition) {

	Attribute lhsConditionAttribute;
	Attribute rhsConditionAttribute;

	if (condition.bRhsIsAttr == false) {

		void *recordAttrVal = calloc(PAGE_SIZE, 1);
		vector<string> conditionAttributeNames;
		conditionAttributeNames.push_back(condition.lhsAttr);
		RC rc = readAttributeValues(record, attrs, conditionAttributeNames, recordAttrVal);
		if (rc != SUCCESS) {
			cout << "Some issue readAttributeValues" << endl;
		}

		if (condition.op == NO_OP) return true;

		if (*(char *) recordAttrVal == 128) {
			free(recordAttrVal);
			return false;
		}

		int res = compareValues((char *) recordAttrVal + 1, condition.rhsValue.type, (char *) condition.rhsValue.data);
		free(recordAttrVal);
		switch (condition.op) {
			case EQ_OP :
				return (res == 0);
			case LT_OP :
				return (res < 0);
			case LE_OP :
				return (res <= 0);
			case GT_OP :
				return (res > 0);
			case GE_OP :
				return (res >= 0);
			case NE_OP :
				return (res != 0);
			case NO_OP :
				return true;
			default:
				return false;
		}
		return false;

	} else {
		void *leftRecordAttrVal = calloc(PAGE_SIZE, 1);
		void *rightRecordAttrVal = calloc(PAGE_SIZE, 1);
		vector<string> conditionAttributeNames;
		conditionAttributeNames.push_back(condition.lhsAttr);
		RC rc = readAttributeValues(record, attrs, conditionAttributeNames, leftRecordAttrVal);
		if (rc != SUCCESS) {
			cout << "Some issue readAttributeValues" << endl;
		}

		vector<string> rightConditionAttributeNames;
		rightConditionAttributeNames.push_back(condition.rhsAttr);
		rc = readAttributeValues(record, attrs, rightConditionAttributeNames, rightRecordAttrVal);
		if (rc != SUCCESS) {
			cout << "Some issue readAttributeValues" << endl;
		}

		if (condition.op == NO_OP) return true;

		if (*(char *) leftRecordAttrVal == 128 || *(char *) rightRecordAttrVal == 128) {
			free(leftRecordAttrVal);
			free(rightRecordAttrVal);
			return false;
		}

		int res = compareValues((char *) leftRecordAttrVal + 1, condition.rhsValue.type, (char *) rightRecordAttrVal + 1);
		free(leftRecordAttrVal);
		free(rightRecordAttrVal);
		switch (condition.op) {
			case EQ_OP :
				return (res == 0);
			case LT_OP :
				return (res < 0);
			case LE_OP :
				return (res <= 0);
			case GT_OP :
				return (res > 0);
			case GE_OP :
				return (res >= 0);
			case NE_OP :
				return (res != 0);
			case NO_OP :
				return true;
			default:
				return false;
		}
		return false;
	}

	return false;
}

unsigned getTupleSize(const void *data, const vector<Attribute> &attrs) {

	unsigned tupleSize = 0;
	char *record = (char *) data;

	short numFields = attrs.size();
	int numNullBites = ceil((float) numFields / 8.0);

	//------------------------------------------------------

	char *recordNullBites = (char *) calloc(numNullBites, 1);
	memcpy(recordNullBites, record, numNullBites);
	record += numNullBites;
	tupleSize += numNullBites;

	//------------------------------------------------------

	for (int i = 0; i < attrs.size(); i++) {
		if (gBit(recordNullBites[(i) / 8], ((i) % 8)) == false) {
			if (attrs[i].type == AttrType::TypeVarChar) {
				int varCharSize;
				memcpy(&varCharSize, record, 4);
				record += 4 + varCharSize;
				tupleSize += 4 + varCharSize;
			} else {
				record += 4;
				tupleSize += 4;
			}
		}
	}

	free(recordNullBites);
	return tupleSize;
}

// returns the size of the merged tuple
unsigned mergeTuples(const void *leftData, const vector<Attribute> &leftAttrs, const void *rightData,
					 const vector<Attribute> &rightAttrs, void *joinData, const vector<Attribute> &joinAttrs) {

	unsigned leftNumNullBytes = ceil((float) leftAttrs.size() / 8.0);
	unsigned rightNumNullBytes = ceil((float) rightAttrs.size() / 8.0);
	unsigned joinNumNullBytes = ceil((float) joinAttrs.size() / 8.0);
	unsigned leftTupleSize = getTupleSize(leftData, leftAttrs);
	unsigned rightTupleSize = getTupleSize(leftData, leftAttrs);

	char *leftRecordNullBites = (char *) calloc(leftNumNullBytes, 1);
	memcpy(leftRecordNullBites, leftData, leftNumNullBytes);
	char *rightRecordNullBites = (char *) calloc(rightNumNullBytes, 1);
	memcpy(rightRecordNullBites, rightData, rightNumNullBytes);
	char *joinRecordNullBites = (char *) calloc(joinNumNullBytes, 1);

	// Set Null bytes
	for (int i = 0; i < leftAttrs.size(); i++) {
		sBit(joinRecordNullBites[(i / 8)], i, gBit(leftRecordNullBites[(i) / 8], ((i) % 8)));
	}
	for (int i = leftAttrs.size(); i < leftAttrs.size() + rightAttrs.size(); i++) {
		sBit(joinRecordNullBites[(i / 8)], i,
			 gBit(leftRecordNullBites[(i - leftNumNullBytes) / 8], ((i - leftNumNullBytes) % 8)));
	}

	//set data
	unsigned leftRecordDataBytes = leftTupleSize - leftNumNullBytes;
	unsigned rightRecordDataBytes = rightTupleSize - rightNumNullBytes;
	unsigned joinRecordDataBytes = leftRecordDataBytes + rightRecordDataBytes;

	char *joinRecordData = (char *) calloc(joinRecordDataBytes, 1);

	memcpy(joinRecordData, (char *) leftData + leftNumNullBytes, leftRecordDataBytes);
	memcpy((char *) joinRecordData + leftRecordDataBytes, (char *) rightData + rightNumNullBytes, rightRecordDataBytes);

	memcpy(joinData, joinRecordNullBites, joinNumNullBytes);
	memcpy((char *) joinData + joinNumNullBytes, joinRecordData, joinRecordDataBytes);

	free(leftRecordNullBites);
	free(rightRecordNullBites);
	free(joinRecordNullBites);
	free(joinRecordData);
	return (joinNumNullBytes + joinRecordDataBytes);
}

Attribute mapAttribute(vector<Attribute> allAttributes, const string &attributeName) {

	Attribute attribute;
	attribute.name = "nullName";

	for (Attribute attribute:allAttributes) {
		if (attribute.name == attributeName) return attribute;
	}

	return attribute;
}

// Filter Interface ---------------------------------------------------------------------------------------------------------------------------------

Filter::Filter(Iterator *input, const Condition &condition) {
	this->iter = input;
	input->getAttributes(this->attrs);
	this->condition = condition;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

RC Filter::getNextTuple(void *data) {

	RC rc = this->iter->getNextTuple(data);
	if (rc != SUCCESS) {
		return QE_EOF;
	}

	void *record = calloc(PAGE_SIZE, 1);
	RID dummy;
	encode(record, data, this->attrs, 0, dummy, false);

	if (checkCondition(record, this->attrs, this->condition) == true) {
		free(record);
		return 0;
	}

	do {
		rc = this->iter->getNextTuple(data);
		encode(record, data, this->attrs, 0, dummy, false);
		if (rc != SUCCESS) {
			free(record);
			return QE_EOF;
		}
	} while (checkCondition(record, this->attrs, this->condition) != true);

	free(record);
	return 0;
}

// Projection Interface -------------------------------------------------------------------------------------------------------------------------------

Project::Project(Iterator *input, const vector<string> &attrNames) {
	this->iter = input;
	this->attrNames = attrNames;

	this->attrs.clear();
	this->projAttrs.clear();
	input->getAttributes(this->attrs);

	for (int i = 0; i < attrNames.size(); i++) {
		for (int j = 0; j < this->attrs.size(); j++) {
			if (this->attrs[j].name == attrNames[i]) {
				this->projAttrs.push_back(this->attrs[j]);
			}
		}
	}
};

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->projAttrs;
}

RC Project::getNextTuple(void *data) {

	RC rc = this->iter->getNextTuple(data);
	if (rc != SUCCESS) {
		return QE_EOF;
	}

	void *record = calloc(PAGE_SIZE, 1);
	RID dummy;
	encode(record, data, this->attrs, 0, dummy, false);
	readAttributeValues(record, this->attrs, this->attrNames, data);

	free(record);
	return 0;
}

// BNLJoin Interface ---------------------------------------------------------------------------------------------------------------------------------

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	this->leftIter = leftIn;
	this->rightIter = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	this->leftAttrs.clear();
	this->rightAttrs.clear();
	this->joinAttrs.clear();

	leftIn->getAttributes(this->leftAttrs);
	rightIn->getAttributes(this->rightAttrs);

	for (int i = 0; i < this->leftAttrs.size(); i++) {
		this->joinAttrs.push_back(this->leftAttrs[i]);
	}
	for (int i = 0; i < this->rightAttrs.size(); i++) {
		this->joinAttrs.push_back(this->rightAttrs[i]);
	}

	this->leftPageSize = 0;
	this->outputPageSize = 0;

	this->outputPage = (char *) calloc(PAGE_SIZE, 1);

	this->intMap.clear();
	this->floatMap.clear();
	this->stringMap.clear();

	this->joinAttr = mapAttribute(this->leftAttrs, condition.lhsAttr);

	this->leftPageSpace = calloc((this->numPages - 1) * PAGE_SIZE, 1);

	this->outputIndQueue = queue<int>();
	this->outputSizeQueue = queue<int>();
	this->outputIndQueue.push(0);

	createLeftTable();
	this->done = false;
}

BNLJoin::~BNLJoin() {
	free(this->outputPage);
	free(this->leftPageSpace);
//	cout << "destructor" << endl;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->joinAttrs;
}

RC BNLJoin::createLeftTable() {

	this->intMap.clear();
	this->floatMap.clear();
	this->stringMap.clear();
	this->leftPageSize = 0;

	unsigned leftTupleSize = 0;
	void *tempRecord = calloc(PAGE_SIZE, 1);
	void *recordAttrVal = calloc(PAGE_SIZE, 1);

	while (leftPageSize <= (this->numPages - 1) * PAGE_SIZE) {
		RC rc = this->leftIter->getNextTuple((char *) leftPageSpace + leftPageSize);
		if (rc != SUCCESS) {
			free(tempRecord);
			free(recordAttrVal);
			return -1;
		}
		leftTupleSize = getTupleSize((char *) leftPageSpace + leftPageSize, this->leftAttrs);

		// fetch key
		RID dummy;
		encode(tempRecord, (char *) leftPageSpace + leftPageSize, this->leftAttrs, 0, dummy, false);
		vector<string> conditionAttributeNames;
		conditionAttributeNames.push_back(this->condition.lhsAttr);
		readAttributeValues(tempRecord, this->leftAttrs, conditionAttributeNames, recordAttrVal);

		// create map key,leftpage.ind
		switch (this->joinAttr.type) {
			case AttrType::TypeInt:
//				cout<<"hi - "<< *((int*)((char*)recordAttrVal+1))<<" "<<leftPageSize <<endl;
				this->intMap.insert(std::pair<int, int>(*((int *) ((char *) recordAttrVal + 1)), leftPageSize));
				break;
			case AttrType::TypeReal:
				this->floatMap.insert(std::pair<float, int>(*((float *) ((char *) recordAttrVal + 1)), leftPageSize));
				break;
			case AttrType::TypeVarChar:
				int keySize;
				memcpy(&keySize, (char *) recordAttrVal + 1, 4);
				char *charKey = (char *) calloc(keySize, 1);
				memcpy(charKey, (char *) recordAttrVal + 1 + 4, keySize);
				string key = string(charKey, keySize);
				free(charKey);
				this->stringMap.insert(std::pair<string, int>(key, leftPageSize));
				break;
		}

		leftPageSize += leftTupleSize;
	}

//	cout<< this->intMap.size()<<endl;
//	cout<< this->floatMap.size()<<endl;
//	cout<< this->stringMap.size()<<endl;

	free(tempRecord);
	free(recordAttrVal);
	return 0;
}

RC BNLJoin::createOutput() {
	void *rightTuple = calloc(PAGE_SIZE, 1);
	RC rc = this->rightIter->getNextTuple(rightTuple);
	if (rc != SUCCESS) {
		this->rightIter->setIterator();
		this->rightIter->getNextTuple(rightTuple);

//		cout<<"left created again"<<endl;
		RC rc2 = createLeftTable();
		if (rc2 != SUCCESS) {
			this->done = true;
		}
	}

	void *tempRecord = calloc(PAGE_SIZE, 1);
	void *recordAttrVal = calloc(PAGE_SIZE, 1);

	// fetch key
	RID dummy;
	encode(tempRecord, rightTuple, this->rightAttrs, 0, dummy, false);
	vector<string> conditionAttributeNames;
	conditionAttributeNames.push_back(this->condition.rhsAttr);
	readAttributeValues(tempRecord, this->rightAttrs, conditionAttributeNames, recordAttrVal);

	switch (this->joinAttr.type) {
		case AttrType::TypeInt: {
			int key = *((int *) ((char *) recordAttrVal + 1));
			multimap<int, int>::iterator it = this->intMap.lower_bound(key);
			if (it->first != key) {
				free(rightTuple);
				free(tempRecord);
				free(recordAttrVal);
				return -1;
			}
			while (it->first == key) {
				void* tempData = calloc(PAGE_SIZE,1);
				int size = mergeTuples((char *) leftPageSpace + it->second, this->leftAttrs, rightTuple,
									   this->rightAttrs, tempData,
									   this->joinAttrs);

				if(this->outputIndQueue.front() + size > PAGE_SIZE){
					outputIndQueue.push(0);
				}

				memcpy(this->outputPage + this->outputIndQueue.front(), tempData, size);
				outputIndQueue.push(this->outputIndQueue.front() + size);
				free(tempData);
				outputSizeQueue.push(size);
				it++;
			}
			break;
		}

		case AttrType::TypeReal: {
			float keyF = *((float *) ((char *) recordAttrVal + 1));
			multimap<float, int>::iterator itF = this->floatMap.lower_bound(keyF);
			if (itF->first != keyF) {
				free(rightTuple);
				free(tempRecord);
				free(recordAttrVal);
				return -1;
			}
			while (itF->first == keyF) {
				void* tempData = calloc(PAGE_SIZE,1);
				int size = mergeTuples((char *) leftPageSpace + itF->second, this->leftAttrs, rightTuple,
									   this->rightAttrs, tempData,
									   this->joinAttrs);

				if(this->outputIndQueue.front() + size > PAGE_SIZE){
					outputIndQueue.push(0);
				}

				memcpy(this->outputPage + this->outputIndQueue.front(), tempData, size);
				outputIndQueue.push(this->outputIndQueue.front() + size);
				free(tempData);
				outputSizeQueue.push(size);
				itF++;
			}
			break;
		}

		case AttrType::TypeVarChar: {
			int keySize;
			memcpy(&keySize, (char *) recordAttrVal + 1, 4);
			char *charKey = (char *) calloc(keySize, 1);
			memcpy(charKey, (char *) recordAttrVal + 1 + 4, keySize);
			string keyS = string(charKey, keySize);
			multimap<string, int>::iterator itS = this->stringMap.lower_bound(keyS);
			if (itS->first != keyS) {
				free(rightTuple);
				free(tempRecord);
				free(recordAttrVal);
				return -1;
			}
			while (itS->first == keyS) {
				void* tempData = calloc(PAGE_SIZE,1);
				int size = mergeTuples((char *) leftPageSpace + itS->second, this->leftAttrs, rightTuple,
									   this->rightAttrs, tempData,
									   this->joinAttrs);

				if(this->outputIndQueue.front() + size > PAGE_SIZE){
					outputIndQueue.push(0);
				}

				memcpy(this->outputPage + this->outputIndQueue.front(), tempData, size);
				outputIndQueue.push(this->outputIndQueue.front() + size);
				free(tempData);
				outputSizeQueue.push(size);
				itS++;
			}
			break;
		}
	}

	free(rightTuple);
	free(tempRecord);
	free(recordAttrVal);
	return rc;
}

RC BNLJoin::initiate() {
	if (this->done == true) {
		return -1;
	}

	while(true){
		RC rc = createOutput();
		if(rc!=SUCCESS){
			return -1;
		}else{
			break;
		}
		if (this->done == true) {
			return -1;
		}
	}

	return 0;
}

RC BNLJoin::getNextTuple(void *data) {
	if (outputSizeQueue.empty() == true) {

		RC rc = initiate();
		if (rc != SUCCESS) {
//			cout<<"QE_EOF reached!"<<endl;
			return QE_EOF;
		}
	}



	if (outputSizeQueue.empty() == true) {
		cout << "some issue: QE_EOF reached!" << endl;
		return QE_EOF;
	}
	int ind = outputIndQueue.front();
	int size = outputSizeQueue.front();
	outputIndQueue.pop();
	outputSizeQueue.pop();
	memcpy(data, this->outputPage + ind, size);

	return 0;
}

int INLJoin::autoIncId = 0;

// INLJoin Interface ---------------------------------------------------------------------------------------------------------------------------------
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

	outputFileName = condition.lhsAttr + "_" + condition.rhsAttr + ("_") + to_string(autoIncId);
	rbfm = RecordBasedFileManager::instance();
	rbfm->createFile(outputFileName);
	rbfm->openFile(outputFileName, outputFileHandle);

	INLJoin::autoIncId++;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	vector<string> joinAttrNames;

	for (const auto &leftAttr : leftAttrs) {
		joinAttrs.push_back(leftAttr);
		joinAttrNames.push_back(leftAttr.name);
	}
	for (const auto &rightAttr : rightAttrs) {
		joinAttrs.push_back(rightAttr);
		joinAttrNames.push_back(rightAttr.name);
//		cout << rightAttr.name << ", " << endl;
	}

	vector<string> leftConditionAttrVector;
	leftConditionAttrVector.push_back(condition.lhsAttr);
	vector<string> rightConditionAttrVector;
	rightConditionAttrVector.push_back(condition.rhsAttr);

	void *leftRecord = calloc(PAGE_SIZE, 1);
	void *rightRecord = calloc(PAGE_SIZE, 1);
	while (leftIn->getNextTuple(leftRecord) == SUCCESS) {

		RID dummy;
		void *encodedLeftRecord = calloc(PAGE_SIZE, 1);
		encode(encodedLeftRecord, leftRecord, leftAttrs, 0, dummy, false);

		void *leftConditionAttrValue = calloc(PAGE_SIZE, 1);
		readAttributeValues(encodedLeftRecord, leftAttrs, leftConditionAttrVector, leftConditionAttrValue);
//		cout << endl << "left = " << *(float *)((char *)leftConditionAttrValue + 1);
		rightIn->setIterator((char *)leftConditionAttrValue + 1, (char *)leftConditionAttrValue + 1, true, true);
		while (rightIn->getNextTuple(rightRecord) == SUCCESS) {
//			cout << " right = " << *(float *)((char *)rightRecord + 5) << endl;
			void *mergedRecord = calloc(PAGE_SIZE, 1);
			mergeTuples(leftRecord, leftAttrs, rightRecord, rightAttrs, mergedRecord, joinAttrs);
			RID insertedRID;
			rbfm->insertRecord(outputFileHandle, joinAttrs, mergedRecord, insertedRID);
		}
		free(encodedLeftRecord);
	}
	free(leftRecord);
	free(rightRecord);
	rbfm->scan(outputFileHandle, joinAttrs, "", NO_OP, nullptr, joinAttrNames, rbfm_scanIterator);

}

RC INLJoin::getNextTuple(void *data) {

	RID dummy;
	return rbfm_scanIterator.getNextRecord(dummy, data);
}

INLJoin::~INLJoin() {
	rbfm_scanIterator.close();
	rbfm->closeFile(outputFileHandle);
	rbfm->destroyFile(outputFileName);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->joinAttrs;
}


Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {

	Attribute attribute;
	attribute.type = TypeReal;
	attribute.length = 4;
	switch (op) {

		case MAX:
			attribute.name = "MAX";
			break;
		case MIN:
			attribute.name = "MIN";
			break;
		case COUNT:
			attribute.name = "COUNT";
			break;
		case SUM:
			attribute.name = "SUM";
			break;
		case AVG:
			attribute.name = "AVG";
			break;
	}
	attribute.name = attribute.name + "(" + aggAttr.name + ")";
	attrs.clear();
	attrs.push_back(attribute);

	sum = 0;
	count = 0;
	max = INT_MIN;
	min = INT_MAX;
	this->op = op;
	this->aggAttr = aggAttr;

	vector<Attribute> attrs;
	input->getAttributes(attrs);
	vector<string> aggAttrNameVector;
	aggAttrNameVector.push_back(aggAttr.name);
	void *record = calloc(PAGE_SIZE, 1);
	while (input->getNextTuple(record) == SUCCESS) {

		void *valueWithNullBits = calloc(10, 1);
		RID dummy;
		void *encodedLeftRecord = calloc(PAGE_SIZE, 1);
		encode(encodedLeftRecord, record, attrs, 0, dummy, false);
		readAttributeValues(encodedLeftRecord, attrs, aggAttrNameVector, valueWithNullBits);
		char c;
		memcpy(&c, valueWithNullBits, 1);
		// NULL value
		if (c != 0) {
			continue;
		}
//		cout << aggAttr.type << " ";
		switch (aggAttr.type) {
			case TypeInt:
				int valueInt;
				memcpy(&valueInt, (char *) valueWithNullBits + 1, sizeOfInt);
//				cout << valueInt << " ";
//				cout.flush();
				sum += valueInt;
				if (valueInt < min) {
					min = valueInt;
				}
				if (valueInt > max) {
					max = valueInt;
				}
				break;
			case TypeReal:
				float valueFloat;
				memcpy(&valueFloat, (char *) valueWithNullBits + 1, sizeOfInt);
				sum += valueFloat;
				if (valueFloat < min) {
					min = valueFloat;
				}
				if (valueFloat > max) {
					max = valueFloat;
				}
				break;
			default:
				free(record);
				break;
		}
		count++;
	}
	free(record);
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {

	attrs.clear();
	attrs = this->attrs;

}

RC Aggregate::getNextTuple(void *data) {

	char c;    //Null bit
	if (count == -1) {
		return QE_EOF;
	}
	count == 0 ? c = 128 : c = 0;
	memcpy(data, &c, sizeOfChar);
	if (count != 0) {

		switch (op) {
			case MAX:
				memcpy((char *) data + 1, &max, sizeOfFloat);
				break;
			case MIN:
				memcpy((char *) data + 1, &min, sizeOfFloat);
				break;
			case COUNT:
				memcpy((char *) data + 1, &count, sizeOfFloat);
				break;
			case SUM:
				memcpy((char *) data + 1, &sum, sizeOfFloat);
				break;
			case AVG:
				float avg = sum / count;
				memcpy((char *) data + 1, &avg, sizeOfFloat);
				break;
		}
	}
	count = -1;
	return 0;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op) {

	Attribute attribute;
	attribute.type = TypeReal;
	attribute.length = 4;
	switch (op) {

		case MAX:
			attribute.name = "MAX";
			break;
		case MIN:
			attribute.name = "MIN";
			break;
		case COUNT:
			attribute.name = "COUNT";
			break;
		case SUM:
			attribute.name = "SUM";
			break;
		case AVG:
			attribute.name = "AVG";
			break;
	}
	attribute.name = attribute.name + "(" + aggAttr.name + ")";
	attrs.push_back(groupAttr);
	attrs.push_back(attribute);

//	map<>

	sum = 0;
	count = 0;
	max = INT_MIN;
	min = INT_MAX;
	this->op = op;
	this->aggAttr = aggAttr;

	vector<Attribute> attrs;
	input->getAttributes(attrs);
	vector<string> aggAttrNameVector;
	aggAttrNameVector.push_back(aggAttr.name);
	void *record = calloc(PAGE_SIZE, 1);
	while (input->getNextTuple(record) == SUCCESS) {

		void *valueWithNullBits = calloc(10, 1);
		RID dummy;
		void *encodedLeftRecord = calloc(PAGE_SIZE, 1);
		encode(encodedLeftRecord, record, attrs, 0, dummy, false);
		readAttributeValues(encodedLeftRecord, attrs, aggAttrNameVector, valueWithNullBits);
		char c;
		memcpy(&c, valueWithNullBits, 1);
		// NULL value
		if (c != 0) {
			continue;
		}
		switch (aggAttr.type) {
			case TypeInt:
				int valueInt;
				memcpy(&valueInt, (char *) valueWithNullBits + 1, sizeOfInt);
				sum += valueInt;
				if (valueInt < min) {
					min = valueInt;
				}
				if (valueInt > max) {
					max = valueInt;
				}
				break;
			case TypeReal:
				float valueFloat;
				memcpy(&valueFloat, (char *) valueWithNullBits + 1, sizeOfInt);
				sum += valueFloat;
				if (valueFloat < min) {
					min = valueFloat;
				}
				if (valueFloat > max) {
					max = valueFloat;
				}
				break;
			default:
				free(record);
				break;
		}
		count++;
	}
	free(record);
}
GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions) {

	bnlJoin = new BNLJoin(leftIn, (TableScan *)rightIn, condition, 100);
}

RC GHJoin::getNextTuple(void *data) {
	return bnlJoin->getNextTuple(data);
}

// For attribute in vector<Attribute>, name it as rel.attr
void GHJoin::getAttributes(vector<Attribute> &attrs) const {

	return bnlJoin->getAttributes(attrs);
}

GHJoin::~GHJoin() {
	delete bnlJoin;
}

