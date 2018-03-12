#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_8(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test8";

    // Create a file named "test8"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *record2 = malloc(100);
    void *record3 = malloc(1000);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 3, "Avi", 25, 177.8, 6200, record2, &recordSize);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 80, "AnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteater", 25, 177.8, 6200, record3, &recordSize);

    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    RID rid3;
    for(int i=0;i<10;i++){
		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid3);
		assert(rc == success && "Inserting a record should not fail.");
    }

    for(int i=0;i<199;i++){
    		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record2, rid3);
    	    assert(rc == success && "Inserting a record should not fail.");
    }

    for(int i=0;i<93;i++){
		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
		assert(rc == success && "Inserting a record should not fail.");
    }

    rid3.pageNum = 1;
    rid3.slotNum = 30;
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, record3, rid3);
    assert(rc == success && "Updating a record should not fail.");


    rid3.pageNum = 0;
	rid3.slotNum = 0;
	rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid3);
	assert(rc == success && "deleteRecord a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    cout << endl;

    RBFM_ScanIterator rbfm_ScanIterator;
    	string conditionAttribute = "EmpName";

    	vector<string> vect;
    	vect.push_back(conditionAttribute);

    	void* value = calloc(4096,1);
    	int length = 80;
    	memcpy(value, &length ,4);

    	string str= "AnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteaterAnteater";
    	memcpy((char*)value+4,str.c_str(),length );

    rc = rbfm->scan(fileHandle, recordDescriptor, conditionAttribute , EQ_OP, value, vect, rbfm_ScanIterator);

    RID ridNew;
    	void* data = calloc(4096,1);
    	bool finished = false;
    	int i = 0;
    	while(finished != true){
    		if(rbfm_ScanIterator.getNextRecord(ridNew,data) != 0){
    			finished = true;
    		}else{
    			cout<<"RID found: Page: "<<ridNew.pageNum<<" Slot: "<<ridNew.slotNum<<endl;
    			i++;
    		}
    	}
    	cout<<endl<<"Number of records found: "<<i<<endl;


    // Close the file "test8"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "RBF Test Case 8 Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test8");
       
    RC rcmain = RBFTest_8(rbfm);
    return rcmain;
}
