#include "pfm.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <cstdlib>

#include "fm_util.h"
using namespace std;


unsigned SIGN;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
	rbfmAccess = false;
	sharedKey = SHARED_KEY;
}


PagedFileManager::~PagedFileManager()
{
}


//////// REMOVE THIS FOR SECURITY ISSUES
RC PagedFileManager::getAccess(bool access, unsigned sharedKey){
//	return 0; // Comment this line for secure functionality
	if(sharedKey == this->sharedKey){
		rbfmAccess = access;
		if(rbfmAccess == true){
			SIGN = SIGNATURE_RBFM;
		}else{
			SIGN = SIGNATURE;
		}
		return 0;
	}
	return -1;
}


RC PagedFileManager::createFile(const string &fileName)
{

	if(fileName == "" || FileAlreadyExists(fileName)){
		return -1;
	}
	fstream newFileStream;
	newFileStream.open(fileName, ios::out );

	// write header page
	PageHeader pageHeader;
	pageHeader.signature = SIGN;
	pageHeader.readPageCounter = 0;
	pageHeader.writePageCounter = 0;
	pageHeader.appendPageCounter = 0;

	void* buffer = calloc(PAGE_SIZE,1);
	memcpy(buffer,&pageHeader,sizeof(pageHeader));

	void* freeSpace = calloc(PAGE_SIZE,1);
	memcpy((char*)buffer+sizeof(pageHeader),freeSpace,PAGE_SIZE-sizeof(pageHeader));

	newFileStream.seekg(0, ios_base::beg);
	newFileStream.write((char*)buffer, PAGE_SIZE);
	newFileStream.flush();


	if(newFileStream.is_open()){
		newFileStream.close();

		free(buffer);
		free(freeSpace);
		return 0;
	}

	free(buffer);
	free(freeSpace);
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(remove(fileName.c_str()) == 0){
		return 0;
	}
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(fileName == ""){
		return -1;
	}
	if(FileAlreadyExists(fileName) && fileHandle.open(fileName)){
		return 0;
	}
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if(fileHandle.close()){
		return 0;
	}
    return -1;
}


FileHandle::FileHandle()
{

	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;

}


FileHandle::~FileHandle()
{

	if(fileStream.is_open()){
		fileStream.flush();
		fileStream.close();
	}
	//------------------------------------------------------------------------------------------------------------------------------------
	//																DESTROY STUFF
	//------------------------------------------------------------------------------------------------------------------------------------
}

bool FileHandle::isOpen(){
	return fileStream.is_open();
}

bool FileHandle::open(const string &fileName)
{
	fstream tempFileStream;
	tempFileStream.open(fileName, ios::in);
	tempFileStream.seekg(0, ios_base::beg);

	PageHeader * test = (PageHeader*) calloc(sizeof(PageHeader),1);
	tempFileStream.read((char*)test, sizeof(PageHeader));

	if(test->signature != SIGN){
		tempFileStream.close();
		free(test);
		return false;
	}


	if(isDifferent(fileName)){
		fileStream.open(fileName, ios::in | ios::out);
		if(fileStream.is_open()){
			this->fileName = fileName;
			if(readHeaderPage() == success){
				free(test);
				return true;
			}
		}
	}

	free(test);
    return false;
}

bool FileHandle::close()
{
	writeHeaderPage();
	fileStream.flush();
	fileStream.close();
	if(!fileStream.is_open()){
		return true;
	}
    return false;
}

bool FileHandle::isDifferent(const string &fileName)
{
	//if((fileStream.is_open()) && this->fileName.compare(fileName)){
	if(fileStream.is_open()){
		return false;
	}
    return true;
}

bool FileHandle::incrementCounterValues(bool readIncrement, bool writeIncrement, bool appendIncrement){

	if(readIncrement){
		pageHeader.readPageCounter++;
		readPageCounter = pageHeader.readPageCounter;
	}
	if(writeIncrement){
		pageHeader.writePageCounter++;
		writePageCounter = pageHeader.writePageCounter;
	}
	if(appendIncrement){
		pageHeader.appendPageCounter++;
		appendPageCounter = pageHeader.appendPageCounter;
	}
//	writeHeaderPage();
	return true;
}

RC FileHandle::writeHeaderPage()
{
	void* buffer = calloc(PAGE_SIZE,1);
	memcpy(buffer,&pageHeader,sizeof(pageHeader));

	void* freeSpace = calloc(PAGE_SIZE,1);
	memcpy((char*)buffer+sizeof(pageHeader),freeSpace,PAGE_SIZE-sizeof(pageHeader));

	fileStream.seekg(0, ios_base::beg);
	fileStream.write((char*)buffer, PAGE_SIZE);
	fileStream.flush();

	free(buffer);
	free(freeSpace);
    return 0;
}

RC FileHandle::readHeaderPage()
{

	fileStream.seekg(0, ios_base::beg);

	char * buffer = (char*) calloc(PAGE_SIZE,1);
	fileStream.read(buffer, PAGE_SIZE);


	memcpy(&pageHeader,buffer,sizeof(PageHeader));
	readPageCounter = pageHeader.readPageCounter;
	writePageCounter = pageHeader.writePageCounter;
	appendPageCounter = pageHeader.appendPageCounter;

	if(pageHeader.signature != SIGN){
//		cout<<"file opened with wrong sign"<<pageHeader.signature<<endl;

		free(buffer);
		return -1;
	}

	free(buffer);
	return 0;

}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(fileStream.is_open()){
		pageNum++;
		if (pageNum < getNumberOfPages()+1){
			fileStream.seekg((pageNum * PAGE_SIZE), ios_base::beg);

			char * buffer = new char [PAGE_SIZE];
			fileStream.read(buffer, PAGE_SIZE);

			incrementCounterValues(true, false, false);
			memcpy(data,buffer,PAGE_SIZE);        // exits -1 if not successful

			free(buffer);
			return 0;
		}
	}
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(fileStream.is_open()){
		pageNum++;
		if(pageNum < getNumberOfPages()+1){

			fileStream.seekg((pageNum * PAGE_SIZE), ios_base::beg);

			char * buffer = (char *)data;
			fileStream.write(buffer, PAGE_SIZE);
			fileStream.flush();

			incrementCounterValues(false, true, false);
			return 0;
		}
	}
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
	if(fileStream.is_open()){
		fileStream.seekg(((getNumberOfPages()+1) * PAGE_SIZE), ios_base::beg);

		char * buffer = (char *)data;
		fileStream.write(buffer, PAGE_SIZE);
		fileStream.flush();

		incrementCounterValues(false, false, true);
		return 0;
	}
	return -1;
}


unsigned FileHandle::getNumberOfPages()
{
	return pageHeader.appendPageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = pageHeader.readPageCounter;
	writePageCount = pageHeader.writePageCounter;
	appendPageCount = pageHeader.appendPageCounter;
	return 0;
}


