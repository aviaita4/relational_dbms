#include <iostream>
#include <iomanip>
#include <sys/stat.h>
#include <stdio.h>

using namespace std;

const int success = 0;

// Check whether a file exists
bool FileAlreadyExists(const string &fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

void Bytes(
    const char* pBytes,
    const uint32_t nBytes) // should more properly be std::size_t
{
	uint32_t i;
    for ( i = 0; i != nBytes; i++)
    {
        std::cout <<
            std::hex <<           // output in hex
            std::setw(2) <<       // each byte prints as two characters
            std::setfill('0') <<  // fill with 0 if not enough characters
            static_cast<unsigned int>(pBytes[i]);// << std::endl;
    }
}
