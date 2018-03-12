#include <string.h>
#include <cstdlib>
#include <stdio.h>
using namespace std;

#define PAGE_SIZE 4096

struct PageHeader {
	unsigned signature;
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
//	byte byteArray[PAGE_SIZE -sizeof(int) -(3*(sizeof(unsigned)))];
} ;
