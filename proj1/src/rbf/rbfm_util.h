#include <iostream>
#include <iomanip>
#include <sys/stat.h>
#include <stdio.h>

using namespace std;

#define sizeOfShort 2
#define sizeOfSlot 4
#define OFFSET_FOR_DELETED -1
#define TOMBSTONE_FLAG 127
#define REDIRECTED_RECORD_FLAG 1
#define NEAT_RECORD_FLAG 0

const int SUCCESS = 0;

bool getBit(char byte, int position) // position in range 0-7
{
	position = 7 - position;
	return (byte >> position) & 0x1;
}

void setBit(char &outputBite,int ind ,bool zeroOne){

	ind = 7- ind;

	if(zeroOne == true){
		outputBite |= 1 << ind;
	}else{
		outputBite &= ~(1 << ind);
	}
}

void PrintBytes(const char *pBytes, const uint32_t end, uint32_t start) // should more properly be std::size_t
{
	cout << "-------------------" << endl;
	for (uint32_t i = start; i != end; i++) {
		std::cout <<
				  std::hex <<           // output in hex
				  std::setw(2) <<       // each byte prints as two characters
				  std::setfill('0') <<  // fill with 0 if not enough characters
				  static_cast<unsigned int>(pBytes[i]);
		if (i % 10 == 0) cout << std::endl;
		else cout << "  ";
	}
	cout << std::dec;
	cout << endl << "-------------------" << endl;
}

//void PrintBytesWithRange(const char *pBytes, const uint32_t end, uint32_t start) {
//    for (uint32_t i = start; i != end; i++) {
//        std::cout <<
//                  std::hex <<           // output in hex
//                  std::setw(2) <<       // each byte prints as two characters
//                  std::setfill('0') <<  // fill with 0 if not enough characters
//                  static_cast<unsigned int>(pBytes[i]);
//        if (i % 10 == 0) cout << std::endl;
//        else cout << "  ";
//    }
//    cout << std::dec;
//    cout << endl;
//}

int defragment(const RID updatedRid, short slideToLeft, const void *inputPage) {

	char *page = (char *) inputPage;
	short freeSpace;
	memcpy(&freeSpace, page + PAGE_SIZE - sizeOfShort, sizeOfShort);
	freeSpace += slideToLeft;
	// Updating free space
	memcpy(page + PAGE_SIZE - sizeOfShort, &freeSpace, sizeOfShort);

	unsigned int updatedSlotNum = updatedRid.slotNum;
	short updatedSlotInfo[2];
	memcpy(updatedSlotInfo, page + PAGE_SIZE - 2 * sizeOfShort - (updatedSlotNum + 1) * sizeOfSlot, sizeOfSlot);

	short numSlots;
	memcpy(&numSlots, page + PAGE_SIZE - 2 * sizeOfShort, sizeOfShort);

	short fragmentLeftEnd = updatedSlotInfo[0] + updatedSlotInfo[1];
	short fragmentRightEnd = 0;
	// Calculating the new offset entries
	short slotsInDirectory[2 * numSlots];
	memcpy(slotsInDirectory, page + PAGE_SIZE - 2 * sizeOfShort - (numSlots) * sizeOfSlot,
		   sizeOfSlot * numSlots);
	for (int i = 0; i < numSlots; i++) {
		if (slotsInDirectory[2 * i] + slotsInDirectory[2 * i + 1] > fragmentRightEnd) {
			fragmentRightEnd = slotsInDirectory[2 * i] + slotsInDirectory[2 * i + 1];
		}
		if (slotsInDirectory[2 * i + 1] >= fragmentLeftEnd) {
			slotsInDirectory[2 * i + 1] -= slideToLeft;
		}
	}
	// Storing the offset entries
	memcpy(page + PAGE_SIZE - 2 * sizeOfShort - (numSlots) * sizeOfSlot, slotsInDirectory,
		   sizeOfSlot * numSlots);

	// Moving the fragment
	memmove(page + fragmentLeftEnd - slideToLeft, page + fragmentLeftEnd, fragmentRightEnd - fragmentLeftEnd);
	if(slideToLeft > 0) {
		void *emptySpace = calloc(slideToLeft, 1);
		memcpy(page + fragmentRightEnd - slideToLeft, emptySpace, slideToLeft);
		free(emptySpace);
	}
	return SUCCESS;
}
