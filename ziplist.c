#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"
#include "redisassert.h"

/**
 *ziplist end marker, and 5 bytes length marker
 */
#define ZIP_END 255
#define ZIP_BIGLEN 254

/**
 * Different encoding/length possibilites
 */
#define ZIP_STR_MASK 0xc0
#define ZIP_INT_MASK 0x30

/**
 * Type of the string encoding
 */
#define ZIP_STR_06B (0<<6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32N (2 << 6)

/**
 * Integer type encoding
 */
#define ZIP_INT_16B  (0xc0|0<<4)
#define ZIP_INT_32B  (0xc0|1<<4)
#define ZIP_INT_64B  (0xc0|2<<4)
#define ZIP_INT_24B  (0xc0|3<<4)
#define ZIP_INT_8B   0xfe

/**
 * 4 bit integer immediate encoding
 */
#define ZIP_INT_IMM_MASK 0x0f
#define ZIP_INT_IMM_MIN 0xf1  /* 1111 0001*/
#define ZIP_INT_IMM_MAX 0xfd  /* 1111 1101*/
#define ZIP_INT_IMM_VAL(v) (v & ZIP_INT_IMM_MASK)

/**
 * 24 bit integer max and min value
 */
#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/** 
 * Marco to determine type
 */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)


/**
 * Utility Marco
 */
//Get the bytes of the ziplist 
//We can write this code all because the first element of the struct is
//the size of the struct, so when we change the type of the pointer, we 
//change the bytes that we manipulate.
#define ZIPLIST_BYTES(zl)  (*((uint32_t*)(zl)))
//Move the pointer to the tail variable position,
//and get the tail offset.
#define ZIPLIST_TAIL_OFFSET(zl) (*(uint32_t*(zl+sizeof(uint32_t))))
//move the pointer to the length variable, and retrive the length
#define ZIPLIST_LENGTH(zl) (*(uint32_t*((zl) + sizeof(uint32_t) * 2)))
//return the header size of the ziplist
#define ZIPLIST_HEADER_SIZE  (sizeof(uint32_t)*2 + sizeof(uint16_t))
//return the first entry that the ziplist contains
#define ZIPLIST_ENTRY_HEAD(zl) ((zl)+ZIPLIST_HEADER_SIZE)
//return the pointer that points to the last entry of the ziplist
#define ZIPLIST_ENTRY_TAIL(zl) ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))
//return the pointer that points to the end of the ziplist
#define ZIPLIST_ENTRY_END(zl) ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/**
 * We know a positive increment can only be 1 because entries can only be
 * pushed one at a time
 *
 * T = O(1)
 */
#define ZIPLIST_INCR_LENGTH(zl, incr) { \
	if(ZIPLIST_LENGTH(zl) < UINT16_MAX) \
		ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr);\
}

/**
 * The structure to hold the variable
 * There is a point we need to consider, the underlying
 * data in memory is saved as this format.
 *
 * |--------------------------+-----------+--------------|
 * |  previous_entry_length   +  encoding +   content    |
 * |--------------------------+-----------+--------------|
 *
 */
typedef struct zlentry{
	//prevrawlen: the length of the previous entry
	unsigned int prevrawlensize, prevrawlen;

	//len: current entry size
	//lensize: how many space to hold the len
	unsigned int lensize, len;

	//THe size of header of current node
	//prevrawlensize + lensize
	unsigned int headersize;

	//current node value encoding
	unsigned char encoding;

	//the pointer to the current node
	unsigned char *p;
}zlentry;

/**
 * Extract the encoding from the byte pointed by 'ptr' and set it into 'encoding'
 * T = O(1)
 */
#define ZIP_ENTRY_ENCODING(ptr, encoding) do{\
	(encoding) = (ptr[0]);\
	if((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK;\
	}while(0)

/**
 * Return bytes needed to store integer encoded by 'encoding'
 * T = O(1)
 */
static unsigned int zipIntSize(unsigned char encoding){
	switch(encoding){
		case ZIP_INT_8B: return 1;
		case ZIP_INT_16B: return 2;
		case ZIP_INT_24B: return 3;
		case ZIP_INT_32B: return 4;
		case ZIP_INT_64B: return 8;
		default: return 0; /* 4 bits immediate */
	}
	assert(NULL);
	return 0;
}

/**
 * Encode the length 'l' writing it in 'p'.If p is NULL it just returns 
 * the amount of bytes required to encode such a length.
 * T = O(1)
 */
static unsigned int zipEncodeLength(unsigned char *p, unsigned char encoding, unsigned int rawlen){
	unsigned char len = 1, buf[5];

	//encode the string
	if(ZIP_IS_STR(encoding)){
		/**
		 * Although encoding is given it may not be set for strings,
		 * so we determine it here using the raw length.
		 */
		if(rawlen <= 0x3f){
			if(!p) return len;
			buf[0] = ZIP_STR_06B | rawlen;
		}else if(rawlen <= 0x3fff){
			len += 1;
			if(!p) return len;
			buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
			buf[1] = rawlen & 0xff;
		}else{
			len += 4;
			if(!p) return len;
			buf[0] = ZIP_STR_32B;
			buf[1] = (rawlen >> 24) & 0xff;
			buf[2] = (rawlen >> 16) & 0xff;
			buf[3] = (rawlen >> 8) & 0xff;
			buf[4] = rawlen & 0xff;
		}
	}else{
		if(!p) return len;
		buf[0] = encoding;
	}

	//store this length at p
	memcpy(p, buf, len);
	
	//return encoding needed storage size
	return len;
}

/**
 * Decode the length encoded in 'ptr'.The encoding variable will hold the entries 
 * encoding, the 'lensize' variable will hold the number of bytes required to encode
 * the entries length, and the 'len' variable will hold the entries length.
 * T = O(1) 
 */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len)  do {         \
	/*Get the entry encoding*/			             \
	ZIP_ENTRY_ENCODING((ptr), (encoding));                       \
	                                                             \
	/*String encoding*/                                          \
	if(encoding < ZIP_STR_MASK){                                 \
		if((encoding) == ZIP_STR_06B){                       \
			(lensize) = 1;                               \
			(len) = (ptr)[0] & 0x3f;                     \
		}else if((encoding) == ZIP_STR_14B){                 \
			(lensize) = 2;                               \
			(len) = (((ptr[0]) & 0x3f) << 8) | (ptr)[1]; \
		}else if(encoding == ZIP_STR_32B){                   \
			(lensize) = 5;                               \
			(len) = ((ptr)[1] << 24) |                   \
				((ptr)[2] << 16) |                   \
				((ptr)[3] << 8)  |	             \
				((ptr)[4]);                          \
		}else{                                               \
			assert(NULL);                                \
		}                                                    \
	}else{							     \
		/* Integer */                                        \
		(lensize) = 1;                                       \
		(len) = zipIntSize(encoding);                        \
	}                                                            \
}while(0)                                                           \
	
/**
 * Encoding the length of the previous entry and write it to 'p'.Return
 * the number of bytes needed to encode this length if 'p' is not NULL
 */
static unsigned int zipPrevEncodeLength(unsigned char *p, unsigned int len){
	if(p == NULL){
		return (len < ZIP_BIGLEN) ? 1 : sizeof(len) + 1;
	}

	if(len < ZIP_BIGLEN){
		p[0] = len;
		return 1;
	}else{
		p[0] = ZIP_BIGLEN;
		memcpy(p+1, &len, sizeof(len));
		memrev32ifbe(p+1);
		return 1+sizeof(len);
	}
}

/**
 * Encoding the length of the previous entry and write it to 'p'.This only uses
 * the larger encoding
 */
static void zipPrevEncodeLengthForceLarge(unsigned char *p, unsigned int len){
	if(p == NULL) return;

	//Set 5 bytes flag
	p[0] = ZIP_BIGLEN;

	//write len
	memcpy(p+1, &len, sizeof(len));
	memrev32ifbe(p+1);
}

/**
 * Decode the number of bytes required to store the length of the previous element
 * , from the perspective of the entry pointed to by 'ptr'
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {     \
	if((ptr)[0] < ZIP_BIGLEN){			  \
		(prevlensize) = 1;			  \
	}else{						  \
		(prevlensize) = 5;                        \
	}						  \
}while(0);						  \

/**
 * Decode the length of the previous element, from the perspective of the entry 
 * pointed by 'ptr'
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prelen) do{                                  \
								                          \
	/*Caculate the bytes of encoded length*/			                  \
	ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);		                          \
								  	                  \
	/*If prev element len size is 1, it means the prevlens is less than 254*/         \
	if(prevlensize == 1){                                                             \
		(prevlen) = (ptr)[0];							  \
	}else if((prevlensize) == 5){							  \
		memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);				  \
		memrev32ifbe(&prevlen);							  \
	}										  \
}while(0);										  \

/**
 * Return the difference in number of bytes needed to store the length of the 
 * previous element 'len', in the entry pointed to by 'p'
 */
static int zipPrevLenByteDiff(unsigned char *p, unsigned int len){
	unsigned int prevlensize;

	//Get the previous element length which is point by p
	ZIP_DECODE_PREVLENSIZE(p, prevlensize);

	//Caculate the the bytes that need to store len
	return zipPrevEncodeLength(NULL, len) - prevlensize;
}

/**
 * Return the total number of bytes used by the entry pointed to by 'p'.
 * T = O(1)
 */
static unsigned int zipRawEntryLength(unsigned char *p){
	unsigned int prevlensize, encoding, lensize, len;

	//Get the number of bytes needed to store the length of previous element
	ZIP_DECODE_PREVLEN(p, prevlensize);

	//Get current element 
	ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
	
	//return the total size 
	return prevlensize + lensize + len;
}

/**
 * Check if string pointed by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'.
 *
 * Note the entry here is not the [structure entry].
 *
 */
static int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding){
	long long value;

	if(entrylen >= 32 || entrylen == 0) return 0;

	//Try to convert
	if(string2ll((char*)entry, entrylen, &value)){
		/**
		 * This means this string can be encoded. Check what's the smallest
		 * of our encoding types that can hold this value.
		 */
		
		//For this type, the encoding and data are combine into one byte
		//so we put them both in encoding.
		if(value >= 0 && value <= 12) *encoding = ZIP_INT_IMM_MIN + value;
		else if(value >= INT8_MIN && value  <= INT8_MAX) *encoding = ZIP_INT_8B;
		else if(value >= INT16_MIN && value <= INT16_MAX) *encoding = ZIP_INT_16B;
		else if(value >= INT24_MIN && value <= INT24_MAX) *encoding = ZIP_INT_24B;
		else if(value >= INT32_MIN && value <= INT32_MAX) *encoding = ZIP_INT_32B;
		else *encoding = ZIP_INT_64B;

		*v = value;
		return 1;
	}	
	return 0;
}

/**
 * Resize the ziplist
 * Resuze the ziplist to len size, if the current ziplist size is bigger than len, then we do nothing
 */
static unsigned char *ziplistResize(unsigned char *zl, unsigned int len){
	
	//use the zrealloc which is not change the current value
	zl = zrealloc(zl, len);

	ZIPLIST_BYTES(zl) = intrev32ifbe(len);

	//reset the end
	zl[len - 1] = ZIP_END;
	
	return zl;
}

/**
 * When an entry is inserted, we need to set the prevlen field of the next entry to equal the
 * length of the inserted entry.It can occur that this length cannot be encoded in 1 bytes and 
 * the next entry needs to be grow a bit larger to hold the 5-bytes encoded prelen.This can be 
 * done for free, because this only happens when an entry is already being inserted(which cause
 * a relloc and memmove).However, encoding the prevlen may require that this entry is grown as
 * well.This effect may cascade throughout the ziplist when there are consecutive entries with 
 * a size close to ZIP_BIGLEN, so we need to chekc that the prevlen can be encoded in every 
 * consecutive entry.
 */

/**
 * Note that this effect can also happen in reverse, where the bytes required to encode the prevlen
 * field can shrink. This effect is deliberately ingored, because it can cause a "flapping" effect where
 * a chain prevlen fields is first grown and then shrunk again after consecutive insert. Rather, the 
 * field is allowed to stay larger than necessary, because a large prevlen field imples the ziplist
 * is holding large entries anyway.
 */

/**
 * The pointer "p" points to the first entry that does NOT need to be updated, i.e consecutive fields MAY need 
 * an update.
 *
 * T = O(N^2)
 */
static unsigend char *__ziplistcascadeUpdate(unsigned char *zl, unsigned char *p){

	//Here please note, the curlen is the value of the total bytes of the ziplist.	
	size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
	size_t offset, noffset, extra;
	unsigned char *np;
	zlentry cur, next;

	/**
	 * The following code is deal with this situation, we start from entry(may be the inserted entry),
	 * and move backward each loop, until we reach the end or we meet and entry that doesn't need to 
	 * resize.
	 */
	while(p[0] != ZIP_END){
		//save the data encapulate in cur
		cur = zipEntry(p);
		//caculate the current entry length
		rawlen = cur.headersize + cur.len;

		//caculate how many bytes  the variable that hold the cursize need
		rawlensize = zipPrevEncodingLength(NULL, rawlen);

		/*If we aleardy reach the end, there is no need to continue*/
		if(p[rawlen] == ZIP_END) break;
		//We encapsulate the next entry info into the variable next
		next = zipEntry(p+rawlen);
		
		/**
		 * Abort when "prevlen" has not changed
		 * This means the value of next entry prerawlen is
		 * equals to rawlen
		 */
		if(next.prevrawlen == rawlen) break;
		/**
		 * This case we need to resize the following entry prevlen variable
		 */
		if(next.prevrawlensize < rawlensize){
			/**
			 * The example of ziplist is illustrate in the destop png file
			 */
			
			offset = p - zl;
			extra = rawlensize - next.prevrawlensize;
			//Here hides a detial, the ziplistResize method will mark the last
			//bytes of the new zl as 0XFF.
			zl = ziplistResize(zl, curlen+extra);
			//Restore the pointer p
			p = zl + offset;
			//use np points to the next entry
			np = p + rawlen;
			noffset = np - zl;

			/**
			 * If the next entry is not the last entry, it means when we move the bytes 
			 * after the current entry, we may change the position of the last entry,
			 * so we do some work to make sure the zl + tail offset always points to 
			 * the last entry.
			 */
			if((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np){
				ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
			}
			
			/**
			 * Here is a detail, the offset is curlen - offset - next.prevrawlensize - 1 not 
			 * curlen - offset - next.prevrawlensize.Why?
			 *
			 * Recall that the ziplistResize already mark the last byte to 0XFF, so why we do the shift
			 * we do not need to care the last bytes(0XFF), so the offset need to minus 1.
			 */
			memmove(np + rawlensize, np + next.prevrawlensize, curlen - noffset - next.prevrawlensize - 1);
			//Change the prevlen field of the next entry
			zipPrevEncodeLength(np, rawlen);

			/**
			 * Advanced the pointer
			 */
			p += rawlen;
			curlen + extra;
		}else{
			if(next.prevrawlensize > rawlensize){
				/**
				 * This case would result in shrinking, which we want to avoid, but we do not shrink the
				 * prevrawlen field of next entry, we only write the rawlen into the next entry prevrawlen field.
				 */
				zipPrevEncodedLengthForceLarge(p+rawlen, rawlen);
			}else{
				//When comes to here it means the cur length can just fit in the next entry rawlen field.
				zipPrevEncodeLength(p+rawlen, rawlen);
			}
			break;
		}		
	}
	return zl;
}

/**
 * Delete "num" entries, starting at "p". Returns pointer to ziplist.
 *
 * T = O(N^2)
 */
static unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num){
	unsigned int i, totlen, deleted = 0;
	size_t offset;
	int nextdiff = 0;
	zlentry first, tail;

	//Caculate the total bytes of the deleted entry.
	first = zipEntry(p);
	for(i = 0; p[0] != ZIP_END && i < num; i++){
		p += zipRawEntryLength(p);
		deleted++;
	}
	//totlen: it is the total bytes of deleted entries
	totlen = p - first.p;
	if(totlen > 0){
		if(p[0] != ZIP_END){
			//When comes in this if block, it means there are entries after the deleted entries.
			
			//Because the first entry after the deleted entries may not have enough room to store
			//the length of previous entry, so we may need to some extra work, to keep this ziplist
			//corret.
			//This nextdiff holds the different between the bytes of bytes of first.prevrawlen and p prevlen.
			nextdiff = zipPrevLenByteDiff(p, first.prevrawlen);
			//If needed, move backward the pointer p to leave space for new header
			p -= nextdiff;
			//encode the prevlen into the p
			zipPrevEncodeLength(p, first.prevrawlen);

			/*Update the offset for tail*/
			ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) - totlen);
			
			/**
			 * When the tail contains more than one entry, we need to take "nextdiff" in account as well.
			 * Otherwise, a change in the size of prevlen dosen;t have an effect on the *tail* offset
			 *
			 * This seems hard to understand, let me try to explain this.For example, you have a ziplist, which
			 * contains 4 entries. If you start from p(entry2), and 3 entries(entry2, entry3, entry4) followed this entry, 
			 * and currently the zl + tail_offset points to entry4 ,you need to delete two entries.
			 * now the p' points to the entry4, beacause the entry is the entry which next to end, so p' - totlen points to
			 * the entry2, and the memove will put the content to the position of entry2.But if there are more entries after
			 * entry4, we need to some extra work to keep the tail offset is correct.
			 */
			tail = zipEntry(p);
			if(p[tail.headersize + tail.len] != ZIP_END){
				ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
			}
			//Move tail to the front of the ziplist
			memmove(first.p, p, intrev32ifbe(ZIPLIST_BYTES(zl)) - (p - zl) - 1);
		}else{
			//when comes here it means there is no entry after the deleted entry
			ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe((first.p - zl) - first.prevrawlen);
		}
		//Resize and update the length
		offset = first.p - zl;
		zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl)) - totlen + nextdiff);
		ZIPLIST_INCR_LENGTH(zl, -deleted);
		p = zl + offset;

		/**
		 * When nextdiff is not 0, the raw length of the next entry has been changed,
		 * so we need to cascade the update throughout the ziplist.
		 * T = O(N^2)
		 */
		if(nextdiff != 0){
			zl = __ziplistCascadeUpdate(zl, p);
		}
	}
	return zl;	
}

/**
 * Insert  item at "p".
 *
 * T = O(N ^ 2)
 */
static unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen){
	//caculate the current ziplist length
	size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen , prevlen = 0;
	size_t offset = 0;
	int nextdiff = 0;
	unsigned char encoding = 0;
	long long value = 123456789; //Use a dummy value to init the value to avoid the compiler warning.

	zlentry entry, tail;

	/**Find out prevlen for the entry that is inserted**/
	if(p[0] != ZIP_END){
		//If the p[0] does not points to the end of the list, it means the ziplist is not empty,
		//we need to consider the previous entry length, because for an entry it consisit of three
		//parts: prev-len, encoding, content
		entry = zipEntry(p);
		prevlen = entry.prevrawlen;
	}else{
		//If the p points to the tail. We still need to figure out if it is an empty ziplist or an 
		//unempty ziplist, because for an unempty list we do not need to concern the length of previous
		//entry
		unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
		if(ptail[0] != ZIP_END){
			prevlen = zipRawEntryLength(ptail);
		}
	}
	/**
	 * After we determine the first part of entry --- prevlen, we need to consider the second part of an 
	 * entry ----- encoding, we use a very intuitive way, we first try to convert the input string into 
	 * a number, if failed it means we can not use an number to represent this input in the ziplist.
	 */
	if(zipTryEncoding(s, slen, &value, &encoding)){
		//If the input can be represent as a number, we use the function to determine how many space
		//we need to hold the content
		req = zipIntSize(encoding);
	}else{
		//Why comes to here, we know the input need to be represent as bytes array.
		//Notice we haven't decide the encoding for this string input, Thats explains
		//why the function zipEncodeLength caculate the how much bytes to encode such a
		//length use length not encoding.
		reqlen = slen;
	}
	reqlen += zipPrevEncodeLength(NULL, prevlen);

	reqlen += zipEncodeLength(NULL, encoding, slen);
	
	//After the below caculation, the reqlen holds the total bytes to store a new input.
	
	nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p, reqlen) : 0;
	/**
	 * When the insert position is not equals to the tail, we need to make sure that the next entry
	 * can hold this entry's length in its prevlen field.
	 * 
	 * The nextdiff variable holds the difference between the new lensize and the old lensize.
	 */
	offset = p - zl;
	/**
	 * curlen is the original length of the ziplist
	 * reqlen is the new inserted entry length
	 * nextdiff is the different between the new prevlen and the old prevlen which can be
	 * -4 0 4
	 */
	zl = ziplistResize(zl, curlen + reqlen + nextdiff);
	p = zl + offset;

	/*Apply memory move when necessary and update tail offset*/
	if(p[0] != ZIP_END){
		//When we comes here, it means we need to some extra work.
		//We need make space for new entry, and to rewrite the prev field of the next entry
		//after p.
		memove(p + reqlen, p - nextdiff, curlen - offset - 1 + nextdiff);
		//Encode this entry's raw length in the next entry
		zipPrevEncodeLength(p+reqlen, reqlen);

		/*Update offset for tail*/
		ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + reqlen);
		
		tail = zipEntry(p+reqlen);
		if(p[reqlen + tail.headersize + tail.len] != ZIP_END){
			ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
		}
	}else{
		/*The tail now points to the new insert entry*/
		ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p - zl);
	}
	
	/**
	 * When the nextdiff is not 0, the raw length of the next entry has changhed, so we need to
	 * cascade the update throughout the ziplist
	 */
	if(nextdiff != 0){
		offset = p - zl;
		zl = __ziplistCascadeUpdate(zl, p+reqlen);
		p = zl + offset;
	}

	p += zipPrevEncodeLength(p, prevlen);
	//wirte the 'encode' value into entry encode field
	p += zipEncodeLength(p, encoding, slen);

	if(ZIP_IS_STR(encoding)){
		memcpy(p, s, slen);
	}else{
		zipSaveInteger(p, value, encoding);
	}
	ZIPLIST_INCR_LENGTH(zl, 1);
	return zl;
}

/**
 *  push the string s of length slen into the ziplist zl.
 *  where parameter determines the direction.
 *  If the where equals to ZIPLIST_HEAD, the new value push to the head
 *  otherwise push to the tail.
 *
 *  T = O(N ^ 2)
 */
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where){
	
	unsigned char *p;
	p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
	
	return __ziplistInsert(zl, p, s, slen);
}

/**
 * Returns an offset to use for iterating with ziplistNext. When the given index
 * is negetaive, the list is traversed back to front. When the list doesn't contain
 * an element at the provided index, NULL is returned.
 * 
 * T = O(N)
 */
unsigned char *ziplistIndex(unsigned char *zl, int index){
	
	unsigned char *p;

	zlentry entry;

	if(index < 0){
		//convert to positive number
		index = (-index) - 1;
		p = ZIPLIST_ENTRY_TAIL(zl);
		if(p[0] != ZIP_END){
			entry = zipEntry(p);
			while(entry.prevrawlen > 0 && index--){
				p -= entry.prevrawlen;
				entry = zipEntry(p);
			}
		}
	}else{
		p = ZIPLIST_ENTRY_HEAD(zl);
		while(p[0] != ZIP_END && index--){
			p += zipRawEntryLength(p);
		}
	}
	return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

/**
 * Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist.
 * p is the pointer to the current element.
 * the element after p is returned, otherwise NULL if we are at the end.
 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p){
	
	if(p[0] == ZIP_END){
		return NULL;
	}
	p += zipRawEntryLength(p);
	if(p[0] == ZIP_END){
		return NULL;
	}
	return p;

}

/**
 * Return pointer to previous entry in ziplist
 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p){
	
	zlentry entry;
	if(p[0] == ZIP_END){
		//When the p equals to ZIP_END, it may cause by two condition
		//1.The ziplist is empty.
		//2.Just start the iteration from the tail.
		p = ZIPLIST_ENTRY_TAIL(zl);
		
		return (p[0] == ZIP_END) ? NULL : p;
	}else if(p == ZIPLIST_ENTRY_TAIL(zl)){
		return NULL;
	}else{
		entry = zipEntry(p);
		return p - entry.prevrawlen;
	}
}

/**
 * Get entry pointed by 'p' and store in either 'e' or 'v' depending 
 * on the encoding of the entry. 'e' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Returns 0 if 'p' points to the end of ziplist, 1 otherwise. 
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval){
	
	zlentry entry;
	if(p == NULL || p[0] == ZIP_END) return 0;
	//do the default init
	if(sstr) *sstr = NULL;

	entry = zipEntry(p);
	if(ZIP_IS_STR(entry.encoding)){
		if(sstr){
			*slen = entry.len;
			*sstr = p +entry.headersize;
		}
	}else{
		if(sval){
			*sval = zipLoadInteger(p+entry.headersize, entry.encoding);
		}
	}
	return 1;
}


/**
 * Insert an entry at "p"
 *
 * if p points to a entry, insert the new entry before the entry (p points to)
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen){
	return __ziplistInsert(zl, p, s, slen);
}

/**
 *Delete a single entry from the ziplist, pointed to byt *p.
 *Also update *p in place, to be able to iterate over the 
 *ziplist, while deleting entries.
 *
 *T = O(N ^ 2)
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p){
	//Because t__ziplistDelete will change the memory layout
	//And the resize may change the zl memory address.
	//So we record the offset, when we delete the entry we can
	//use the offset to put *p into right position.
	size_t offset = *p - zl;
	zl = __ziplistDelete(zl, *p, 1);

	/**
	 * Store pointer to current element in p, because ziplistDelete will do a
	 * realloc which might result in a different "zl" pointer.
	 * When the delete direction is back to front, we might deleted the last entry
	 * and end up with "p" pointing to ZIP_END, so check this.
	 */
	*p = zl + offset;
	return zl;
}

/**
 * Delete a range of entries from the ziplist 
 */
unsigned char *ziplistDeleteRange(unsigned char *zl, unsigned int index, unsigned int num){
	
	//Find the postion where the index indicate
	unsigned char *p = ziplistIndex(zl, index);

	return (p == NULL) ? zl : __ziplistDelete(zl, p, num);
}

/** 
 * Compare entry pointer 'p' with 'etnry.Return 1 if equal.
 *
 * If the entry value is equals to sstr return 1, otherwise return 0.
 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen){

	long long sval, zval;
	if(p[0] == ZIP_END){
		return 0;
	}	

	entry = zipEntry(p);
	if(ZIP_IS_STR(entry.encoding)){
		if(entry.len == slen){
			return memcpy(p+entry.headersize, sstr, slen) == 0;
		}else{
			return 0;
		}	
	}else{
		//If the encoding indicate it is a number, so try to
		//convert sstr  to a long long type, it may failded, beacause
		//the sstr may not be represent in a 64bit number.
		if(zipTryEncoding(sstr, slen, &sval, &encoding)){
			zval = zipLoadInteger(p+entry.headersize, entry.encoding);
			return zval == sval;
		}
	}
	return 0;
}

/**
 * Return length of ziplist
 *
 * T = O(N)
 */
unsigned int ziplistLen(unsigned char *zl){
	
	unsigned int len = 0;

	//When the node size is less than UINT_16MX
	if(intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX){
		len = interev16ifbe(ZIPLIST_LENGTH(zl));
	}else{
		//When the size is greater than UINT16_MAX, we can not 
		//simply get the value from the ziplist header, we need
		//to loop through this ziplist.
		unsigned char *p = zl + ZIPLIST_HEADER_SIZE;
		while(*p != ZIP_END){
			p += zipRawEntryLength(p);
			len++;
		}
		//Restore length if smalll enough
		if(len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
	}
	return len;
}

/**
 *Return ziplist blob size in bytes
 */
size_t ziplistBlobLen(char *zl){
	return intrev32ifbe(ZIPLIST_BYTES(zl));
}

/**
 * Find pointer to the entry equal to the specified entry.
 *
 * Skip 'skip' entries between every comparsion.
 *
 * Returns NULL when the field could not be found.
 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip){
	
	int skipcnt = 0;
	unsigned char vencoding = 0;
	long long vll = 0;

	//If we never touch the end of the tail, we continue loop
	while(p[0] != ZIP_END){
		unsigned int prevlensize, encoding, lensize, len;
		unsigned char *q;
		
		ZIP_DECODE_PREVLENSIZE(p, prevlensize);
		ZIP_DECODE_LENGTH(p+prevlensize, encoding, lensize, len);
		q = p + prevlensize + lensize;

		if(skipcnt == 0){
			/**
			 * When the skipcnt is 0, it means we get a entry which we need to compare.
			 */
			if(ZIP_IS_STR(encoding)){
				if(len == vlen && memcpy(q, vstr, vlen) == 0)
					return p;
			}else{
				//This means current entry store a number 
				//so we need try to convert the vstr to a number and do the comparsion.
				//We use the vencoding as a flag to indicate whether we have done the convertion
				//if we did, the corresponding integer value is stored in vll, we do not need to
				//do that again.
				if(vencoding == 0){
					if(!zipTryEncoding(vstr, vlen, &vll, &vencoding)){
						/**
						 * If the vstr can't be convert to a number
						 * then mark it.
						  */
						 vencoding = UCHAR_MAX;
					}
				}
				//Compare happened only when the vstr can be convert to integer
				if(vencoding != UCHAR_MAX){
					long long ll = zipLoadInteger(q, encoding);
					if(ll == vll){
						return p;
					}
				}
			}
			skipcnt = skip;
		}else{
			skipcnt--;
		}

		p = q + len;
	}
	return NULL;
}






