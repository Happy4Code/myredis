#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#incluse "endianconv.h"

/**
 * In redis, the underlying array is int8_t type, we use the
 * the encoding to specify the content in it is int16_t, int_32_t
 * or int64_t, when the content is int16_t, it use to 2 cells(each one
 * is uint8_t type) to represent the int16_t data.
 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/**
 * Return the required enconding for the provided value
 * T = O(1)
 * */
static uint8_t _intsetValueEncoding(int64_t v){
	if(v < INT32_MIN || v > INT32_MAX)
		return INTSET_ENC_INT64;
	if(v < INT16_MIN || v > INT16_MAX)
		return INTSET_ENC_INT32;
	return INTSET_ENC_INT16;
}

/**
 * Return the value at pos, given an encoding
   Redis use the little-endian to save and retrive
   data.
 */
static int64_t _intsetGetEncoded(intset *in, int pos, uint8_t enc){
	int64_t v64;
	int32_t v32;
	int16_t v16;

	if(enc == INTSET_ENC_INT64){
		memcpy(&v64, ((int64_t*)is->content)+pos, sizeof(v64));
		memrev64ifbe(&v64);
		return v64;
	} else if(enc == INTSET_ENC_INT32){
		memcpy(&v32, ((int32_t*)is->content)+pos, sizeof(v32));
		memrev32ifbe(&v32);
		return v32;
	}else{
		memcpy(&v16, ((int16_t*)is->content)+pos, sizeof(v16));
		memrev16ifbe(&v16);
		return v16;
	}
}

/**
 * Return the value at pop, using the configured encoding
 */
static int64_t _intsetGet(intset *is, int pos){
	uint8_t encoding = intrev32ifbe(is->encoding);
	return _intsetGetEncoded(is, pos, encoding);
}

/**
 * Set the value at pos, using the configured encoding
 */
static void _intsetSet(intset *is, int pos, int64_t value){
	//Get the collection encoding, because the set info
	//may save as little, so do the convert operation if
	//necessary
	uint32_t encoding = intrev32ifbe(is->encoding);
	if(encoding == INTSET_ENC_INT64){
		((int64_t*)is->content)[pos] = value;
		memrev64ifbe(((int64_t*)is->contents)+pos);
	}else if(encoding == INTSET_ENC_INT32){
		((int32_t*)is->content)[pos] = value;
		memrev32ifbe(((int32_t*)is->contents)+pos);
	}else{
		((int16_t*)is->content)[pos] = value;
		memrev16ifbe(((int16_t*)is->content)+pos)
	}
}

/**
 * Create an empty inset
 */
intset *intsetNew(void){
	//Allocate memory space for the empty int collection
	intset *is = zmalloc(sizeof(intset));

	//Set the default encoding
	is->encoding = intrev32ifbe(INTSET_ENC_INT16);

	//Init number of element 
	is->length = 0;

	return is;
}

/**
 * Resize the intset
 *
 * T = O(N)
 */
static intset *intsetResize(intset *is, uint32_t len){
	
	//Caculate the size of resize space
	uint32_t size = len * intrev32ifbe(is->encoding);

	is = zrealloc(is, size);
	return is;
}

/**
 * Search for the positin of "value"
 *
 * Return 1 when the value was found and set "pos" to
 * the position of the value within the inset,
 *
 * Return 0 when the value is not present in the intset 
 * and set "pos" to the position where "value" can be insert.
 *
 * T = O(logN) 
 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos){
	
	int min = 0, max = intrev32ifbe(is->length) - 1, mid = -1;	
	int64_t cur = -1;

	/**
	 * The value can never be found when the set is empty
	 */
	if(intrevifbe(is->length) == 0){
		if(pos) *pos = 0;
		return 0;
	}else{
		/**
		 * Check fo the case where we know we cannot find the 
		 * value , but do know the insert position.
		 * Because the underlaying array is sorted, so we can
		 * check if the value is greater than the last element,
		 * than means the value is greater than all the value in
		 * the array.
		 */	
		if(value > _intsetGet(is, intrev32ifbe(is->length) - 1)){
			if(pos) *pos = intrev32ifbe(is->length);
			return 0;
		}else if(value < _intsetGet(is, 0)){
			if(pos) *pos = 0;
			return 0;
		}
	}
	//Becase the underlying array is a sorted array, we can use
	//the binary search to find the proper position.
	while(max >= min){
		mid = (min + max) / 2;
		cur = _intsetGet(is, mid);
		if(value > cur){
			min = mid + 1;
		}else if(value < cur){
			max = mid - 1;
		}else
			break;
	}

	//Check if we find the value
	if(value == cur){
		if(pos) *pos = mid;
		return 1;
	}else {
		if(pos) *pos = min;
		return 0;
	}
}

/**
 * Upgrade the intset to a larger encoding and insets the given integer.
 *
 * T = O(N)
 */
static void intset *intsetUpgradeAndAdd(intset *is, int64_t value){
	
	//Get current encoding
	uint8_t curenc = intrev32ifbe(is->encoding);

	//Get the new encoding from the input value
	uint8_t newenc = _intsetValueEncoding(value);

	int length = intrev32ifbe(is->length);

	int prepend = value < 0 ? 1 : 0;

	is->encoding = intrev32ifbe(newenc);
	//According the new encoding to change the underlying space
	//T = O(N)
	is = intsetResize(is, intrev32ifbe(is->length)+1);
	
	/**
	 * Update back-to-front so we don't overwrite values.
	 * Note that the "prepend" variable is used to make sure
	 * we have an empty space at either the beginning or the 
	 * end of the intset
	 *
	 * For example:there is a intset which contains three elements.
	 * |x|y|z|
	 * when we do the resize the space is getting larger
	 * |x|y|z|?|?|?|
	 * The program start from the end to adjust the element
	 * |x|y|z|?|z|?|
	 *
	 * |x|y| y | z | new space|
	 *
	 * Finally into 
	 * | x | y | z | new |
	 *
	 * Because the value is ethier greater than all elements in the 
	 * array or smaller than all elements in the array.So it means
	 * it either insert into the head or tail of the list.So we use
	 * the prepend to determine whether to insert it into the head
	 * or the end.
	 */
	while(length--){
		_intsetSet(is, length+prepend, _intsetGetEncoded(is, length, curenc));
	}
	if(prepend)
		_intsetSet(is, 0, value);
	else
		_intsetSet(is, intrev32ifbe(is->length), value);

	//update the length of the set
	is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);
	return is;
}

/**
 * Move the elements in given range forward or backward.
 *
 * This function is very useful when we do the insert or
 * delete operation.Because when we insert(except we insert
 * them into the head or tail) we need to shift the elements.
 * When we do the delete operation we may also shift the elements.
 *
 * T = O(N)
 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to){
	void *src, *dst;

	//The number of elements that need to be shifted
	uint32_t bytes = intrev32ifbe(is->length) - from;

	//The encoding 
	uint32_t encoding = intrev32ifbe(is->encoding);

	//Because the underlying array is int8_t types, 
	//so as we describe before, we combine there them 
	//according to different encoding to represent differnt
	//types of intset.
	if(encoding == INTSET_ENC_INT64){
		src = (int64_t*)is->contents + from;
		dst = (int64_t*)is->contents + to;
		bytes *= sizeof(int64_t);
	}else if(encoding == INTSET_ENC_INT32){
		src = (int32_t*)is->contents + from;
		dst = (int32_t*)is->contents + to;
		bytes *= sizeof(int32_t);
	}else{
		src = (int16_t*)is->contents + from;
		dst = (int16_t*)is->contents + to;
		bytes *= sizeof(int16_t);
	}
	memmove(dst, src, bytes);
}

/**
 * Insert an integer in the intset
 * use the success to represent if the insert is success.
 * If *success is 1, it means insertion success.
 * If the insertion failed, because of the value is already exsited,
 * *success is 0
 *
 * T = O(N)
 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success){
	
	//Caculate the insert value encoding
	uint8_t valenc = _intsetValueEncoding(value);
	uint32_t pos;

	//Default set the success to 1
	if(success) *success = 1;

	/**
	 * Upgrade encoding if necessary.If we need to upgrade, 
	 * we know this value is either appended(>0) or prepended(<0)
	 * because it is out of range of existing value.
	 */
	if(valenc > intrev32ifbe(is->encoding)){
		return intsetUpgradeAndAdd(is, value);
	}else{
		//Because the set can not hold duplicate values,
		//so before we insert into the set, we check if
		//it is already existed.
		if(intsetSearch(is, value, &pos)){
			if(success) *success = 0;
			return is;
		}
		//When comes to here, it means the value is not
		//present in the collections.We allocate space
		//space for them
		is = intsetResize(is, intrev32ifbe(is->length)+1);

		if(pos < intrev32ifbe(is->length)) intsetMoveTail(is, pos, pos+1);
	}
	_intsetSet(is, pos, value);
}

/**
 * Delete integer from intset
 * 
 * *success indicate is the delete is success
 * -Delete error because of the value is not existed, *success is 0.
 * -Delete success, *success is 1.
 * T = O(N)
 */
intset *intsetRemove(intset *is, int64_t value, int *success){
	
	//Caculate the encoding value
	uint8_t valenc = _intsetValueEncoding(value);
	uint32_t pos;

	//Set the default value 
	if(success) *success = 0;

	//When the value is less or less equals than the current encoding,
	//we can execute the delete operation.
	if(valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, &pos)){
		//Get the number of elements inside
		uint32_t len = intrev32ifbe(is->length);

		if(succes) *success = 1;

		if(pos < (len - 1)) intsetMoveTail(is, pos+1, pos);
		
		is = intsetResize(is, len - 1);
		
		is->length = intrev32ifbe(len - 1);	
	}
	return is;
}

/**
 * Determine whether a value is belongs to this set
 * if exist returns 1 or returns 0
 *
 * T = O(logN)
 */
uint8_t intsetFind(intset *is, int64_t value){
	uint8_t valenc = _intsetValueEncoding(value);

	return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, NULL);
}

/**
 * Return a random number from the integer set
 * This is useful when the set is not empty.
 * T = O(1)
 */
int64_t intsetRandom(intset *is){
	return _intsetGet(is, rand() % intrev32ifbe(is->length));
}

/**
 * Sets the value to the value at the given position.When this position
 * is out of range the function returns 0, when in range it returns 1.
 *
 * T = O(1)
 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value){
	uint32_t len = intrev32ifbe(is->length);
	
	if(pos >= len || pos < 0) return 0;
	
	*value = _intsetGet(is, pos);
}

/**
 * Return intset length
 */
uint32_t intsetLen(intset *is){
	return intrev32ifbe(is->length);
}

size_t intsetBlobLen(intset *is){
	return sizeof(intset) + intrev32ifbe(is->length) * intrev32if(is->encoding);
}




