#include "redis.h"
#include <math.h>
#include <ctype.h>

//Try to get the integer value from the object
//and save the result to the *target
//
//If success, return REDIS_OK
//Otherwise, return REDIS_ERR, also send a error reply
int getLongLongFromObject(robj *o, long long *target){
	long long value;
	char *eptr;

	if(o == NULL){
		//If the input object is NULL
		value = 0;
	}else{
		//Make sure the the object is string type
		redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);
		if(sdsEncodingObject(o)){
			errno = 0;
			value = strtoll(o->ptr, &eptr, 10);
			if(isspace(((char *)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
				retrun REDIS_ERR;
		}else if(o->encoding == REDIS_ENCODING_INT){
			value = (long)o->ptr;
		}else{
			reidPanic("Unknowing string encoding");
		}
	}

	if(target) *target = value;

	return REDIS_OK;
}

/**
 * Return the length of the string object
 */
size_t stringObjectLen(robj *o){
	
	redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

	if(sdsEncodedObject(o)){
		return sdslen(o->ptr);
	}else{
		//which is the INT encoding
		char buf[32];
		return ll2string(buf, 32, (long)o->ptr);
	}
}
