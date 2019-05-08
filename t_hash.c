#include "redis.h"
#include <math.h>

/*------------------------------------------------------------
 *          Hash Type API
 *------------------------------------------------------------*/

/*
 * Check in the given range if the size of the element exceeds
 * the limit of single ziplist element.
 * This program only check the string, because for an integer
 * the size can never exceed the defalut size limit(64bytes).
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end){
    int i;
    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        return;
    }
    for(i = start; i <= end; i++){
        if(sdsEncodedObject(argv[i]) && sdslen(argv[i]->ptr) > server.hash_max_ziplist_value){
            //Convert object to the dict
            hashTypeConvert(o, REDIS_ENCODING_HT);
            break;
        }
    }
}

/**
 * Encoding given objects in-place when the hash use a dict.
 * When the subject encoding is REDIS_ENCODING_HT, we try to 
 * encode both o1 and o2 to save more memory. 
 * 
 * This part we use tge robj **, beacause the input will be
 * the argv[i], which is a pointer, and we need to modify the
 * content of this argv[i] points to, so we use the robj **.
 */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2){
    if(subject->encoding == REDIS_ENCODING_HT){
        if(*o1) *o1 = tryObjectEncoding(*o1);
        if(*o2) *o2 = tryObjectEncoding(*o2);        
    }
}

/**
 * Get the value from from a ziplist encoded hash, indentified by field.
 * Returns -1 when the field cannot be found.
 * 
 * parameters:
 * field : 
 * vstr : which is a string pointer, the value will will eventually save
 *        into here(the ziplist entry save a char array).
 * vlen : the saved string length.
 * ll   : if the ziplist entry is integer, then saved to here.
 */ 
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen, 
                           long long *vll){
    
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    //Make sure this works on the ziplist
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    //Get the field, this field may be encoded
    field = getDecodedObject(field);

    //Loop through the whole list
    zl = o->ptr;
    //Get the first entry
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);

    /*If this list is not empty*/
    if(fptr != NULL){
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        //If we find this key in the ziplist
        if(fptr != NULL){
            /*Grab correspond value */
            vptr = ziplistNext(zl, fptr);
            redisAssert(vptr != NULL);
        }
    }
    //Here is a ref decrment because the getDecodeObject function
    //it will return a new object or incr the original reference count
    //when it is a RAW string.
    //So here after we use this field, we need to decr this ref for 
    //GC. 
    decrRefCount(field);
    
    //Get the value from the ziplist
    if(vptr != NULL){
        ret = ziplistGet(vptr, vstr, vlen, vll);
        redisAssert(ret);
        return 0;
    }
    return -1;
}

/**
 * Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found.
 * 
 */ 
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value){
    dictEntry *de;
    
    redisAssert(o->encoding == REDIS_ENCODING_HT);

    de = dictFind(o->ptr, field);

    if(de != NULL){
        *value = dictGetVal(de);
        return 0;
    }
    return -1;
}

/**
 * High level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 * 
 * The lower level function can prevent copy on write so it is 
 * the preferred way of ding read operation.
 * 
 * If not found, return NULL.
 */ 
robj *hashTypeGetObject(robj *o, robj *field){
    robj *value = NULL;
    if(o->encoding == REDIS_ENCODING_HT){
        robj *aux;
        if(hashTypeGetFromHashTable(o, field, &aux) == 0){
            incrRefCount(aux);
            value = aux;
        }
    }else if(o->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if(hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0){
            if(vstr){
                value = createStringObject(vstr, vlen);
            }else{
                value = createStringObjectFromLongLong(vll);
            }
        }
    }else{
        redisPainc("Unknown type");
    }
    return value;
}

/**
 * Test if the specified field exists in the given hash.Returns 1 if the field
 * exists, and 0 when it doesn't.
 * 
 * If exists returns 1 otherwise return 0.
 */ 
int hashTypeExists(robj *o, robj *field){
    unsigned char *vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;
    robj *val;

    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        if(hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    }else if(o->encoding == REDIS_ENCODING_HT){
        if(hashTypeGetFromHashTable(o, field, &val) == 0) return 1;
    }else{
        redisPainc("Unknown type");
    }
    return 0;
}
