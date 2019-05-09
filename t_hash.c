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

/**
 * Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 * 
 * This function will take care of incrementing the reference 
 * count of the retained fields and value objects.
 * 
 * Return 0 means this element is already exists, this time
 * is an update operation.
 * 
 * Return 1 means this elements is not exists, this time 
 * is an add operation.
 */ 
int hashTypeSet(robj *o, robj *field, robj *value){
    int update = 0;

    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *zl, *fptr, *vptr;
        
        robj *field = getDecodedObject(field);
        robj *value = getDecodedObject(value);
        
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if(fptr != NULL){
            fptr = ziplistFind(zl, field->ptr, sdslen(field->ptr), 1);
            if(fptr != NULL){
                //We find this element key
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);
                update = 1;

                //This means we need to modify this content.
                ziplistDelete(zl, &vptr);
                ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
            }
        }

        if(!update){
            /*When we can't find this element, we push it into the tail of the ziplist*/
            ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        }
        //Remember the operation to the ziplist may change the pointer of the original
        //ziplsit.
        o->ptr = zl;
        decrRefCount(value);
        decrRefCount(field);

        /**
         * We need to check after this set operation did we exceeds the maxmum of number
         * if ziplist
         */
        if(hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);
    }else if(o->encoding == REDIS_ENCODING_HT){
            //Replace this key in the dictionary
            if(dictReplace(o->ptr, field, value)){
                incrRefCount(field);
            }else{
                update = 1;
            }
    }else{
        redisPainc("Unknown type");
    }
    return update;
}

/**
 * Delete an element from a hash
 * Returns 1 on  deleted and 0 on not found
 */ 
int hashTypeDelete(robj *o, robj *field){
    int deleted = 0;
    
    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *zl, *fptr, vptr;
        field = getDecodedObject(field);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if(fptr != NULL){
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if(fptr != NULL){
                //The element is exists
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);
                zl = ziplistDelete(zl, &fptr);
                zl = ziplistDelete(zl, &vptr);
                o->ptr = zl;
                deleted = 1;
            }
        }
        decrRefCount(field);
    }else if(o->encoding == REDIS_ENCODING_HT){
        if(dictDelete((dict*)o->ptr, field) == DICT_OK){
            deleted = 1;
            /*Always check if the dictionary needs a resize after the delete*/
            if(htNeedsResize(o->ptr)) dictResize(o->ptr);
        }
    }else{
        redisPainc("Unknown type");
    }
    return deleted;
}

/**
 * Returns the number of elements in hash 
 */
unsigned long hashTypeLength(robj *o){
    unsigned long len;
    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        len = ziplistLen(o->ptr) / 2;
    }else if(o->encoding == REDIS_ENCODING_HT){
        len = dictSize((dict *)o->ptr);
    }else{
        redisPainc("Unknown type");
    }
    return len;
}

/**
 * Create a hash type iterator
 * Time Complex: O(1) 
 */
hashTypeIterator *hashTypeInitIterator(robj *subject){
    hashTypeIterator *it = zmalloc(sizeof(hashTypeIterator));
    it->subject = subject;
    it->encoding = subject->encoding;

    if(it->encoding == REDIS_ENCODING_ZIPLIST){
        it->fptr = NULL;
        it->vptr = NULL;
    }else if(it->encoding == REDIS_ENCODING_HT){
        it->di = dictGetIterator(subject->ptr);
    }else{
        redisPainc("Unknown type");
    }
    return it;
}

/**
 * Release the iterator 
 */ 
void hashTypeReleaseIterator(hashTypeIterator *hi){
    if(hi->subject->encoding == REDIS_ENCODING_HT){
        zfree(hi->di);
    }
    zfree(hi);
}

/**
 * Move to the next entry in the hash.
 * Could be found and REDIS_ERR when the iterator reaches the end.
 * If found the REDIS_OK will return.
 */ 
int hashTypeNext(hashTypeIterator *hi){
    if(hi->encoding == REDIS_ENCODING_ZIPLIST){
        //todo
    }else if(hi->encoding == REDIS_ENCODING_HT){
        if((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;
    }else{
        redisPainc("Unknown type");
    }
    return REDIS_OK;
}

