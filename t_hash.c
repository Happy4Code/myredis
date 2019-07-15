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
                value = createStringObject((char *)vstr, vlen);
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
        unsigned char *zl;
        unsigned char *fptr, vptr;

        fptr = hi->fptr;
        vptr = hi->vptr;
        zl = hi->subject->ptr;
        if(fptr == NULL){
            /*Init to ZIPLIST HEAD*/
            redisAssert(vptr == NULL);
            fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        }else{
            /**
             * If the fptr is not empty, it means it is not the first time
             * we iterate the ziplist, the vptr must not be NULL.
             */ 
            redisAssert(vptr == NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if(fptr == NULL) return REDIS_ERR;
        vptr = ziplistNext(zl, fptr);
        redisAssert(vptr == NULL);
        hi->fptr = fptr;
        hi->vptr = vptr;
    }else if(hi->encoding == REDIS_ENCODING_HT){
        if((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;
    }else{
        redisPainc("Unknown type");
    }
    return REDIS_OK;
}

/**
 *  Get the field or value at iterator cursor, for an iterator on a hash value
 *  encoded as a ziplist.Prototype is similar to `hashTypeGetFromZiplist`
 *  We use the what to indicate we want to get the `key` or the `value`
 *  from the ziplist.
 */ 
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what, 
                                unsigned char **vstr, 
                                unsigned int *vlen, 
                                long long *vll){
    int ret;

    //Make sure current working on a ziplist
    redisAssert(hi->encoding == REDIS_ENCODING_ZIPLIST);

    //Get the key
    if(what & REDIS_HASH_KEY){
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        redisAssert(ret);
    }else{
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        redisAssert(ret);
    }
}


/**
 * Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hashtable. Prototype is similar to `hashTypeGetFromHashTable`
 */ 
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst){

    //Make sure current working on a hashtable
    redisAssert(hi->encoding == REDIS_ENCODING_HT);

    if(what & REDIS_HASH_KEY){
        *dst = dictGetKey(hi->de);
    }else{
        *dst = dictGetVal(hi->de);
    }
}

/**
 * A non copy-on write friendly but higher level version of hashTypeCurrent*()
 * thats returns an object with incremented refcount(or a new object).
 * 
 * It is up to the caller to decrRefCount() the object if no reference is retained.
 */ 
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what){
    robj *value = NULL;
    if(hi->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if(vstr){
            value = createStringObject((char *)vstr, vlen);
        }else{
            value = createStringObjectFromLongLong(vll);
        }
    }else if(hi->encoding == REDIS_ENCODING_HT){
        hashTypeCurrentFromHashTable(hi, what, &value);
        incrRefCount(value);
    }else{
        redisPainc("Unknown type");
    }
    return value;
}

/**
 * Find an object in the database based on the `key`,
 * if existed return it otherwise create a new one.
 */ 
robj *hashTypeLookupWirteOrCreate(redisClient *c, robj *key){

    robj *o = lookupKeyWrite(c->db, key);
    if(o == NULL){
        o = createHashObject();
        dbAdd(c->db, key, o);
    }else{
        if(o->type != REDIS_HASH){
            addReply(c, shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

/**
 * Convert an object from ziplist encoding to hash encoding.
 */ 
void hashTypeConvertZiplist(robj *o, int enc){
    
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);
    
    if(enc == REDIS_ENCODING_ZIPLIST){

    }else if(enc == REDIS_ENCODING_HT){

        hashTypeIterator *hi =  hashTypeInitIterator(o);        
        dict *d = dictCreate(&hashDictType, NULL);
        robj *key, *value;
        int ret;

        while(hashTypeNext(hi) != REDIS_ERR){

            key = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
            value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);

            hashTypeTryObjectEncoding(d, &key, &value);
            ret = dictAdd(d, key, value);
             if (ret != DICT_OK) {
                redisLogHexDump(REDIS_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                redisAssert(ret == DICT_OK);
            }
        }

        hashTypeReleaseIterator(hi);
        free(o->ptr);
        o->ptr = d;
        o->encoding = REDIS_ENCODING_HT;
    }else{
        redisPainc("Unknown type");
    }
}

/**
 *  Do the encoding convert for hash object o.
 *  Current only working on ZIPLIST to HT. 
 */ 
void hashTypeConvert(robj *o, int enc){

    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        hashTypeConvertZiplist(o, enc);
    }else if(o->encoding == REDIS_ENCODING_HT){
        redisPanic("Not implemented");
    } else {
        redisPanic("Unknown hash encoding");
    }
}

/**
 * Hash type Command
 */
void hsetCommand(redisClient *c){
    int update;
    robj *o;

    if((o = hashTypeLookupWirteOrCreate(c, c->argv[1])) == NULL) return;

    hashTypeTryConversion(o, c->argv, 2, 3);
    
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    c->argv[3] = tryObjectEncoding(c->argv[3]);

    update = hashTypeSet(o, c->argv[2], c->argv[3]);

    addReply(c, update ? shared.czero : shared.cone);

    //Send the signal
    signalModifiedKey(c->db, c->argv[1]);

    //Notify the event
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hset", c->argv[1], c->db->id);

    //incr the server dirty
    server.dirty++;
}

/**
 * For a long time, i found a problem.When the redis save data as a t_hash type
 * they will encode the field and value before put them into the databse.But 
 * when they do some operation such as `hsetnxCommand` which needs to check
 * if a given field is existed.This time the input field is unencoded.Here 
 * arise a question, the data is saved as encoded, but when we query them
 * the input always are unencoded, how this work?
 * 
 * When we reading the source code of the tryObjectEncoding() method, and 
 * getDecodeObject() method, we make it out!
 * 
 * The tryObjectEncoding() method will do something:
 * 1.If the input object is a shared object, we do nothing.
 * 2.If the input is a EMBER or RAW encoding, we encoded it.
 * 3.If the input is a long data type, we use the ptr pointer to directly save it.
 * 
 * The getDecodeObject() method will restore the object as a string 
 * if it encoding is INT.Ohterwise it only increment the refCount.
 * 
 * So we need to figure out when tryObjectEncoding() encode an object, what does he do?
 * There a two ways:
 * 1.Encode it as an ember string, which combine the sds structure and the o->ptr.
 * 2.Free unnecessary space.
 * 
 * The two ways never change the content, it modify the structure of an object, but
 * keep o->ptr and o->len remains the same as the input, so for example in ziplist,
 * we use memcmp() to compare if a entry match the given input.That works well!. 
 */ 
void hsetnxCommand(redisClient *c){
    
    robj *o;
    if((o = hashTypeLookupWirteOrCreate(c, c->argv[1])) == NULL) return;

    hashTypeTryConversion(o, c->argv, 2, 3);

    if(hashTypeExists(o, c->argv[2])) {
        addReply(c, shared.czero);
        return;
    }
    //Encoding the field and value
    hashTypeTryObjectEncoding(o, &c->argv[2], &c->arv[3]);
    hashTypeSet(o, c->argv[2], c->argv[3]);
    addReply(c, shared.cone);

    //Send the signal 
    signalModifiedKey(c->db, c->argv[1]);

    //Notify the event
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hset", c->argv[1], c->db->id);

    //incr the server dirty
    server.dirty++;
}

void hmsetCommand(redisClient *c){
    
    int i;
    robj *o;

    if(c->argc % 2 != 0){
        addReplyError(c, "wrong number of arguments for HMSET");
        return;
    }

    if((o = hashTypeLookupWirteOrCreate(c, c->argv[1])) == NULL) return;

    //Check the inputs are they exceeds the limit of the node size
    hashTypeTryConversion(o, c->argv, 2, c->argc - 1);
    
    for(i = 2; i < c->argc; i += 2){
        hashTypeTryObjectEncoding(o, &c->argv[i], &c->argv[i+1]);
        hashTypeSet(o, c->argv[i], c->argv[i+1]);
    }

    //Send the reply to the client 
    addReply(c, shared.ok);

    //Send the key modified signal
    signalModifiedKey(c->db, c->argv[1]);

    //Send event notification
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hset", c->argv[1], c->db->id);

    //set the db as dirty
    server.dirty++;
}

void hincrbyCommand(redisClient *c){
    
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    if(getLongLongFromObjectOrReply(c, c->argv[3], &incr, NULL) == REDIS_ERR) return;

    if((o = hashTypeLookupWirteOrCreate(c, c->argv[1])) == NULL) return;

    //check if current field related value is existed.
    if((current = hashTypeGetObject(o, c->argv[2])) != NULL){
        //get the value 
        if(getLongLongFromObjectOrReply(c, current, &value, "hash value is not an integer") != REDIS_OK){
            decrRefCount(current);
            return;
        }
    }else{
        value = 0;
    }
    //Check if the incr operation will cause over-flow
    oldvalue = value;
    if((oldvalue < 0 && incr < 0 && incr < (LLONG_MIN - oldvalue)) ||
       (oldvalue > 0 && incr > 0 && incr > (LLONG_MAX - oldvalue))){
           addReplyError(c, "increment or decrement would overflow");
           return;
       }
    value = value + incr;
    //Create a object from the value
    new = createStringObjectFromLongLong(value);
    tryObjectEncoding(o, c->argv[2]);
    //Associate the new value with the key
    hashTypeSet(o, c->argv[2], new);
    decrRefCount(new);

    //Returns the result as reply
    addReplyLongLong(c, value);

    //Send signal
    signalModifiedKey(c->db, c->argv[1]);

    //Notify 
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hincrby", c->argv[1], c->db->id);

    server.dirty++;
}

void hincrbyfloatCommand(redisClient *c){
    long double incr, value;
    robj *o, *new, *old, *aux;

    if(getLongDoubleFromObjectOrReply(c, c->argv[3], &incr, NULL) != REDIS_OK) return;

    if((o = hashTypeLookupWirteOrCreate(c, c->argv[1])) == NULL) return;

    if((old = hashTypeGetObject(o, c->argv[2])) != NULL){
        //Try this value if it is a double
        if(getLongDoubleFromObjectOrReply(c, old, &value, "hash value is not an double") != REDIS_OK){
            decrRefCount(old);
            return;
        }
    }else{
        value = 0.0;
    }
    //do the add oepration
    value += incr;
    if(isnan(value) || isinf(value)){
        addReplyError(c, "increment would product Nan or Infinity");
		return;
    }
    new = createStringObjectFromLongDouble(value);

    tryObjectEncoding(o, c->argv[2]);
    //Associate the new value with the key
    hashTypeSet(o, c->argv[2], new);

    //Returns the result as reply
    addReplyBulk(c, new);

    //Send signal
    signalModifiedKey(c->db, c->argv[1]);

    //Notify 
    notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hincrbyfloat", c->argv[1], c->db->id);

    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 在传播 INCRBYFLOAT 命令时，总是用 SET 命令来替换 INCRBYFLOAT 命令
    // 从而防止因为不同的浮点精度和格式化造成 AOF 重启时的数据不一致
    aux = createStringObject("HSET", 4);
    rewriteClientCommandArgument(c, 0, aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c, 3, new);
    decrRefCount(new);
}

/**
 * Helper function: set the value object into the return.
 */ 
static void addHashFieldToReply(redisClient *c, robj *o, robj *field){

    //If the o is not existed.
    if(o == NULL){
        addReply(c, shared.nullbulk);
        return;
    }

    //ziplist
    if(o->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if(hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) < 0){
            addReply(c, shared.nullbulk);
            return;
        }else{
            if(vstr){
                addReplyBulkCBuffer(c, vstr, vlen);
            }else{
                addReplyBulkLongLong(c, vll);
            }
        }

    }else if(o->encoding == REDIS_ENCODING_HT){
        robj *value;

        //Get the value
        if(hashTypeGetFromHashTable(o, field, &value) < 0){
            addReply(c, shared.nullbulk);
        }else{
            addReplyBulk(c, value);
        }
    }else{
        redisPanic("Unknown hash encoding");
    }
}

void hgetCommand(redisClient *c){
    robj *o;

    if((o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL ||
       chechType(c, REDIS_HASH)) return;
    
    //Get and return the value
    addHashFieldToReply(c, o, c->argv[2]);
}

void hmgetCommand(redisClient *c){
    robj *o;
    int i;

    /**
     * Don't abort when the key cannot be found. Non-existing keys are empty hashes,
     * when HMGET should respond with a series of full bulks.
     */ 
    if((o = lookupKeyRead(c->db, c->argv[])) == NULL ||
        chechType(c, o, REDIS_HASH)) return;
    
    //Get mutiple field value
    addReplyMultiBulkLen(c. c->argc-2);
    for(i = 2; i < c->argc; i++){
        addHashFieldToReply(c, o, c->argv[i]);
    }
}

void hdelCommand(redisClient *c){
    robj *o;
    int j, deleted = 0, keyremove = 0;

    //Get the object
    if((o = lookupKeyWriteOrReply(c, c->argv[1], shared.czero)) == NULL ||
       checkType(c, o, REDIS_HASH)) return;

    for(j = 2; j < c->argc; j++){
        if(hashTypeDelete(o, c->argv[j]){

            //if we success remove a item, incr the variable
            deleted++;

            //If the hash type is empty, the we need to remove the key
            if(hashTypeLength(o) == 0){
                dbDelete(c->db, c->argv[1]);
                keyremove = 1;
                break;
            }
        }
    }

    //If at least on key is delete, then triggle the following code.
    if(deleted){
            //Send signal
            signalModifiedKey(c->db, c->argv[1]);
            
            //Send event notification
            notifyKeyspaceEvent(REDIS_NOTIFY_HASH, "hdel", c->argv[1], c->db->id);

            //Send event notification if the key is remove
            if(keyremove)
                notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
            
            //Send the database as dirty
            server.dirty += deleted;
    }
    addReplyLongLong(c, deleted);
}

void hlenCommand(redisClient *c){
    robj *o;

    //Get the object
    if((o = lookupKeyWriteOrReply(c, c->argv[1], shared.czero)) == NULL ||
       checkType(c, o, REDIS_HASH)) return;

    addReplyLongLong(c, hashTypeLength(o));
}

/**
 * Get the current cursor pointed item at the iterator
 */
static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what){

    //Here is a point we don't use the hashTypeCurrentObject, the hashTypeCurrent provide the 
    //similiar function, why? The reason may be that, when u look at the code of `hashTypeCurrent`
    //it get the current value from the iterator and encapsulate it into an object then return.
    //We have different function to reply to the client according to the input type, we need to
    //use different function to return to client but the `hashTypeCurrent` only returns object
    //This may be the reason.
    if(hi->encoding == REDIS_ENCODING_ZIPLIST){
        unsigned char *vstr = NULL;
        unsigned int len = UINT_MAX;
        long long vll = LLONG_MAX;
        
        hashTypeCurrentFromZiplist(hi, what, &vstr, &len, &vll);

        if(vstr){
            addReplyBulkCBuffer(c, vstr, vlen);
        }else{
            addReplyBulkLongLong(c, vll);
        }

    }else if(hi->encoding == REDIS_ENCODING_HT){
        robj *o = NULL;
        hashTypeCurrentFromHashTable(hi, what, &o);
        addReplyBulk(c, o);

    }else{
        redisPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(redisClient *c, int flags){
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0

    if((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk)) == NULL ||
        chechType(o, REDIS_HASH)) return;
    
    hi = hashTypeInitIterator(o);

    //Caculate how much item i need to get
    if(flags & REDIS_HASH_KEY) multiplier++;
    if(flags & REDIS_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;

    while(hashTypeNext(hi) != REDIS_ERR){
        if(flags & REDIS_HASH_KEY){
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
            count++
        }
        if(flag &REDIS_HASH_VALUE){
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
            count++
        }
    }

        // 释放迭代器
    hashTypeReleaseIterator(hi);
    redisAssert(count == length);
}

void hkeysCommand(redisClient *c){
    genericHgetallCommand(c, REDIS_HASH_KEY);
}

void hvalsCommand(redisClient *c){
    genericHgetallCommand(c, REDIS_HASH_VALUE);
}

void hgetallCommand(redisClient *c){
    genericHgetallCommand(c, REDIS_HASH_KEY | REDIS_HASH_VALUE);
}

void hexistsCommand(redisClient *c){
    robj *o;

    if((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL ||
        chechType(o, REDIS_HASH)) return;
    
    addReply(c, hashTypeExists(o, c->argv[2]) ? shared.cone : shared.czero);
}

void hscanCommand(redisClient *c){

}
