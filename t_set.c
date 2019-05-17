#include "redis.h"
/**-------------------------------------------------------------
 * Set Commands 
 **------------------------------------------------------------*/

void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op);

/** Factory method to return a set that cant hold `value`.
 *  When the object has an integer-encodable value, an intset 
 *  will be returned.Otherwise a regular hash tble.
 */ 
robj *setTypeCreate(robj *value){
    long long vll;
    if(isObjectRepresentableAsLongLong(value, &vll) == REDIS_OK){
        return createIntsetObject();
    }else{
        return createSetObject();
    }
}

/**
 * Set add operation.
 * If add success return 1, otherwise(the element is already existed) 
 * return 0.
 */ 
int setTypeAdd(robj *subject, robj *value){
    long long llval;

    if(subject->encoding == REDIS_ENCODING_INTSET){
        //If it can be represent as a long long.
        if(isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK){
            uint8_t success = 0;
            subject->ptr =  intsetAdd(subject->ptr, llval, &success);
            if(success){
                /**This means the add is success and we need to check if
                 * current total size of the set exceeds the configured 
                 * maxmum size of set.
                 */ 
                if(intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject, REDIS_HASH);
                return 1;
            }
        }else{
            //Otherwise we can only treat it as a sds string.
            setTypeConvert(subject, REDIS_HASH);
            /**
             * The set was original intset and the input value can't be represetn as 
             * a long long type, so in normal, this input value never exist in previous
             * set and the dictAdd always be true.
             */ 
            redisAssertWithInfo(NULL, value, dictAdd(subject->ptr, value, NULL) == DICT_OK);
            incrRefCount(value);
            return 1;
        }
    }else if(subject->encoding == REDIS_ENCODING_HT){
        if(dictAdd(subject->ptr, value, NULL) == DICT_OK){
            incrRefCount(value);
            return 1;
        }
    }else{
        redisPainc("Unknown type");
    }
    return 0;
}

/**
 * Remove operation
 * If remove success return 1, if the element is not existed then return 0;
 */
int setTypeRemove(robj *setobj, robj *value){
    long long llval;
    
    if(setobj->encoding == REDIS_ENCODING_INTSET){
        if(isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK){
            int success;
            setobj->ptr = intsetRemove(setobj->ptr, llval, &success);
            if(success) return 1;
        }
    }else if(setobj->encoding == REDIS_ENCODING_HT){
          if(dictDelete(setobj->ptr, value) == REDIS_OK){
              //Check if we need to resize the dictionary if necessary
              if(htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
              return 1;
          }
    }else{
        redisPainc("Unknown type");
    }
    return 0;
}

/** Is member operation
 * If it is existed in the set return 1,
 * otherwise returns 0.
 */ 
int setTypeIsMember(robj *subject, robj *value){
    long long llval;
    
    if(subject->encoding == REDIS_ENCODING_HT){
        return dictFind(subject->ptr, value) != NULL;
    }else if(subject->encoding == REDIS_ENCODING_INTSET){
        if(isObjectRepresentableAsLongLong(value, &llval) == REDIS_OK){
            return intsetFind(subject->ptr, llval);
        }
    }else{
         redisPainc("Unknown type");
    }
    return 0;
}

/**
 * Create and return a set iterator.
 */ 
setTypeIterator *setTypeInitIterator(robj *subject){
    
    setTypeIterator *it = zmalloc(sizeof(setTypeIterator));
    it->subject = subject;
    it->encoding = subject->encoding;

    if(subject->encoding == REDIS_ENCODING_HT){
        it->di = dictGetIterator(subject->ptr);
    }else if(subject->encoding == REDIS_ENCODING_INTSET){
        it->ii = 0;
    }else{
         redisPainc("Unknown type");
    }
    return it;
}

/**
 * Free the iterator
 */ 
void setTypeReleaseIterator(setTypeIterator *si){
    if(si->encoding == REDIS_ENCODING_HT){
        dictReleaseIterator(si->di);
    }
    zfree(si);
}

/**
 * Move to the next entry in the set.Returns the object at the current position.
 * 
 * Since set elements can be internally be stored as redis objects or simple arrays
 * of integers, setTypeNext returns the encoding of the set object you are iteratring,
 * and will polpulate the appropriate pointer(eobj) or (llobj) accordingly.
 * 
 * When there are no longer elements -1 is returned, Otherwise return the current
 * iterate object encoding.
 * 
 * The return value is not incr the object reference count.
 * Returned objects ref count is not incremented, so this function is copy on write friendly.
 */ 
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele){

    if(si->encoding == REDIS_ENCODING_INTSET){
        if(!intsetGet(si->subject->ptr, si->ii++, llele)) return -1;
    }else if(si->encoding == REDIS_ENCODING_HT){
        dictEntry *de =  dictNext(si->di);
        if(de == NULL) return -1;

        *objele = dictGetKey(de);
    }else{
         redisPainc("Unknown type");
    }
    return si->encoding;
}

/**
 * The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new objects
 * or incrementing the ref count of returned objects. So if you don't
 * retain a pointer to this object you should call decrRefCount() against it.
 * 
 * setTypeNext non copy-on-write version.
 * Always return a new or already-incr reference object.
 * 
 * After the caller use this object, they should call decrRefCount().
 * 
 * This function is the way to go for write operations where COW is not
 * an issue as the reult will be anyway of incremnting the ref count.
 */ 
robj *setTypeNextObject(setTypeIterator *si){
    robj *objele;
    int64_t intele;
    int encoding;

    encoding = setTypeNext(si, &objele, &intele);
    if(encoding == REDIS_ENCODING_HT){
        incrRefCount(objele);
        return objele;
    }else if(encoding == REDIS_ENCODING_INTSET){
        return createStringObjectFromLongLong(intele);
    }else if(encoding == -1){
        return NULL;
    }else{
         redisPainc("Unknown type");
    }
    return NULL;
}

/**
 * Return random element from a non empty set.
 * 
 * The returned element can be a int64_t value if the set is encoded as 
 * an `intset`, or a redis object if the set is a regular set.
 * 
 * The caller provides both pointers to be populated with the right object.
 * The return value of the function is the object->encoding field of the object
 * and is used by the caller to check if the int64_t pointer or the redis object
 * was populated.
 * 
 * When an object is returned the ref count of the object is not incremented so this 
 * function can be considered copy on write friendly. 
 */ 
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele){

    if(setobj->encoding == REDIS_ENCODING_INTSET){
        *llele = intsetRandom(setobj->ptr);
    }else if(setobj->encoding == REDIS_ENCODING_HT){
        dictEntry *de = dictGetRandomKey(setobj->ptr);
        *objele = de->key;
    }else{
         redisPainc("Unknown type");
    }
    return setobj->encoding;
}

/**
 * Set size function
 */ 
unsigned long setTypeSize(robj *subject){
    if(subject->encoding == REDIS_ENCODING_INTSET){
        return intsetLen(subject->ptr);
    }else if(subject->encoding == REDIS_ENCODING_HT){
        return dictSize((dict *)subject->ptr);
    }else{
         redisPainc("Unknown type");
    }
}

/**
 * Convert the set to specific encoding.
 * 
 * The resulting dict (when converting to a hash table)
 * is presized to hold the number of elements in the origninal set.
 */
void setTypeConvert(robj *setobj, int enc){

    setTypeIterator *si;

    // 确认类型和编码正确
    redisAssertWithInfo(NULL,setobj,setobj->type == REDIS_SET &&
                             setobj->encoding == REDIS_ENCODING_INTSET);

    si = setTypeInitIterator(setobj);
    if(enc == REDIS_ENCODING_HT){
        robj *o;
        dict *d = dictCreate(&setDictType, NULL);
        //This means to expand the space able to hold the intset.
        dictExpand(d, intsetLen(setobj->ptr));

        while((o = setTypeNextObject(si)) != NULL){
            redisAssertWithInfo(NULL,o,dictAdd(d,o,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        setobj->encoding = REDIS_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    }else{
        redisPainc("Unknown type");
    }
}

/*------------------------------------------------------------------
 * Set Command
 *-----------------------------------------------------------------*/ 

void saddCommand(redisClient *c){
    robj *set;
    int j, added = 0;
    set = lookupKeyWrite(c->db, c->argv[1]);

    if(set == NULL){
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db, c->argv[1, set]);
    }else{
        if(checkType(c, set, REDIS_SET)) return;
    }

    for(j = 2; j < c->argc; j++){
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if(setTypeAdd(set,c->argv[j])) added++;
    }

    /**
     * If we added something, then we need to notify the changeds
     */ 
    if(added){
        //Send the key modified signal
        signalModifiedKey(c->db, c->argv[1]);
        //Send the even notification
        notifyKeyspaceEvent(REDIS_NOTIFY_SET, "sadd", c->argv[1], c->db->id);
    }

    server.dirty += added;

    addReplyLongLong(c, added);
}

void sremCommand(redisClient *c){
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    if((set = lookupKeyWriteOrReply(c->db, c->argv[1], shared.czero) == NULL) ||
      checkType(c, set, REDIS_SET)) return;

    for(j = 2; j < c->argc; j++){
        if(setTypeRemove(set, c->argv[j])){
            deleted++;
            if(setTypeSize(set) == 0){
                keyremoved++;
                dbDelete(c->db, c->argv[1]);
                break;
            }
        }
    }

    if(deleted){
        //Send the key modified signal
        signalModifiedKey(c->db, c->argv[1]);
        //Send the even notification
        notifyKeyspaceEvent(REDIS_NOTIFY_SET, "srem", c->argv[1], c->db->id);
        //If the key is delete notify that
        if(keyremoved){
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        }
        server.dirty += deleted;
    }
    addReplyLongLong(c, deleted);
}

void smoveCommand(redisClient *c){
    robj *sset, *dset, *ele;

    //Get the source set object
    if((sset = lookupKeyWriteOrReply(c->db, c->argv[1], shared.czero)) == NULL || 
       !checkType(c, sset, REDIS_SET)) return;
    
    dset = lookupKeyWrite(c->db, c->argv[2]);
    if(dset && checkType(c, dset, REDIS_SET)) return;

    /**
     * Check if the src set and dst set is the same.
     */ 
    if(sset == dset){
        addReply(c, shared.czero);
        return;
    }

    ele = tryObjectEncoding(c->argv[3]);
    if(!setTypeRemove(sset, ele)){
        //If remove not success
        addReply(c, shared.czero);
        return;
    }

    //Send event notification
    notifyKeyspaceEvent(REDIS_NOTIFY_SET, "srem", c->argv[1], c->db->id);

    /*After remove the src from the src set, check if we need to delete the key*/
    if(setTypeSize(sset) == 0){
        dbDelete(c->db, c->argv[1]);
        //Send event notification
        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }

    //Send the key signal 
    signalModifiedKey(c->db, c->argv[1]);
    signalModifiedKey(c->db, c->argv[2]);

    //set the datbase as dirty
    server.dirty++;

    if(dset == NULL){
        dset = setTypeCreate(ele);
        dbAdd(c->db, c->argv[2],dset);
    }

    /**
     * Now we add this key into the dst set, if success that will triggle the server.dirty
     * incr and incr notification.
     */  
    if(setTypeAdd(dset, ele)){
        server.dirty++;
        notifyKeyspaceEvent(REDIS_NOTIFY_SET, "sadd", c->argv[2], c->db->id);
    }

    addReply(c, shared.cone);
}

void sismemberCommand(redisClient *c){
    robj *set;
    if((set = lookupKeyReadOrReply(c->db, c->argv[1], shared.cone)) == NULL ||
       checkType(c, set, REDIS_SET)) return;

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    addReply(c, setTypeIsMember(set, c->argv[2]) ? shared.cone : shared.czero);
}

void scardCommand(redisClient *c){
    robj *set;
    if((set = lookupKeyReadOrReply(c->db, c->argv[1], shared.cone)) == NULL ||
       checkType(c, set, REDIS_SET)) return;

    addReplyLongLong(c, setTypeSize(set));
}
























