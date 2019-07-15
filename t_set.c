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

void spopCommand(redisCleint *c){
    robj *set, *ele, *aux;
    int64_t llele;
    int encoding;

    if((set = lookupKeyWriteOrReply(c->db, c->argv[1], shared.nullbulk)) == NULL ||
       checkType(c, set, REDIS_SET)) return;

    encoding = setTypeRandomElement(set, &ele, &llele);
    if(encoding == REDIS_ENCODING_INTSET){
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr, llele, NULL);
    }else{
        incrRefCount(ele);
        setTypeRemove(set, ele);
    }

    //Send event notification
    notifyKeyspaceEvent(REDIS_NOTIFY_SET, "spop", c->argv[1], c->db->id);

    /*Replicate this command as an SREM operation*/
    aux = createStringObject("SREM", 4);
    rewriteClientCommandVecotr(c, 3, aux, c->argv[1], ele);
    decrRefCount(ele);
    decrRefCount(aux);

    //Reply
    addReplyBulk(c, ele);

    //If this set is empty delete it from the database
    if(setTypeSize(set) == 0){
        dbDelete(c->db, c->argv[1]);

        notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
    }

    signalModifiedKey(c->db, c->argv[1]);

    server.dirty++;
}

/**
 * Handle the "SRABDNENVER" key count variant. The normal version of the command is handled by 
 * the srandmemberCommand() function itself.
 * 
 * How many times bigger should be the set compared to the requested size for us to don't
 * use the "remove element" strategy? Read later in the implementation for more info.
 */ 
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(redisClient *c){
    long l;
    unsigned long count, size;

    //In the default, we treat the return set doesn't contains duplicate element;
    int uniq = 1;

    rob *set, *ele;
    int64_t llele;
    int encoding;

    dict *d;

    if(getLongFromObjectOrReply(c, *l, c->argv[2]) != REDIS_OK) return;

    if(l >= 0){
        count = l;
    }else{
        uniq = 0;
        count = -l;
    }

    if((set = lookupKeyReadOrReply(c->db, c->argv[1], shared.nullbulk)) == NULL || 
       checkType(c, set, REDIS_SET)) return;

    //We deal this speical case, if the input count is zero, we return 
    if(count == 0){
        addReply(c, shared.emptymultibulk);
        return;
    }

    size = setTypeSize(set);

    /**
     * Case 1: the count is negative
     * If the count is zero then we do `count` times setTypeRandom.
     */ 
    if(!uniq){
        addReplyMultiBulkLen(c, count);

        while(count--){
            encoding = setTypeRandomElement(set, &ele, &llele);
            if(encoding == REDIS_ENCODING_INTSET){
                addReplyBulkLongLong(c, llele);
            }else if(encoding == REDIS_ENCODING_HT){
                addReplyBulk(c, ele);
            }
        }
        return;
    }

    /**
     * Case 2: the count is positive and greater than the size.
     * Return the whole set.
     */ 
    if(count >= size){
        //return the whole set
        return;
    }

    /**
     * Case 3 & 4 return part of the set, and the return sub-set need to be
     * unique, the different between 3 & 4 lies is pretty like search something
     * in a double-linked list we start from head or tail.
     */ 
    d = dictCreate(%setDictType, NULL);


    /**
     * Case 3: The number of elements inside the set is not greater than 
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * 
     * 这种情况就类似于，我们使用SRANDMEMBER_SUB_STRATEGY_MUL作为一个因子，我们操作的
     * count乘该因子如果大雨集合的大小时，表明此时要返回的元素多，那么拷贝原集合并且从中删除
     * 元素可能会快一点（类似于双向链表，我们的index>len/2，从尾部开始会快一点一个思维）
     */ 
    if(count * SRANDMEMBER_SUB_STRATEGY_MUL > size){
        setTypeInitIterator *si;
        int retval = REDIS_ERR;

        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si, &ele, &llele)) != -1){
            if(encoding == REDIS_ENCODING_INTSET){
                retval = dictAdd(d, createStringObjectFromLongLong(llele), NULL);        
            }else{
                retval = dictAdd(d, dupStringObject(ele), NULL);
            }

            redisAssert(retval == REDIS_OK);
        }

        setTypeReleaseIterator(si);
        redisAssert(dictSize(d) == size);

        while(size > count){
            dictEntry *de;

            //Get Random key then delete
            de = dictGetRandomKey(d);
            dictDelete(d, de->key);
            size--;
        }
    }else{
        unsigned long added = 0;

        while(added < count){
            encoding = setTypeRandomElement(set, &ele, &llele);
            if(encoding == REDIS_ENCODING_INTSET){
                ele = createStringObjectFromLongLong(llele);
            }else{
                ele = dupStringObject(ele);
            }

            if(dictAdd(d, ele, NULL) == DICT_OK)
                added++;
            else
                decrRefCount(ele);
        }
    }

    /**
     * The common part of the case 3 and case4
     */
    dictIterator *di;
    dictEntry *de;
    di = dictGetIterator(d);

    addReplyMultiBulkLen(c, count);
    while((de = dictNext(d)) != NULL){
        addReplyBulk(c, dictGetKey(de));
    }
    dictReleaseIterator(di);
    dictRelease(d);
}

void srandmemberCommand(redisClient *c){
    robj *set, *ele;
    int64_t llele;
    int encoding;

    if(c->argc == 3){
        srandmemberWithCountCommand(c);
        return;
    }
    if(c->argc > 3){
        addReply(c, shared.syntaxerr);
        return;    
    }

    if((set = lookupKeyReadOrReply(c->db, c->argv[1], shared.nullbulk)) == NULL ||
       checkType(c, set, REDIS_SET)) return;

    encoding = setTypeRandomElement(set, &ele, &llele);

    if(encoding == REDIS_ENCODING_INTSET){
        addReplyBulkLongLong(c, llele);
    }else {
        addReplyBulk(c, ele);
    }
}

/**
 * Caculate the difference between radix of set s1 and radix of set s2 
 */ 
int qsortCompareSetsByCardinality(const void *s1, const void *s2){
    return setTypeSize(*(robj**)s1) - setTypeSize(*(robj**)s2);
}

/**This is used by SDIFF and in this case we can receive NULL that should
 * be handle as empty set.
 */ 
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2){
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;

    return (o2 ? setTypeSize(o2) : 0) - (o1 ? sizeType(o1) : 0);
}

/**
 * This part is a generic funtion for set intset operation.
 * If the dstkey is not empty, then we need to save the inet set element into the dstKey
 * respect set.
 * The operation is pretty simple, we sort this input sets, use the first one as a privot
 * loop through all the elements in this set and to check if each element is existed.
 */ 
void sinterGenericCommand(redisClient *c, robj **setKeys, unsigned long setnum, robj *dstkey){
    
    //Set array
    robj **sets = zmalloc(sizeof(robj *) * setnum);

    setTypeIterator *si;
    robj *eleobj, *dstset = NULL;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    //Check all the elements in the setKeys if they are existed && objectType is REDIS_SET
    for(j = 0; j < setnum; j++){
        robj *setobj = dstkey ?
                       lookupKeyWrite(c->db, setkey[j]) : 
                       lookupKeyRead(c->db, setKey[j]);

        //If any set is not existed, then this intset operation is give up.
        if(!setobj){
            zfree(sets);
            if(dstkey){ 
                if(dbDelete(c->db, dstkey)){
                    signalModifiedKey(c->db, key);
                    addReply(c, shared.czero);
                }
            }else{
                addReply(c, shared.emptymultibulk);
            }
            return;
        }

        //Check current obj type 
        if(checkType(c, setobj, REDIS_SET)) {
            zfree(sets);
            return;
        }
        setobj[j] = setobj;
    }

    /**
     * Sort sets from the smallest to largest, this will improve our 
     * alogrithm's performance.
     */ 
    qsort(sets, setnum, sizeof(robj*), qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    // 因为不知道结果集会有多少个元素，所有没有办法直接设置回复的数量
    // 这里使用了一个小技巧，直接使用一个 BUFF 列表，
    // 然后将之后的回复都添加到列表中
    if (!dstkey) {
        replylen = addDeferredMultiBulkLength(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createIntsetObject();
    }

    /**
     * We use the first element of this set as a privot, choose each
     * element of it then check if this element in other sets, it
     * this element exist in all the other sets, then we find a inset.
     */ 
    si = setTypeInitIterator(sets[0]);

    while((encoding = setTypeNext(si, &eleobj, &intobj)) != -1){
        for(j = 1; j < setnum; j++){

            if(s[j] == s[0]) continue;

            if(encoding == REDIS_ENCODING_INTSET){
                if(s[j]->encoding == REDIS_ENCODING_INTSET && !intsetFind(s[j], intobj)) break;

                else if(s[j]->encoding == REDIS_ENCOIDNG_HT){
                    eleobj = createStringObjectFromLongLong(intobj);
                    if(!setTypeIsMember(s[j], eleobj)){
                        decrRefCount(eleobj);
                        break;
                    }
                    decrRefCount(eleobj);
                }
            }else if(encoding == REDIS_ENCODING_HT){
                if(s[j]->encoding == REDIS_ENCDOING_INTSET && 
                   eleobj->encoding == REDIS_ENCDOING_INT && 
                   !intsetFind(s[j], (long)eleobj->ptr)){
                       break;
                }else if(s[j]->encoding == REDIS_ENCODING_HT){
                    if(!setTypeIsMember(s[j], ele)) break;
                }
            }
        }
            //This means the element exist in all the otherset
        if(j == setnum){
            //SINTER command
            if(!dstkey){
                if(encoding == REDIS_ENCODING_HT)
                    addReplyBulk(c, eleobj);
                else
                    addReplyBulkLongLong(c, intobj);
                cardinality++;
            }else{
                if(encoding == REDIS_ENCODING_INTSET){
                    eleobj = createStringObjectFromLongLong(intobj);
                    setTypeAdd(dstset, eleobj);
                    decrRefCount(eleobj);
                }else{
                    setTypeAdd(dstset, eleobj);
                }
            }
        }
    }

    setTypeReleaseIterator(si);

    //SETINTERSTORE Command
    if(dstkey){
        /**Store the resulting set into the target, if the intersection is not 
         * an emptylist.
         */ 
        int deleted = dbDelete(c->db, dstkey);

        if(setTypeSize(dstset) > 0){
            dbAdd(c->db, dstkey, dstset);
            addReplyLongLong(c, setTypeSize(dstset));
            notifyKeyspaceEvent(REDIS_NOTIFY_SET, "sinterstore", dstkey, c->db->id);
        }else{
            decrRefCount(dstset);
            addReply(c, shared.czero);
            if(deleted){
                notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",
                dstkey,c->db->id);
            }
            signalModifiedKey(c->db, dstkey);

            server.dirty++;
        }
    }else{
        setDeferredMultiBulkLength(c,replylen,cardinality);
    }
    zfree(sets);
}

void sinterCommand(redisClient *c){
    sinterGenericCommand(c, c->argv+1, c->argc - 1, NULL);
}

void sinterstoreCommand(redisClient *c){
    sinterGenericCommand(c, c->argv+1, c->argc-2, c->argv[1]);
}

//Command Type
#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

void sunionDiffGenericCommand(redisClient *c,robj **setkeys, int setnum, robj *dstkey, int op){

    robj **sets = zmalloc(sizeof(robj*) * setnum);

    setTypeIterator *si;
    robj *ele, *dstset = NULL;
    robj *setobj = NULL;
    int j, cardinality = 0;
    int diff_algo = 1;

    //Check all the input set and put then into the sets object.
    for(j = 0; j < setnum; j++){
        setobj = dstkey ?
                 lookupKeyWrite(c, setkeys[j]) :
                 lookupKeyRead(c, setkeys[j]);
        if(setobj == NULL) {
            sets[j] = NULL;
            continue;
        }

        if(checkType(c, setobj, REDIS_SET)) {
            zfree(sets);
            return;
        }

        sets[j] = setobj;
    }

    /**
     * Select which alogrithm to use for the diff operation.
     * Two alogrithm is avaiable:
     * 1.Use the first sets as a privot, pick every item inside then
     *  check if this item is exist in all the other sets.
     * 2.loop from the second sets, check if the element exists in the 
     *   first set, if exist delete it from the frist sets.
     * 
     * There need a way to figure out when we need to choose 1 when we
     * choose 2.
     */ 
    if(op == REDIS_OP_DIFF && set[0]){
        long long algo_one_work = 0, alog_two_work = 0;

        for(j = 0; j < setnum; j++){
            if(!set[j]) continue;

            algo_one_work += setTypeSize(set[0]);
            alog_two_work += setTypeSize(set[j]);
        }

        /**Alogrithm 1 has better constant time and perform less operation.
         * If there are element in common.
         */ 
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= alog_two_work) ? 1 : 2;
        /* With algorithm 1 it is better to order the sets to subtract
         * by decreasing size, so that we are more likely to find
         * duplicated elements ASAP. */
        // 如果使用的是算法 1 ，那么最好对 sets[0] 以外的其他集合进行排序
        // 这样有助于优化算法的性能
        if(diff_algo == 1 && setnum > 1){
            qsort(sets+1, setnum - 1, sizeof(robj*), qsortCompareSetsByRevCardinality);
        }
    }

    /**
     * We need a temo object set to store out union. If the dstKey is not NULL,
     * then the set will be used to save the result object respect to the key.
     */ 
    dstset = createIntsetObject();

    if(op == REDIS_OP_UNION){
        for(j = 0; j < setnum; j++){
            if(sets[j] == NULL) continue;

            si = setTypeInitIterator(set[j]);
            while((ele = setTypeNextObject(si) != NULL){
                if(setTypeAdd(dstset, ele)) cardinality++;
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);
        }
    }else if(op == REDIS_OP_DIFF && set[0] && diff_algo == 1){
        /* We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         */ 

        /* This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. 
         */ 
        si = setTypeInitIterator(si[0]);
        while((ele = setTypeNextObject(si)) != NULL){

            for(j = 1; j < setnum; j++){
                if(!set[j]) continue;
                if(set[j] == set[0]) continue;
                if(setTypeIsMember(sets[j], ele)) break;
            }

            if(j == setnum){
                setTypeAdd(dstset, ele);
                cardinality++;
            }
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
    }else if(op == REDIS_OP_DIFF && set[0] && diff_algo == 2){
        /* DIFF Algorithm 2:
         *
         * 差集算法 2 ：
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * 将 sets[0] 的所有元素都添加到结果集中，
         * 然后遍历其他所有集合，将相同的元素从结果集中删除。
         *
         * This is O(N) where N is the sum of all the elements in every set. 
         *
         * 算法复杂度为 O(N) ，N 为所有集合的基数之和。
         */
        for(j = 0; j < setnum; j++){
            if(!set[j]) continue;

            si = setTypeInitIterator(s[j]);
            while((ele = setTypeNext(si)) != NULL){
                if(j == 0){
                    if(setTypeAdd(dstset, ele)) cardinality++;
                }else{
                    if(setTypeRemove(dstset, ele)) cardinality--;
                }
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);

            if(cardinality == 0) break;
        }
    }

    if(!dstkey){
        addReplyMultiBulkLen(c, cardinality);

        //Loop all the elements in the set
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL){
            addReplyBulk(c, ele);
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(dstset);
    }else{
        int deleted = dbDelete(c->db, dstkey);

        //If the result set is not empty, that means we need
        //to store the result into the dstkey respect value.
        if(setType(dstset) > 0){
            dbAdd(c->db, dstkey, dstset);
            addReplyLongLong(c, setTypeSize(dstset));
            notifyKeyspaceEvent(REDIS_NOTIFY_SET,
                op == REDIS_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
        }else{
            decrRefCount(dstset);
            addReply(c, shared.czero);
            if(deleted) notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", dstkey, c->db->id);
        }
        signalModifiedKey(c->db, dstkey);
        server.dirty++;
    }
    zfree(sets);
}
void sunionCommand(redisClient *c){
    sunionDiffGenericCommand(c, c->argv+1, c->argc - 1, NULL, REDIS_OP_UNION);
}

void sunionstoreCommand(redisClient *c){
    sunionDiffGenericCommand(c, c->argv+2, c->argc - 2, c->argv[1], REDIS_OP_UNION);
}

void sdiffstoreCommand(redisClient *c){
    sunionDiffGenericCommand(c, c->argv+1, c->argc - 1, NULL, REDIS_OP_DIFF);
}

void sdiffstoreCommand(redisClient *c){
    sunionDiffGenericCommand(c, c->argv+2, c->argc - 2, c->argv[1], REDIS_OP_DIFF);
}

void sscanCommand(redisClient *c){
    robj *set;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,REDIS_SET)) return;
    scanGenericCommand(c,set,cursor);
}
















