#include "redis.h"
#include "cluster.h"

#include <signal.h>
#include <ctype.h>

void slotToKeyAdd(robj *key);
void slotToKeyDel(robj *key);
void slotToKeyFlush(void);

/************************************************************************
 *         C-level DB API                                               *
 ************************************************************************/

/* Get the value from database by key,
 * if existed returns the object, otherwise returns NULL.
 */
robj *lookupKey(redisDb *db, robj *key){

    dictEntry *de = dictFind(db->dict, key->ptr);
    
    //If this element is existed.
    if(de){
        robj *val = dictGetVal(de);

        if(server.rdb_child_pid == -1 && server.aof_child_pid == -1)
            val->lru = LRU_CLOCK();
        
        return val;
    }
    return NULL;
}

/* In order to execute the read operation, to check if a key
 * is existed in the database, and update the hit/miss hit message.
 * if existed return the object, otherwise returns NULL.
 */
robj *lookupKeyRead(redisDb *db, robj *key){
    
    robj *val;

    //Check if this key has expired
    expireIfNeeded(db, key);

    val = lookupKey(db, key);

    if(val == NULL)
        server.stat_keyspace_misses++;
    else
        server.stat_keyspace_hits++;

    return val;
}

/**
 * This is similar function like lookupKeyRead, but with little
 * difference, this function will not update the info of hit or miss
 * hit.
 */
robj *lookupKeyWirte(redisDb *db, robj *key){
    
    expireIfNeeded(db, key);

    return lookupKey(db, key);
}

/* This only enhance the function of lookupKeyRead.
 * if key existed, return the key corresponding value object.
 * otherwise it will return NULL, and send the reply message.
 */
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply){

    robj *o = lookupKeyRead(c->db, key);

    if(!o) addReply(c, reply);

    return o;
}

robj *lookupKeyWriteOrReply(redisClient *c, robj *key, robj *reply){

    robj *o = lookupKeyWirte(c->db, key);

    if(!o) addReply(c, reply);

    return o;
}

/* Add the key to the DB. It is up to the caller to increment the reference
 * counter of the value if needed.
 */
void dbAdd(redisDb *db, robj *key, robj *val){

    //Make a copy of the input key parameter
    sds copy = sdsdup(key->ptr);

    int retval = dictAdd(db->dict, copy, val);

    redisAssertWithInfo(NULL, key, retval == REDIS_OK);

    //If the server enable cluster mode, save the value into the slot.
    if(server.cluster_enable) slotToKeyAdd(key);

}

/* OverWrite an existing key with a new value. Incrementing the reference
 * count of the new vlaue is up to the caller.
 * 
 * This function does not modify the expire time of the expire key.
 * 
 * The program is aborted if the key was not already present.
 */
void dbOverWrite(redisDb *db, robj *key, robj *val){
    dictEntry *de = dictFind(db->dict, key->ptr);

    //The node must exist.
    redisAssertWithInfo(NULL, key, de != NULL);

    dictReplace(db->dict, key, val);
}

/* High level set operation. This function is used to set 
 * a key, whatever it was existing or not, to a new object.
 * 
 * 1) The ref count of the value object is incremented.
 * 
 * 2) Clients WATCHing for the destination key notified.
 * 
 * 3) The expire time of the key is reset (the key will be persistent)
 */
void setKey(redisDb * db, robj *key, robj *val){

    //Add or overwrite the key-value pair in database.
    if(lookupKeyWirte(db, key) == NULL){
        dbAdd(db, key, val);
    }else{
        dbOverWrite(db, key, val);
    }

    incrRefCount(val);

    removeExpire(db, key);

    signalModifiedKey(db, key);
}

int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis Object
 * If there are no key, NULL is returned.
 * 
 * The function makes sure to return keys not already expired.
 */
robj *dbRandomKey(redisDb *db){
    dictEntry *de;

    while(1){
        sds key;
        robj *keyobj;

        de = dictGetRandomKey(db->dict);

        //If the databse is empty then return NULL
        if(de == NULL) return NULL;

        key = dictGetKey(de);
        //Create a string object
        keyobj = createStringObject(key, sdslen(key));

        //Check if this key associate with a expire time, then check if it is 
        //expired.
        if(dictFind(db->expires, key)){
            //If this key is expired,then deleted it
            if(expireIfNeeded(db, keyobj)){
                decrRefCount(keyobj);
                continue;
            }
        }
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB 
 */
int dbDelete(redisDb *db, robj *key){

    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if(dictSize(db->expires) > 0) dictDelete(db->expires, key->ptr);

    //delete key-value pair
    if(dictDelete(db->dict, key->ptr) == DICT_OK){
        if(server.cluster_enabled) slotToKeyDel(key);
    }else{
        return 0;
    }
}

/* Prepare the string object stored at 'key' to be modified destructively
 * to implement commands like SETBIT or APPEND.
 *
 * An object is usually ready to be modified unless one of the two conditions
 * are true:
 *
 * 1) The object 'o' is shared (refcount > 1), we don't want to affect
 *    other users.
 * 2) The object encoding is not "RAW".
 *
 * If the object is found in one of the above conditions (or both) by the
 * function, an unshared / not-encoded copy of the string object is stored
 * at 'key' in the specified 'db'. Otherwise the object 'o' itself is
 * returned.
 *
 * USAGE:
 *
 * The object 'o' is what the caller already obtained by looking up 'key'
 * in 'db', the usage pattern looks like this:
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,REDIS_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * At this point the caller is ready to modify the object, for example
 * using an sdscat() call to append some data, or anything else.
 */
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o){
    redisAssert(o->type == REDIS_STRING);
    if(o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW){
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        /* 这个地方有个很微妙的细节，有一个场景，一个字符串在多个地方被共享，那么我们
         * 现在要修改这个字符串的时候，肯定会用到这个函数。 举个例子， A B 两个
         * key共同使用到了一个字符串 "123" 那么进入这个函数时，首先进入getDecodedObject
         * 函数，这个函数中，发现他是字符串，随后将其引用计数+1（此时引用计数为3），接下来创建一个全新的字符串，
         * 随后将引用计数-1（回到2），紧接着执行dbOverWrite函数。那么从直觉来讲，这个地方我们应该要把“123”
         * 的引用再-1，因为我们已经创建了一个和他一样的一个拷贝（不再使用共享的方式），但是这里似乎没看见这个
         * 操作，这个操作在哪里进行的呢？ 答案是dbOverWrite里面有一个dictReplace函数，他会设置新value，
         * 释放旧的value，释放的操作dictFreeVal，由于db这个dictionary的valDestructor 是一个 decrRefCount
         * 那就是说引用次数的-1 在这里完成的。
         */
        dbOverWrite(db, key, o);
    }
    return o;
}

/* 
 *Empty the database data
 */
long long emptyDb(void(callback)(void *)){
    int j;
    long long removed = 0;

    //Empty the whole databse
    for(j = 0; server.dbnum; j++){
        
        //record the removed number
        removed += dictSize(server.db[j].dict);

        //Remove the whole key-value pairs
        dictEmpty(server.db[j].dict, callback);
        dictEmpty(server.db[j].dict, callback);
    }

    //If this open the cluster mode, we still need to remove slot recode.
    if(server.cluster_enabled) slotToKeyFlush();

    return removed;
}

//Change the destination database
int selectDb(redisClient *c, int id){

    //input validation
    if(id < 0 || id >= server.dbnum)
        return REDIS_ERR;
    
    c->db = &server.db[id];

    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * 键空间改动的钩子。
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * 每当数据库中的键被改动时， signalModifiedKey() 函数都会被调用。
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *
 * 每当一个数据库被清空时， signalFlushDb() 都会被调用。
 *----------------------------------------------------------------------------*/
void signalModifiedKey(redisDb *db, robj *key){
    touchWatchedKey(db, key);
}

void signalFlushedDb(int dbid){
    touchWatchedKeysOnFlush(dbid);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *
 * 与类型无关的数据库操作。
 *----------------------------------------------------------------------------*/

/* clean the database that the client specific
 */
void flushdbCommand(redisClient *c){

    server.dirty += dictSize(c->db->dict);

    //Send the notification
    signalFlushedDb(c->db->id);

    //empty the dict and expire
    dictEmpty(c->db->dict, NULL);
    dictEmpty(c->db->expires, NULL);

    //If open the cluster mode, remove the slot recored
    if(server.cluster_enabled) slotToKeyFlush();

    addReply(c, shared.ok);
}

/* Clean all the database in redisServer
 */
void flushallCommand(redisClient *c){

    //Send info 
    signalFlushedDb(-1);

    //clean all the database
    server.dirty += emptyDb(NULL);
    addReply(c, shared.ok);

    //If we are save the new RDB, then cancel.
    if(server.rdb_child_pid != -1){
        kill(server.rdb_child_pid, SIGUSR1);
        rdbRemoveTempFile(server.rdb_child_pid);
    }

    //update the rdb file
    if(server.saveparamslen > 0){
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        // rdbSave() 会清空服务器的 dirty 属性
        // 但为了确保 FLUSHALL 命令会被正常传播，
        // 程序需要保存并在 rdbSave() 调用之后还原服务器的 dirty 属性
        int saved_dirty = server.dirty;

        rdbSave(server.rbd_filename);

        server.dirty = saved_dirty;
    }

    server.dirty++;
}

void delCommand(redisClient *c){
    int deleted = 0, j;

    for(j = 1; j < c->argc; j++){
    
        expireIfNeeded(c->db, c->argv[j]);

        if(dbDelete(c->db, c->argv[j]){
            
            //delete success then notify
            signalModifiedKey(c->db, c->argv[j]);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            deleted++;
            server.dirty++;
        }
    }
    addReplyLongLong(c, deleted);
}

void existsCommand(redisClient *c){

    expireIfNeeded(c->db, c->argv[1]);

    if(dbExists(c->db, c->argv[1])){
        addReply(c, shared.cone);
    }else{
        addReply(c, shared.czero);
    }
}

void selectCommand(redisClient *c){
    long id;

    if(getLongFromObjectOrReply(c, c->argv[1], &id,
      "invaild DB index") != REDIS_OK)
      return;

    if(server.cluster_enabled && id != 0){
        addReplyError(c, "SELECT is not allowed in cluster mode");
        return;
    }

    //switch the database
    if(selectDb(c, id) == REDIS_ERR){
        addReplyError(c, "invaild DB index");
    }else{
        addReply(c, shared.ok);
    }
}

void randomkeyCommand(redisClient *c){
    robj *key;

    if((key = dbRandomKey(c->db)) != NULL){
        addReplyBulk(c, key);
        decrRefCount(key);
        return;
    }
    addReply(c, shared.nullbulk);
}

