#ifndef __REDIS_H
#define __REDIS_H

#include "fmacros.h"
#include "config.h"

#if defined(__sun)
#include "solarisfixes.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <signal.h>

#include "ae.h"
#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "zmalloc.h"
#include "anet.h"
#include "ziplist.h"
#include "intset.h"
#include "version.h"
#include "util.h"

/*Error code*/
#define REDIS_OK  0
#define REDIS_ERR -1

#define ZSKIPLIST_MAXLEVEL	32
#define ZSKIPLIST_P	0.25

/*Object type
*/
#define REDIS_STRING 0
#define REDIS_LIST	1
#define REDIS_SET	2
#define REDIS_ZSET	3
#define REDIS_HASH	4

/**
 * Object encoding.The object in redis even for the same type, they 
 * can have different underlying impletation. 
 */
#define REDIS_ENCODING_RAW 0
#define REDIS_ENCDOING_INT 1
#define REDIS_ENCODING_HT  2
#define REDIS_ENCODING_ZIPMAP 3
#define REDIS_ENCODING_LINKEDLIST 4
#define REDIS_ENCODING_ZIPLIST 5
#define REDIS_ENCODING_INTSET 6
#define REDIS_ENCODING_SKIPLIST 7
#define REDIS_ENCODING_EMBSTR 8

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
// 记录解除了客户端的阻塞状态的键，以及键所在的数据库。
typedef struct readyList{
	redisDb *db;
	robj *key;
}readyList;

/*--------------------------------------------------------------
 *			Data Structure
 * -----------------------------------------------------------*/

typedef long long mstime_t; /*millisecond time type*/

#define REDIS_LRU_BITS 24
#define REDIS_LRU_CLOCK_MAX ((a<<REDIS_LRU_BITS) - 1)
#define REDIS_LRU_CLOCK_RESOLUTION 1000 //LRU clock resolution in ms

typedef struct redisObject{
	
	//type
	unsigned typed:4;
	//encoding
	unsigned encoding:4;
	//last access time
	unsigned lru:REDIS_LRU_BITS;
	//reference counter
	int refcount;
	//The actual pointer
	void *ptr;
}robj;

typedef struct zskiplistNode{

	robj *robj;

	double score;

	struct zskiplistNode *backward;

	struct zskiplistLevel{
		struct zskiplistNode *forward;

		unsigned int span;
	}level[];

}zskiplistNode;

typedef struct zskiplist{

	struct zskiplistNode *head, *tail;

	unsigned long length;

	int level;

}zskiplist;

typedef struct zset{
	//dictionary the key is the element, value is the score
	dict *dict;
	// skiplist, sorted element by the score
	// Use the avarge O(logN) time complexity to
	// find a specific element or find a bunch of
	// elements.
	zskiplist *zsl;

}zset;

typedef struct {
	double min, max;
	//if minex is 1, it means we want don't want the =, minex is 0, means we want =.
	int minex, maxex;
}zrangespec;

/* Structure to hold list iteration abstraction.
 *
 * 列表迭代器对象
 */
typedef struct {

    // 列表对象
    robj *subject;

    // 对象所使用的编码
    unsigned char encoding;

    // 迭代的方向
    unsigned char direction; /* Iteration direction */

    // ziplist 索引，迭代 ziplist 编码的列表时使用
    unsigned char *zi;

    // 链表节点的指针，迭代双端链表编码的列表时使用
    listNode *ln;

} listTypeIterator;

/* Structure for an entry while iterating over a list.
 *
 * 迭代列表时使用的记录结构，
 * 用于保存迭代器，以及迭代器返回的列表节点。
 */
typedef struct {

    // 列表迭代器
    listTypeIterator *li;

    // ziplist 节点索引
    unsigned char *zi;  /* Entry in ziplist */

    // 双端链表节点指针
    listNode *ln;       /* Entry in linked list */

} listTypeEntry;


/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
/*
 * 哈希对象的迭代器
 */
typedef struct {

    // 被迭代的哈希对象
    robj *subject;

    // 哈希对象的编码
    int encoding;

    // 域指针和值指针
    // 在迭代 ZIPLIST 编码的哈希对象时使用
    unsigned char *fptr, *vptr;

    // 字典迭代器和指向当前迭代字典节点的指针
    // 在迭代 HT 编码的哈希对象时使用
    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2

/* List data type*/
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subj, robj *value, int where);
robj* listTypePop(robj *subject, int where);
unsigned long listTypeLength(robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry);
void listTypeDelete(listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
void unblockClientWaitingData(redisClient *c);
void handleClientBlockedOnList(void);
void popGenericCommand(redisClient *c, int where);

/*Hash data type*/
void hashTypeConvert(robj *o, int enc);
void hashTypeTryConversion(robj *subject, robj **argv, int start, int end);
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2);
robj *hashTypeGetObject(robj *o, robj *key);
int hashTypeExists(robj *o, robj *key);
int hashTypeSet(robj *o, robj *key, robj *value);
int hashTypeDelete(robj *o, robj *key);
unsigned long hashTypeLength(robj *o);
hashTypeIterator *hashTypeInitIterator(robj *subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
							     unsigned char **vstr, 
								 unsigned int *vlen,
								 long long *vll);
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst);
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what);
robj *hashTypeLookupWirteOrCreate(redisClient *c, robj *key);

/**
 * Redis Object implementation
 */
#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)

/* Command prototypes */
/* String related commands*/
void setCommand(redisClient *c);
void setnxCommand(redisClient *c);
void setexCommand(redisClient *c);
void psetexCommand(redisClient *c);
void getCommand(redisClient *c);
void setrangeCommand(redisClient *c);
void getrangeCommand(redisClient *c);
void incrCommand(redisClient *c);
void decrCommand(redisClient *c);
void incrbyCommand(redisClient *c);
void decrbyCommand(redisClient *c);
void incrbyfloatCommand(redisClient *c);
/* list related command  */
void lpushCommand(redisClient *c);
void rpushCommand(redisClient *c);
void lpushxCommand(redisClient *c);
void rpushxCommand(redisClient *c);
void linsetCommand(redisClient *c);
void lpopCommand(redisClient *c);
void rpopCommand(redisClient *c);
void llenCommand(redisClient *c);
void lindexCommand(redisClient *c);
void lrangeCommand(redisClient *c);
void ltrimCommand(redisClient *c);
void lsetCommand(redisClient *c);
void lremCommand(redisClient *c);
/*hash map related command*/
void hsetCommand(redisClient *c);
void hsetnxCommand(redisClient *c);
void hgetCommand(redisClient *c);
void hmsetCommand(redisClient *c);
void hmgetCommand(redisClient *c);
void hdelCommand(redisClient *c);
void hlenCommand(redisClient *c);
void hkeysCommand(redisClient *c);
void hvalsCommand(redisClient *c);
void hgetallCommand(redisClient *c);
void hexistsCommand(redisClient *c);
void hscanCommand(redisClient *c);
void hincrByCommand(redisClient *c);
void hincrByfloatCommand(redisClient *c);


int checkType(redisClient *c, robj *o, int type);
size_t stringObjectLen(robj *o);

#endif