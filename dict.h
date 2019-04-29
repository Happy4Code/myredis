#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

//Operate success
#define DICT_OK 0
//Operate failed
#define DICT_ERR 1

/**
 *Unused arguments generate annoying warnings...
 *If the dict private data is not use, use
 *this marco to aviod compiler warning.
 */
#define DICT_NOTUSED(V) ((void) V)

typedef struct dictEntry{
	
	//Key
	void *key;
	
	//Value
	union{
		void *val;
		uint64_t u64;
		int64_t s64;
	}v;

	//Point to next node
	struct dictEntry *next;


}dictEntry;

typedef struct dictType{

	//caculate the hashCode function
	unsigned int (*hashFunction)(const void *key);

	//duplicate key function
	void *(*keyDup)(void *privdata, const void *key);
	
	//duplicate value function
	void *(*valDup)(void *privdata, const void *obj)

	//key compare function
	int (*keyCompare)(void *privdata, const void *key1, const void *key2);
	
	//destory key function
	void (*keyDestructor)(void *privdata, void *key);

	//destory value function
	void (*valDestructor)(void *privadata, void *obj);

}dictType;

typedef struct dictht{
	
	//Hash table buckets
	dictEntry **table;

	//Size of the hashtable
	unsigned long size;

	//Hash table size mask, used
	//when we need to caculate the
	//index
	unsigned long sizemask;

	//The hash table nodes
	unsigned long used;
	

}dictht;

typedef struct dict{
	
	//dictionary specific function
	dictType *type;

	//private data(which is used with dictType)
	void *privdata;

	//hashtable
	//We use the ht[1] only when we do some rehash work
	//In common sence, we only use ht[0]
	dictht ht[2];

	//rehash index
	//rehashing not in process if rehashidx == -1
	int rehashidx;

	//Numbers of iterators current running
	int iterators;
}dict;

typedef struct dictIterator{
	
	//The dictionary will be iterated
	dict *d;

	//Table : The current iterate hashtable index, 0 or 1
	//        Recall than the dict has a array of dictht.
	//index: The current position the iterator points to.
	//safe:  Mark the iterator if it is safe.
	int table, index, safe;

	//entry: points to the current iterated entry
	//nextEntry: the next entry after the current iterate entry
	//           why we need this variable?
	//           because when the safe-iterator running, the entry
	//           pointer may change points to a different entry, 
	//           so we need an additional pointer to save the 
	//           next entry.	  
	dictEntry *entry, *nextEntry;
	
	// unsafe iterator fingerprint for misuse detection
	long long fingerprint;

}dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);

/*Initial size of every hash table*/
#define DICT_HT_INITIAL_SIZE	4

//Free the given dictionary entry
#define dictFreeVal(d, entry) \
	if((d)->type->valDestructor) \
		(d)->type->valDestructor((d)->privdata, (entry)->v.val)

//Set value for given entry
#define dictSetVal(d, entry, _val_) do{\
	if((d)->type->valDup) \
		entry->v.val = (d)->type->valDup((p)->privadata, _val_); \
	else \
		entry->v.val = (_val_); \
while(0)

// set a signed integer for the node value
#define dictSetSignedIntegerVal(entry, _val_) \
	do {entry->v.s64 = _val_;}  while(0)

// set an unsigned integer value for node
#define dictSetUnsignedIntegerVa(entry, _val_) \
	do {entry->v.u64 = _val_;} while(0)

#define dictFreeKey(d, entry) \
	if((d)->type->keyDestructor) \
		((d)->type->keyDestructor((d), (entry)->key)

#define dictSetKey(d, entry, _key_) do { \
	if((d)->type->dupKey)	\
		entry->key = (d)->type->dup((d)->privdata, _key_);\
	else \
		entry->key = (_key_);\
	}while(0)

//Compare two dictionary key
#define dictCompareKeys(d, key1, key2) \
	(((d)->type->keyCompare) ? \
		(d)->type->keyCompare((d)->privdata, key1, key2) : \
	 (key1) == (key2))

//Caculate the hashcode
#define dictHashKey(d, key) (d)->type->hashFunction(key)

//Get key in specific node
#define dictGetKey(he) ((he)->key)

//Get value in specific node
#define dictGetVal(he) ((he)->v.val)

//Get signed integer value in specific node 
#define dictGetSignedIntegerVal(he) ((he)->v.s64)

//Get unsigned integer value in specific node
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)

//Return the slots number of given dictory
#define dictSlots(d) ((d)->ht[0].size + (d)->ht[1].size)

//Returns the node number of given dictory
#define dictSize(d) ((d)->ht[0].used + (d)->ht[1].used)

//Check if the dictionary is rehashing
#define dictIsRehashing(ht) ((ht)->rehashidx != -1)

/****************API*******************/
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
dictEntry *dictReplaceRaw(dict *d, void *key);
int dictDelete(dict *d, const void *key);
int dictDeleteNoFree(dict *d, const void *key);
void dictRelease(dict *d);
dictEntry *dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
int dictGetRandomKeys(dict *d, dictEntry ***des, int count)
void dictPrintStats(dict *d);
unsigned int dictGetHashFunction(const void *key, int len);
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void *));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(unsigned int initval);
unsigned int dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, void *privdata);

/*Hash table types*/
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif __DICT_H
