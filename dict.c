#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>
#include <ctype.h>

#include "dict.h"
#include "zmalloc.h"
#include "redisassert.h"

/**
 * We can use the dictEnableResize() and dictDisableResize() funtion 
 * manually enable/disable the resize funtion of the hash table.This 
 * is very important for redis, as we use copy-on-write and don't 
 * want to move too much memory around when there is a child performing
 * saving operations.
 *
 * Note that even dict_can_resize is set to 0, not all resizess are prevent
 * : a hash table is still allowed to grow if the ratio between the number
 * of elements and the buckets > dict_force_resize_ratio
 */

//indicate whether perform resize
static int dict_can_resize = 1;
//force hash ratio
static unsigned int dict_force_resize_ratio = 5;

/*-----------private prototype-----------*/

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static int _dictKeyIndex(dict *ht, const void *key);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/*-----------hash Function-----------*/

/* Thomas Wang's 32 bit Mix Function
 * Thomas Wang think the good hash function must have two feature.
 * 1.Reversible.                 which means y = f(x) and x = f(y)
 * 2.Avalanche effect.           when n bits change, it will result
 *				 in output n/2 bit change.
 *
 * Keep in mind that the key += ~(key << n) function is a function
 * own the two features.
 * */

unsigned int dictIntHashFunction(unsigned int key){
	key += ~(key << 15);
	key ^= (key >> 10);
	key += (key << 3);
	key ^= (key >> 6);
	key += ~(key << 11);
	key ^= (key >> 16);
	return key;
}

/* Identity hash function for integer keys*/
unsigned int dictIdentityHashFunction(unsigned int key){
	return key;
}

static uint32_t dict_hash_function_seed = 5381;

void dictSetHashFunctionSeed(uint32_t seed){
	dict_hash_function_seed = seed;
}

uint32_t dictGetHashFunctionSeed(void){
	return dict_hash_function_seed;
}

/**
 * MurmurHash2, by Austin Appleby
 *
 */
unsigned int dictGenHashFunction(const void *key, int len){
	uint32_t seed = dict_hash_function_seed;
	const uint32_t m = 0x5bd1e995;
	const int r = 24;
	
	uint32_t h = seed ^ len;

	const unsigned char *data = (const unsigned char *)key;
	while(len >= 4){
		uint32_t k = *(uint32_t*)data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	/*Handle the last few bytes of input array*/
	switch(len){
		case 3: h ^= data[2] << 16;
		case 2: h ^= data[1] << 8;
		case 1: h ^= data[0]; h *= m;
	};

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;	
	return (unsigned int )h;
}

/*An case insensitive hash function base on djb function*/
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len){
	unsigned int hash = (unsigned int) dict_hash_function_seed;
	while(len--)
		hash = ((hash << 5) + hash) + (tolower(*buf++));
	return hash;
}

/*-----------API implementation-----------*/

static void _dictReset(dictht *ht){
	ht->table = NULL;
	ht->size = 0;
	ht->sizemask = 0;
	ht->used = 0;
}

/**
 * Create a new dictionary 
 */
dict *dictCreate(dictType *type, void *privDataptr){
	dict *d = zmalloc(sizeof(*d));
	
	_dictInit(d, type, privDataPtr);
	return d;
}

/*Initialize the hash table*/
int _dictInit(dict *d, dictType *type, void *privDataPtr){
	/* Init the two hashtable 
	 * But do not allocate memory space
	 * to the array inside the hashtable
	 */
	_dictReset(&d->ht[0]);
	_dictReset(&d->ht[1]);

	//set specific function
	d->type = type;

	//set private data
	d->privdata = privDataPtr;

	//set hash table rehash status
	d->rehashidx = -1;

	//set dictionary safe iterator num
	d->iterators = 0;

	return DICT_OK;
}

/**
 * Resize the table to minimal size that contains all elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1
 *
 * Returns DICT_ERR when the dictionary is rehash or dict_can_resize
 * is false
 *
 * If success create a ht[1] with less size, and return DICT_OK
 */

int dictResize(dict *d){
	int minimal;

	//Can't call this funcion when we are rehashing or dict_can_resize is false
	if(!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;
	
	//Caculate the number of nodes to make the radio close 1:1
	minimal = d->ht[0].used;
	if(minimal < DICT_HT_INITIAL_SIZE)
		minimal = DICT_HT_INITIAL_SIZE;
	
	//Adjust the size of the dictionary
	return dictExpand(d, minimal);
}

/**
 * Expand or create the hashtable
 *
 * Create a hashtable, and according to the situation and make different operation
 * 1) If the ht[0] is empty, then set the new hashtable to ht[0] 
 * 2) If the ht[0] is not empty, then set ht[1] as the new hashtable, and
 *    set the rehashidx, then perform the rehash operation on this dictionary.
 * If the rehashing is proceeding or size is not big enough then return DICT_ERR
 * If success create ht[0] or ht[1] reutrn DICT_OK
 */

int dictExpand(dict *d, unsigned long size){
	
	//new hash table
	dictht n;
	
	//According to size, to caculate the real size
	unsigned long realsize = _dictNextPower(size);
	/*The size is invaild if it is smaller than the number of
	 * elements already inside the hash table.*/
	if(dictIsRehashing(d) || d->ht[0].used > size)
		return DICT_ERR;
	/* Allocate the new hashtable and initialize all pointer to NULL*/
	
	n.size = realsize;
	n.sizemask = realsize - 1;
	n.table = zcalloc(realsize * sizeof(dictEntry*));
	n.used = 0;

	/* Is this the first initialization? If so it's not really a rehashing
	 * we just set the first hash table so that it can accept keys*/
	if(d->ht[0].table == NULL){
		d->ht[0] = n;
		return DICT_OK;
	}

	/**
	 * Prepare a second hash table for incremental rehashing
	 * If the ht[0] is not empty, then it is a rehashing.
	 * So we need to set the new hash table to ht[1], and
	 * set the hashidx flag.
	 */
	d->ht[1] = n;
	d->rehashidx = 0;
	return DICT_OK;
}

/**
 * Perform N steps of incremental rehashing. Returns 1 if there
 * is still key to move from the old to new hashtable, otherwise
 * 0 is returned. 
 *
 * Note that a rehashing step consist in moving a bucket(that may
 * have more than one key as we use chaining) from the old to the
 * new hash table.
 *
 * Time complexity O(N)
 */

int dictRehash(dict *d, int n){
	
	//Perform only when the hash table is rehashing
	if(!dictIsRehashing(d)) return 0;
	
	while(n--){
		dictEntry *de, *nextde;

		/*Check if we already rehashed the whole table*/
		if(h->ht[0].used == 0){
			zfree(d->ht[0].table);
			//Set the original ht[1] to ht[0]
			d->ht[0] = d->ht[1];
			//Reset the old ht[1]
			_dictReset(&d->ht[1]);
			d->rehashidx = -1;
			return 0;
		}
		assert(d->ht[0].size > (unsigned)d->rehashidx);

		//Skip the empty index in the hashtable, until we find a
		//non-Empty index
		while(d->ht[0].table[d->rehashidx] == NULL) d->rehashidx++;

		//Points to the first node of this list
		de = d->ht[0].table[d->rehashidx];

		//Move all the keys in this bucket from the old to the new hash table
		//Time complexity T = O(1)
		while(de){
			unsigned int h;

			//Save the next node pointer
			nextde = de->next;

			/*Get the index in the new hash table*/
			h = dictHashKey(d, de->key) & d->ht[1].sizemask;
			
			//Insert node to the new Hashtable
			de->next = d->ht[1].table[h];
			d->ht[1].table[n] = de;

			//Update the counter
			//Decremental ht[0] used node
			//Incremental ht[1] used node
			d->ht[0].used--;
			d->ht[1].used++;
			
			//Deal next node
			de = nextde;
		}
		//Set the dealed hash table index to NULL
		d->ht[0].table[d->rehashidx] = NULL;
		d->rehashidx++;
	}
	return 1;
}

/**
 * Return the UNIX timestamp in Milliseconds
 */
long long timeInMilliseconds(void){
	struct timeval tv;

	gettimeofday(&tv, NULL);
	return (((long long)tv.tv_sec) * 1000) + (tv.tv_usec/1000);
}

/* Rehash for amount of time between ms milliseconds and ms+1 milliseconds
 * T = O(N)
 * */
int dictRehashMilliseconds(dict *d, int ms){
	//Record start time
	long long start = timeInMilliseconds();
	int rehashes = 0;
	while(dictRehash(d, 100)){
		rehashes += 100;
		//If the time is over then break
		if(timeInMilliseconds() - start > ms) break;
	}
	return rehashes;
}

/**
 * This function perfoms just a step of rehashing, and only if there are
 * no safe iterators bound to our hash table.When we have iterators in 
 * the middle of a rehashing we can't mess with the two hash tables 
 * otherwise some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the 
 * dictionary so that the hash table automatically migrates from H1 or
 * H2 while it is actively used.
 *
 * Time complexity O(1)
 */
static void _dictRehashStep(dict *d){
	if(d->iterators == 0) dictRehash(d, 1);
}

/**
 * Add an elements to the target hash table
 * Only when the given key is not existed in
 * the dictionary, the add operation will success.
 */
int dictAdd(dict *d, void *key, void *val){
	//Try to add this element to the dict
	//and return the new node
	dictEntry *entry = dictAddRaw(d, key);
	//key is existed
	if(!entry) return DICT_ERR;
	
	//the key is not existed
	dictSetVal(d, entry, val);

	//add success
	return DICT_OK;
}

/**
 * Low level add. This function adds the entry but instead of setting
 * a value returns the dictEntry structure to user, that will make sure
 * to fill the value field as he wished.
 *
 * Return values:
 * If key is already exists, NULL is return.
 *
 * If key is not exists, key is added, the hash entry
 * is returned to be manipulated by the user. 
 */
dictEntry *dictAddRaw(dict *d, void *key){
	int index;
	dictEntry *entry;
	dictht *ht;

	//If it is allowed, perform one step rehash
	//Here is the point, the rehash is a time-consume
	//work, but we amortized the time to n operations.
	//Each operation only takes O(1) time, the time-consume
	//is acceptable.
	if(dictIsRehashing(d)) _dictRehashStep(d);

	//Get the index of the new element, or -1 if it
	//is exists
	if((index = dictHashKey(d, key)) == -1)
		return NULL;

	//Choose the table to insert according to 
	//whether we are perform rehash
	ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
	//Allocate new space for the new entry
	entry = zmalloc(sizeof(dictEntry));
	
	entry->next = ht->table[index];
	ht->table[index] = entry;
	ht->used++;
	dictSetKey(d, entry, key);
	return entry;
}

/**
 * Add an element, dicarding the old if the key aleary existed.
 *
 * Returns 1 if the key was added from scratch, 0 if there was
 * already an element with such key and dictReplace() just perform
 * a value update operation.
 */
int dictReplace(dict *d, void *key, void *val){
	dictEntry *entry, auxentry;
	
	//Try to add an element.If the key doesn't
	//exist then it will return DICT_OK
	if((dictAdd(d, key, val) == DICT_OK)	
		return 1;
	
	entry = dictFind(d, key);
	/**
	 * Set the new value and free the old one.Note that it is
	 * important to do that in this order, as the value may just
	 * be exactly the same as the previous one.In this context,
	 * think to reference counting, you want to increment(set)
	 * and then decrement(free), and not the reverse.
	 */	
	auxentry = *entry;
	dictSetVal(d, entry, val);
	dictFreeVal(d, &auxentry);
	return 0;
}

/**
 * dictReplaceRaw() is simply a version of dictAddRaw() that always
 * return the hash entry of the specific key, even if the key already
 * exists and can't be added(in that case the entry of the already exsting key
 * is returned)
 */
dictEntry *dictReplaceRaw(dict *d, void *key){
	dictEntry *entry = dictFind(d, key);

	return entry ? entry : dictAddRaw(d, key);
}

/**
 * Search and remove an element
 */
static int dictGenericDelete(dict *d, const void *key, int nofree){
	unsigned int h, idx;
	dictEntry *he, *prevHe;
	int table;

	//If the hash table is empty
	if(d->ht[0].size == 0) return DICT_ERR;
	
	//Perfom one step rehashing
	if(dictIsRehashing(d)) _dictRehashStep(d);

	//caculate hash  index
	h = dictHashKey(d, key);

	for(table = 0; table <= 1; table++){
		idx = h & d->ht[table].sizemask;
		he = d->ht[table].table[idx];
		prevHe = NULL;

		while(he){
			if(dictCompareKeys(d, key, he->key)){
				if(prevHe){
					prevHe->next = he->next;
				}else{
					d->ht[table].table[idx] = he->next; 
				}

				//Do i need to free the key and value of the deleted node
				if(!nofree){
					dictFreeKey(d, he);
					dictFreeVal(d, he);
				}

				//Release the node itself
				zfree(he);
				//Update the used count of this hashtable
				d->ht[table].used--;
				
				return DICT_OK;
			}

			prevHe = he;
			he = he->next;
		}
		//If we get here, it means we 
		//didn't find the match in the
		//first table, we search the secnod
		//hash table only when the table is
		//rehashing.
		if(!dictIsRehashing(d)) break;
	}
	return DICT_ERR;
}

/**
 * Delete a specfic node from the hashtable
 */
int dictDelete(dict *ht, const void *key){
	return dictGenericDelete(ht, key, 0);
}

/**
 * Delete a specific node from the hashtable
 * but not free the node key and value.
 */
int dictDeleteNoFree(dict *ht, const void *key){
	return dictGenericDelete(ht, key, 1);
}

/**
 * Destroy an entire dictionary
 * And clear the hashtable attribute.
 */
int _dictClear(dict *d, dictht *ht, void(callback)(void *)){
	unsigned long i;

	//Free all elements
	for(i = 0; i < ht->size && ht->used > 0; i++){
		dictEntry *he, *nextHe;
		
		if(callback && (i & 65535) == 0) callback(d->privdata);

		//skip the empty list
		if((he = ht->table[i]) == NULL) continue;

		while(he){
			nextHe = he->next;
			//Delete the key
			dictFreeKey(d, he);
			//Delete the value
			dictFreeVal(d, he);
			//Free the node itself
			zfree(he);
			//Update the he entry pointer
			he = nextHe;

			ht->used--;
		}
	}
	zfree(ht->table);
	_dictReset(ht);
	return DICT_OK;
}

/**
 * Clear and release the whole dictionary
 */
void dictRelease(dict *d){
	//Delete and empty the two hashtable
	_dictClear(d, d->ht[0], NULL);
	_dictClear(d, d->ht[1], NULL);
	//empty the d
	zfree(d);
}

/**
 * Find a specific node in the dictionary,
 * if it is already existed, return the 
 * node, otherwise return NULL.
 */
dictEntry *dictFind(dict *d, const void *key){
	unsigned int h, idx, table;
	dictEntry *he;

	if(d->ht[0].size == 0) return NULL;

	if(dictIsRehashing(d)) _dictRehashStep(d);

	h = dictHashKey(d, key);
	for(table = 0; table <= 1; i++){
		idx = h & d->ht[table].sizemask;
		he = d->ht[table].table[idx];
		
		while(he){
			if(dictCompareKeys(d, key, he->key)){
				return he;
			}
			he = he->next;
		}
		if(!dictIsRehashing(d)) break;
	}
	return NULL;
}

/**
 * Get the specific node value
 */
void *dictFetchValue(dict *d, const void *key){
	dictEntry *entry;
	entry = dictFind(d, key);
	return entry ? dictGetVal(entry) : NULL;
}

/**
 * A fingerprint is a 64 bit number that represent the state of the dictionary
 * at a given time, it is just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 *
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating
 */
long long dictFingerprint(dict *d){
	long long integers[6], hash = 0;
	int j;

	integers[0] = (long)d->ht[0].table;
	integers[1] = (long)d->ht[0].size;
	integers[2] = (long)d->ht[0].used;
	integers[3] = (long)d->ht[1].table;
	integers[4] = (long)d->ht[1].size;
	integers[5] = (long)d->ht[1].used;

	for(j = 0; j < 6; j++){
		hash += integers[j];
		hash = (~hash) + (hash << 21);
		hash = hash ^ (hash >> 24);
		hash = (hash + (hash << 3)) + (hash << 8);
		hash = hash ^ (hash >> 14);
		hash = (hash + (hash << 2)) + (hash << 4);
		hash = hash ^ (hash >> 28);
		hash = hash + (hash << 31);
	}
	return hash;
}

/**
 * Create and return an unsafe iterator
 */
dictIterator *dictGetIterator(dict *d){
	dictIterator *iter = zmalloc(sizeof(*iter));
	iter->d = d;
	iter->table = 0;
	iter->index = -1;
	iter->safe = 0;
	iter->entry = NULL;
	iter->nextEntry = NULL;
	return iter;
}

dictIterator *dictGetSafeIterator(dict *d){
	dictIterator *i = dictGetIterator(d);
	i->safe = 1;
	return i;
}

/**
 * Get the current node the iterator currently points to
 */
dictEntry *dictNext(dictIterator *iter){
	while(1){
		//There is two situation why you get into 
		//this block.
		//1.The first time run the iterator
		//2.Current bucket list get to the end.
		if(iter->entry == NULL){
			dict *ht = &iter->d->ht[iter->table];
			if(iter->index == -1 && iter->table == 0){
				if(iter->safe)
					iter->d->iterators++;
				else
					iter->fingerprint = dictFingerprint(iter->d);
			}
			iter->index++;

			//If current index is greater than the table size
			//it means the table has been iterated.
			if(iter->index >= (signed)ht->size){
				if(dictIsRehashing(iter->d) && iter->table == 0){
					iter->table++;
					iter->index = 0;
					ht = &iter->d->dt[1];
				}else{
					break;
				}	
			}
			iter->entry = ht->table[iter->index];
		}else{
			iter->entry = iter->nextEntry;
		}
		
		if(iter->entry){
			iter->nextEntry = iter->entry->next;
			return iter->entry;
		}
	}
       return NULL;	
}

/**
 * Release the iterator
 */
void dictReleaseIterator(dictIterator *iter){
	if(!(iter->index == -1 && iter->table == 0)){
		if(iter->safe)
			iter->d->iterator--;
		else
			assert(iter->fingerprint == dictFingerprint(iter->d));
	}
	zfree(iter);
}

/**
 * Get a random key from the hash table
 */
dictEntry *dictGetRandomKey(dict *d){
	dictEntry *de, *orighe;
	unsigned int h;
	int listlen, listele;

	if(dictSize(d) == 0) return NULL;

	if(dictIsRehashing(d)) _dictRehashStep(d);'
	
	if(dictIsRehashing(d)){
		do{
			h = random() & (d->ht[0].size + d->ht[1].size);
			he = (h >= ht[0].size) ? d->ht[1].table[h- d->ht[0].size] : 
				d->ht[0].table[h];
		}while(he == NULL);
	}else{
		do{
			h = random() & d->ht[0].sizemask;
			he = d->ht[0].table[h];
		}while(he == NULL);
	}

	listlen = 0;
	orighe = he;

	while(he){
		he = he->next;
		listlen++;
	}
	listele = random() % listlen;
	he = orighe;

	while(listele--) he = he->next;

	return he;
}


/**
 * Our hash table is a power of two
 * Caculate a value that is power of two 
 * that is great or equals to size.
 */
static unsigned long _dictNextPower(unsigned long size){
	unsigned long i = DICT_HT_INITIAL_SIZE;

	if(size >= LONG_MAX) return LONG_MAX;

	while(1){
		if(i >= size)
			return i;
		i <<= 1;
	}
}
