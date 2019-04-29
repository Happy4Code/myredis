#define ZSKIPLIST_MAXLEVEL	32
#define ZSKIPLIST_P	0.25

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

/* List data type*/
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subj, robj *value, int where);
robj* listTypePop(robj *subject, int where);
unsigned long listTypeLength(robk *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry);
void listTypeDelete(listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
void unblockClientWaitingData(redisClient *c);
void handleClientBlockedOnList(void);'
void popGenericCommand(redisClient *c, int where);

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






int checkType(redisClient *c, robj *o, int type);
size_t stringObjectLen(robj *o);
