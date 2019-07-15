#include "redis.h"
#include <math.h>
#include <ctype.h>


/**
 * Create a new robj object
 */ 
robj *createObject(int type, void *ptr){

	robj *o = zmalloc(sizeof(*o));
	
	o->type = type;
	o->encoding = REDIS_ENCODING_RAW;
	o->refcount = 1;
	o->ptr = ptr;

	//Set the LRU to the current lruclock
	o->lru = LRU_CLOCK();
	return o;
}

/* Create a string object with encoding REDIS_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
robj *createRawStringObject(char *ptr, size_t len){
	return createObject(REDIS_STRING, sdsnewlen(ptr, len));
}

/* Create a string object with encoding REDIS_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */
// 创建一个 REDIS_ENCODING_EMBSTR 编码的字符对象
// 这个字符串对象中的 sds 会和字符串对象的 redisObject 结构一起分配
// 因此这个字符也是不可修改的

robj *createEmbeddedStringObject(char *ptr, size_t len){
	robj *o = zmalloc(sizeof(robj) + sizeof(struct sdshdr) + len + 1);
	//注意o是robj 类型 所以o+1 实际上移动的字节为 sizeof(robj),在本出恰好将
	//o指针移动到sdshdr结构开始的地方
	struct sdshdr *sh = (void *)(o + 1);

	o->type = REDIS_STRING;
	o->encoding = REDIS_ENCODING_EMBSTR;
	o->ptr = sh + 1;//这个地方注意了 由于sh的类型是sdshdr 所以sh + 1将sh移动到 sh->buf的位置
	o->refcount = 1;
	o->lru = LRU_CLOCK();

	sh->len = len;
	sh->free = 0;
	if(ptr){
		memcpy(sh->buf, ptr, len);
		sh->buf[len] = '\0';
	}else{
		memset(sh->buf, 0, len + 1);
	}
	return o;
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * REIDS_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 39 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
#define REDIS_ENCODING_EMBSTR_SIZE_LIMIT 39
robj *createStringObject(char *ptr, size_t len){
	if(len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT)
		return createEmbeddedStringObject(ptr, len);
	else
		return createRawStringObject(ptr, len);
}

/* According the input value create an string object.
 * 
 * This value may be encoding as INT encoding ,
 * or be encoding as RAW encoding
 */
robj *createStringObjectFromLongLong(long long value){
	
	robj *o;

	//If the value range is in the redis shared object.
	if(value >= 0 && value <= REDIS_SHARED_INTEGERS){
		incrRefCount(shared.integers[value]);
		o = shared.integer[value];
	}else{
		//Check if it can be represent as a long
		if(value >= LONG_MIN && value <= LONG_MAX){
			o = createObject(REDIS_STRING, NULL);
			o->encoding = REDIS_ENCODING_INT;
			o->ptr = (void *)((long)value);
		}else{
			o = createObject(REDIS_STRING, sdsfromlonglong(value));
		}
	}
	return o;
}

/* Note: this function is defined into object.c since here it is where it
 * belongs but it is actually designed to be used just for INCRBYFLOAT */
/*
 * 根据传入的 long double 值，为它创建一个字符串对象
 *
 * 对象将 long double 转换为字符串来保存
 */
robj *createStringObjectFromLongDouble(long double value){
	char buf[256];
	int len;

	/* We use 17 digits precision since with 128 bit floats that precision
     * after rounding is able to represent most small decimal numbers in a way
     * that is "non surprising" for the user (that is, most small decimal
     * numbers will be represented in a way that when converted back into
     * a string are exactly the same as what the user typed.) */
    // 使用 17 位小数精度，这种精度可以在大部分机器上被 rounding 而不改变
	len = snprintf(buf, sizeof(buf), "%.17LF", value);
	/* Now remove trailing zeroes after the '.' */
    // 移除尾部的 0 
    // 比如 3.1400000 将变成 3.14
    // 而 3.00000 将变成 3
	if(strchr(buf, "." != NULL)){
		char *p = buf + len - 1;
		while(*p == '0'){
			p--;
			len--;
		}
		//remove the .
		if(*p == '.') len--;
	}

	return createStringObject(buf, len);
}

/* Duplicate a string object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * 复制一个字符串对象，复制出的对象和输入对象拥有相同编码。
 *
 * This function also guarantees that duplicating a small integere object
 * (or a string object that contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1).
 *
 * 另外，
 * 这个函数在复制一个包含整数值的字符串对象时，总是产生一个非共享的对象。
 *
 * The resulting object always has refcount set to 1. 
 *
 * 输出对象的 refcount 总为 1 。
 */
robj *dupStringObject(robj *o){
	robj *d;

	switch (o->encoding)
	{
	case REDIS_ENCODING_RAW:
		d = createRawStringObject(o->ptr, sdslen(o->ptr));
		break;
	case REDIS_ENCODING_EMBSTR:
		d = createEmbeddedStringObject(o->ptr, sdslen(o->ptr));
		break;
	case REDIS_ENCODING_INT:
		d = createObject(REDIS_STRING, NULL);
		d->encoding = REDIS_ENCODING_INT;
		d->ptr = o->ptr;
		break;
	default:
		redisPanic("Wrong encoidng...");
	}
	return d;
}

robj *createListObject(void){

	list *l = listCreate();

	robj *o = createObject(REDIS_LIST, l);
	
	listSetFreeMethod(l, decrRefCountVoid);

	o->encoding = REDIS_ENCODING_LINKEDLIST;
	
	return o;
}

/* Create a ziplist list object
 */
robj *createZiplistObject(void){

	unsigned char *zl = ziplistNew();

	robj *o = createObject(REDIS_LIST, zl);

	o->encoding = REDIS_ENCODING_ZIPLIST;

	return o;
}

/**
 * We create a set type object the default 
 * encoding is hashtble 
 */
robj *createSetObject(){

	dict *d = dictCreate(&setDictyType, NULL);

	robj *o = createObject(REDIS_SET, d);

	o->encoding = REDIS_ENCODING_HT;

	return o;
}

/**
 * We create a hash intset object
 */
robj *createIntsetObject(){
	
	intset *set = intsetNew();
	robj *o = createObject(REDIS_SET, set);
	o->encoding = REDIS_ENCODING_INT;
	return o;
}

/**
 * We create a hash type object the default 
 * encoding is ziplist 
 */
robj *createHashObject(){
	unsigned char *zl = ziplistNew();

	robj *o = createObject(REDIS_HASH, zl);

	o->encoding = REDIS_ENCODING_ZIPLIST;

	return o;
}

robj *createZsetObject(void){
	
	zset *zs = zmalloc(sizeof(zset));

	robj *o;
	
	zs->dict = dictCreate(&zsetDictType, NULL);
	zs->zsl = zslCreate();

	o = createObject(REDIS_ZSET, zs);

	o->encoding = REDIS_ENCODING_SKIPLIST;

	return o;
}

robj *createZsetZiplistObject(){

	unsigned char *zl = ziplistNew();

	robj *o = createObject(REDIS_ZSET, zl);

	o->encoding = REDIS_ENCODING_ZIPLIST;

	return o;
}

/* Free the list object
 */
void freeListObject(robj *o){
	
	switch (o->encoding)
	{
	case REDIS_ENCODING_LINKEDLIST:
		listRelease((list *)o->ptr);
		break;

	case REDIS_ENCODING_ZIPLIST:
		zfree(o->ptr);
		break;	
	default:
		redisPanic("Unknown list encoding type");
	}
}

/* Free Set Object
 */
void freeSetObject(robj *o){
	switch (o->encoding){
		
		case REDIS_ENCODING_INTSET:
			zfree(o->ptr);
			break;
		break;
	
		case REDIS_ENCODING_HT:
			dictRelease((dict *)o->ptr);
		break;

		default:
			redisPanic("Unknown set encoding type");
	}
}

void freeZsetObject(robj *o){

	zset *zs;
	switch (o->encoding){
		case REDIS_ENCODING_SKIPLIST:
			zs = o->ptr;
			dictRelease((dict *)zs->dict);
			zfree(zs->zsl);
			break;
		case REDIS_ENCODING_ZIPLIST:
			zfree(o->ptr);
			break;
		default:
				redisPanic("Unknown zset encoding type");
	}	
}

/*
 * 释放哈希对象
 */
void freeHashObject(robj *o) {

    switch (o->encoding) {

    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;

    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;

    default:
        redisPanic("Unknown hash encoding type");
        break;
    }
}

void incrRefCount(robj *o){
	o->refcount++;
}

/**
 * decrement the reference count
 */
void decrRefCount(robj *o){

	if(o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");

	//Release the object
	if(o->refcount == 1){
		switch(o->type){
			case REDIS_STRING: freeStringObject(o); break;
			case REDIS_LIST: freeListObject(o); break;
			case REDIS_SET: freeSetObject(o); break;
			case REDIS_ZSET: freeZsetObject(o); break;
			case REDIS_HASH: freeHashObject(o); break;
			default: redisPanic("Unknown object type"); break;
		}
		zfree(o);
	}else{
		o->refcount--;
	}
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method. 
 *
 * 作用于特定数据结构的释放函数包装
 */
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

/* This function set the ref count to zero without freeing the object.
 *
 * 这个函数将对象的引用计数设为 0 ，但并不释放对象。
 *
 * It is useful in order to pass a new object to functions incrementing
 * the ref count of the received object. Example:
 *
 * 这个函数在将一个对象传入一个会增加引用计数的函数中时，非常有用。
 * 就像这样：
 *
 *    functionThatWillIncrementRefCount(resetRefCount(CreateObject(...)));
 *
 * Otherwise you need to resort to the less elegant pattern:
 *
 * 没有这个函数的话，事情就会比较麻烦了：
 *
 *    *obj = createObject(...);
 *    functionThatWillIncrementRefCount(obj);
 *    decrRefCount(obj);
 */
robj *resetRefCount(robj *o){
	o->refcount = 0;
	return o;
}

//Try to get the integer value from the object
//and save the result to the *target
//
//If success, return REDIS_OK
//Otherwise, return REDIS_ERR, also send a error reply
int getLongLongFromObject(robj *o, long long *target){
	long long value;
	char *eptr;

	if(o == NULL){
		//If the input object is NULL
		value = 0;
	}else{
		//Make sure the the object is string type
		redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);
		if(sdsEncodingObject(o)){
			errno = 0;
			value = strtoll(o->ptr, &eptr, 10);
			if(isspace(((char *)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
				return REDIS_ERR;
		}else if(o->encoding == REDIS_ENCODING_INT){
			value = (long)o->ptr;
		}else{
			reidPanic("Unknowing string encoding");
		}
	}

	if(target) *target = value;

	return REDIS_OK;
}

int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg){

	long long value;

	if(getLongLongFromObject(o, &value) != REDIS_OK){
		if(msg != NULL)
			addReplyError(c, msg);
		else
			addReplyError(c, "value is not an integer or out of range");
		return REDIS_ERR;
	}
	*target = value;

	return REDIS_OK;
}

/**
 * Check object o type is the same as the input `type` parameter.
 * If the same return 0, otherwise return 1, and reply a error msg.
 */ 
int checkType(redisClient *c, robj *o, int type){
	
	if(o->type != type){
		addReply(c, shared.wrongtypeerr);
		return 1;
	}
	return 0;
}

/**
 * Check if the value in the `o` can be represent as long long.
 */ 
int isObjectRepresentableAsLongLong(robj *o, long long *llval){
	if(o->type == REDIS_ENCODING_INT){
		if(llval){
			*llval = (long)o->ptr;
			return REDIS_OK;
		}
	}else{
		return string2ll(o->ptr, sdslen(o->ptr), llval) ? REDIS_OK : REDIS_ERR;
	}
}

/* Try to encode a string object to save memory
 */
robj *tryObjectEncoding(robj *o){
	long value;

	sds s = o->ptr;
	size_t len;

	/* Make sure this is a string object, the only type we encode
	 * in this function. Other types use encoded memory efficient
	 * representations but are handled by the commands implementing 
	 * the type.
	 */
	redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);
    /* We try some specialized encoding only for objects that are
     * RAW or EMBSTR encoded, in other words objects that are still
     * in represented by an actually array of chars. */
	if(!sdsEncodedObject(o)) return o;

	/* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis and may end in places where
     * they are not handled. We handle them only as values in the keyspace. */
	if(o->refcount > 1) return o;

	/* Check if we can represent this string as a long integer.
     * Note that we are sure that a string larger than 21 chars is not
     * representable as a 32 nor 64 bit integer. */
    // 对字符串进行检查
    // 只对长度小于或等于 21 字节，并且可以被解释为整数的字符串进行编码
	len = sdslen(s);
	if(len <= 21 && string2l(s, len, &value)){
		/* This object is encodable as a long. Try to use a shared object.
         * Note that we avoid using shared integers when maxmemory is used
         * because every object needs to have a private LRU field for the LRU
         * algorithm to work well. */
		if(server.maxmemory == 0 &&
		   value >= 0 &&
		   value < REDIS_SHARED_INTEGERS){
			
			decrRefCount(o);
			incrRefCount(shared.integers[value]);
			return shared.integers[value];
		}else{
			if(o->encoding == REDIS_ENCODING_RAW) sdsfree(s);
			o->encoding = REDIS_ENCODING_INT;
			o->ptr = (void*)value;
			return o;
		}
	}

	/* If the string is small and is still RAW encoded,
     * try the EMBSTR encoding which is more efficient.
     * In this representation the object and the SDS string are allocated
     * in the same chunk of memory to save space and cache misses. */
    // 尝试将 RAW 编码的字符串编码为 EMBSTR 编码
	if(len <= REDIS_ENCODING_EMBSTR_SIZE_LIMIT){
		robj *emb;

		if(o->encoding == REDIS_ENCODING_EMBSTR) return o;
		emb = createEmbeddedStringObject(s, sdslen(s));
		return emb;
	}
	/* We can't encode the object...
     *
     * Do the last try, and at least optimize the SDS string inside
     * the string object to require little space, in case there
     * is more than 10% of free space at the end of the SDS string.
     *
     * We do that only for relatively large strings as this branch
     * is only entered if the length of the string is greater than
     * REDIS_ENCODING_EMBSTR_SIZE_LIMIT. */
    // 这个对象没办法进行编码，尝试从 SDS 中移除所有空余空间
	if(o->encoding == REDIS_ENCODING_RAW && 
	   sdsavail(s) > len/10){
		   o->ptr = sdsRemoveFreeSpace(s);
	   }
	return o;
}

/**
 * Return the length of the string object
 */
size_t stringObjectLen(robj *o){
	
	redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

	if(sdsEncodedObject(o)){
		return sdslen(o->ptr);
	}else{
		//which is the INT encoding
		char buf[32];
		return ll2string(buf, 32, (long)o->ptr);
	}
}

/* Get a decoded version of an encoded object (returned as a new object).
 *
 * 以新对象的形式，返回一个输入对象的解码版本（RAW 编码）。
 *
 * If the object is already raw-encoded just increment the ref count. 
 *
 * 如果对象已经是 RAW 编码的，那么对输入对象的引用计数增一，
 * 然后返回输入对象。
 * 这个函数很有趣，它主要是用于当我们需要讲一个经过tryObjectEncoding处理过的对象
 * 还原的场景，有意思的地方在于如果对象是一个RAW或者EMBER编码时，他还是要增加计数，
 * 这个原因在于使得这个函数更加对称，即不管对于任意的string Encoding时，总是会
 * 增加引用计数，调用者在使用完这个对象统一调用DecrRefCount即可，不需要区分是什么
 * String具体是哪一种编码
 */
robj *getDecodedObject(robj *o){
	robj *dec;

	if(sdsEncodedObject(o)){
		incrRefCount(o);
		return o;
	}

	if(o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT){
		char buf[32];

		ll2string(buf, 32, (long)o->ptr);
		//用于存储 的字节数组上限为32，所以此处创建的字符串一定是ember类型的，因为当字符串大小
		//小于39时，都会创建ember类型字符串。
		dec = createStringObject(buf, strlen(buf));
		return dec;
	}else{
		redisPanic("Unknown encoding type");
	}
}

/* Comparte two string objects via memcmp() or strcoll() depending on flags.
 * 
 * Note that the objects may be integer-encoded. In such a case we use ll2string()
 * to get a string representation of the numbers on the stack and compare the stacks,
 * it's much faster than calling getDecodedObject().
 * 
 * Important note : when REDIS_COMPARE_BINARY is used a binary-safe comparsion is used.
 */
#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)

int compareStringObjectsWithFlags(robj *a, robj *b, int flags){
	redisAssertWithInfo(NULL, a->type == REDIS_STRING && b->type == REDIS_STRING);

	char bufa[128], bufb[128], *astr, *bstr;
	size_t alen, blen, minlen;

	if(a == b) return 0;

	//Try to check each if it is a sds encoded, otherwise convert it to a string.
	if(sdsEncodedObject(a)){
		astr = a->ptr;
		alen = sdslen(astr);
	}else{
		alen = ll2string(bufa, sizeof(bufa), (long)a->ptr);
		astr = bufa;
	}

	if(sdsEncodedObject(b)){
		bstr = b->ptr;
		blen = sdslen(bstr);
	}else{
		blen = ll2string(bufb, sizeof(bufb), (long)b->ptr);
		bstr = bufb;
	}

	if(flags & REDIS_COMPARE_COLL){
		return strcoll(astr, bstr);
	}else{
		int cmp;

		minlen = (alen < blen) ? alen : blen;
		cmp = memcmp(astr, bstr, minlen);
		if(cmp == 0) return alen - blen;
		return cmp;
	}
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison */
int compareStringObjects(robj *a, robj *b){
	return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_BINARY);
}

/*Wrapper fo compareStringObjectsWithFlags() with collation */
int collateStringObject(robj *a, robj *b){
	return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_COLL);
}

/* Equal string objects return 1 if the two objects are the same from the 
 * point of view of a string comparsion, otherwise 0 is returned.
 * 
 * Note that this function is faster the compareStringObject(a, b) == 0 
 * because it can perform some optimization.
 */
int equalStringObjects(robj *a, robj *b){
	
	if(a->encoding == REDIS_ENCODING_INT && 
	   b->encoding == REDIS_ENCODING_INT){
		   return a->ptr == b->ptr;
	   }else{
		   return compareStringObjects(a, b) == 0;
	   }
}

/* Get the double from the object */
int getDoubleFromObject(robj *o, double *target){
	double value;
	char *eptr;

	if(o == NULL){
		value = 0;
	}else{
		redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);

		//try to convert the double from the string.
		if(sdsEncodedObject(o)){
			errno = 0;
			value = strtod(o->ptr, &eptr);
			if(isspace(((char *)o->ptr)[0]) || 
			   eptr[0] != '\0' ||
			   (errno == ERANGE && 
			   		(value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
					   errno == EINVAL ||
					   isnan(value))
					   return REDIS_ERR;
		}else if(o->encoding == REDIS_ENCODING_INT){
			value = (long)o->ptr;
		}else{
			redisPainc("Unknown string encoding");
		}
	}
	return REDIS_OK;
}

int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg){
	
	double value;

	if(getDoubleFromObject(o, &value) != REDIS_OK){
		if(msg != NULL)
			addReplyError(c, (char *)msg);
		else
			addReplyError(c, "value is not a valid float");
		return REDIS_ERR;
	}
		
	*target = value;
	return REDIS_OK;
}

/* Try to get the object from the long double
 * If success, put the result into the *target, return REDIS_OK
 * Otherwise, return REDIS_ERR
 */
int getLongDoubleFromObject(robj *o, long double *target){
	long double value;
	char *eptr;

	if(o == NULL){
		value = 0;
	}else{

		redisAssertWithInfo(NULL, o, o->type == REDIS_STRING);
		//RAW encode, try to convert the string into long double
		if(sdsEncodedObject(o)){
			errno = 0;
			value = strtold(o->ptr, &eptr);
			if(isspace(((char *)o->ptr)[0]) || eptr[0] != '\0' ||
			   errno == ERANGE || isnan(value))
			   return REDIS_ERR;
		}else if(o->encoding == REDIS_ENCODING_INT){
			value = (long)o->ptr;
		}else{
			redisPainc("Unknown string encoidng");
		}
	}
	*target = value;
	return REDIS_OK;
}

/*
 * 尝试从对象 o 中取出 long 类型值，
 * 或者尝试将对象 o 中的值转换为 long 类型值，
 * 并将这个得出的整数值保存到 *target 。
 *
 * 如果取出/转换成功的话，返回 REDIS_OK 。
 * 否则，返回 REDIS_ERR ，并向客户端发送一条 msg 出错回复。
 */
int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    // 先尝试以 long long 类型取出值
    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;

    // 然后检查值是否在 long 类型的范围之内
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg){

	long double value;

	if(getLongDoubleFromObject(o, &value) != REDIS_OK){
		if(msg != NULL){
			addReplyError(c, (char *)msg);
		}else{
			addReplyError(c, "value is not a valid float");
		}
		return REDIS_ERR;
	}

	*target = value;
	return REDIS_OK;
}

/*
 * Return the string encoding
 */
char *strEncoding(int encoding){

	switch (encoding){

		case REDIS_ENCODING_RAW : return "raw";
		case REDIS_ENCODING_INT : return "int";
		case REDIS_ENCODING_HT : return "hashtable";
		case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
		case REDIS_ENCODING_ZIPLIST: return "ziplist";
		case REDIS_ENCODING_INTSET: return "intset";
		case REDIS_ENCODING_SKIPLIST: return "skiplist";
		case REDIS_ENCODING_EMBSTR: return "embstr";
		default : return "unknown";
	}
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long long estimateObjectIdleTime(robj *o){
	unsigned long long lruclock = LRU_CLOCK();
	
	if(lruclock >= o->lru){
		return (lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
	}else{
		return (lruclock + (REDIS_LRU_CLOCK_MAX - o->lru)) * REDIS_LRU_CLOCK_RESOLUTION;
	}
}

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters.
 *
 * OBJECT 命令的辅助函数，用于在不修改 LRU 时间的情况下，尝试获取 key 对象
 */
robj *objectCommandLookup(redisClient *c, robj *key){
	dictEntry *de;

	if((de = dictFind(c->db->dict, key->ptr)) == NULL) return NULL;

	return (robj*)dictGetVal(de);
}

/*
 * 在不修改 LRU 时间的情况下，获取 key 对应的对象。
 *
 * 如果对象不存在，那么向客户端发送回复 reply 。
 */
robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <verb> ... arguments ... */
void objectCommand(redisClient *c){
	robj *o;

	//if you want to inspect the refcount of the object
	if(!strcasecmp(c->argv[1]->ptr, "refcount") && c->argc == 3){
		if((o = objectCommandLookupOrReply(c, c->argv[2], shared.nullbulk)) == NULL)
			return ;
		addReplyLongLong(c, o->refcount);
	}else if(!strcasecmp(c->argv[1]->ptr, "encoding") && c->argc == 3){
		if((o = objectCommandLookupOrReply(c, c->argv[2], shared.nullbulk)) == NULL)
			return;
		addReplyBulkCString(c,strEncoding(o->encoding));
	}else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o)/1000);
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}



