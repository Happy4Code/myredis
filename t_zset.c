#include "redis.h"

static int zslLexValueGteMin(robj *value, zlexrangespec *spec);
static int zslLexValueLteMax(robj *value, zlexrangespec *spec);

/**
 * Create a node has n levels, the node object is obj and the 
 * score is score
 */
zskiplistNode *zslCreateNode(int level, double score, robj *obj){
	
	//allocate the mem space
	zskiplistNode *zn = zmalloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));

	zn->score = score;
	zn->obj = obj;
	return zn;
}

zskiplist *zslCreate(void){
	int j;
	zskiplist *zsl;

	//allocate mem space
	zsl = zmalloc(sizeof(*zsl));

	//set height and init level
	zsl->level = 1;
	zsl->length = 0;

	//init the head node
	zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, NULL);
	for(j = 0; j < ZSKIPLIST_MAXLEVEL; j++){
		zsl->header->level[j].forward = NULL;
		zsl->header->level[j].span = 0;
	}
	zsl->header->backward = NULL;

	//set the tail
	zsl->tail = NULL;

	return zsl;
}

/**
 * Free given skiplist node
 */
void zslFreeNode(zskiplistNode *node){
	decrRefCount(node->obj);
	zfree(node);	
}

/**
 * Free the whole skiplist and all nodes inside
 */
void zslFree(zskiplist *zsl){
	zskiplistNode *next, *node = zsl->header->level[0].forward;
	
	//Free the header
	zfree(zsl->header);

	//Release all nodes
	while(node){
		next = node->level[0].forward;
		zslFreeNode(node);
		node = next;
	}
	zfree(zsl);
}

/**
 * Returns a random level for the new skiplist node we are going to create
 *
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL,
 * with a powerlaw-alike distribution where higher levles are less likely
 * to be returned.
 */
int zslRandomLevel(void){
	int level = 1;
	while((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
		level++;
	return level > ZSKIPLIST_MAXLEVEL ? ZSKIPLIST_MAXLEVEL : level;
}

/**
 * Create a skiplistnode which has obj and score as given value,then
 * insert this into the skiplist zsl;
 * 
 * Return value: the new skiplistNode
 *
 * T_wrost = O(n^2) T_avg = O(NlogN)
 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	unsigned int rank[ZSKIPLIST_MAXLEVEL];
	int i, level;

	//Look for insert position in each level
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		/**
		 * store rank that is crossed to reach the insert position
		 */
		rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];

		//Move forward
		while(x->level[i].forward &&
			(x->level[i].forward->score < score ||
			(x->level[i].forward->score == score && 
			 compareStringObject(x->level[i].forward->obj, obj) < 0))){
			rank[i] += x->level[i].span;
			x = x->level[i].forward;
		}
		update[i] = x;
	}

	/**
	 * we assume the key is not already inside, since we allow duplicated
	 * scores, and the re-insertion of score adn redis object should never
	 * happen since the caller should test in the hash table if the element
	 * is already inside or not.
	 */
	level = zslRandomLevel();

	/**
	 * If the new generate level is greater than other node.
	 * Then initialize the unused levle in skiplist head node
	 * ,also record them in the update array.
	 */
	if(level > zsl->level){
		for(i = zsl->level; i < level; i++){
			rank[i]= 0;
			update[i] = zsl->header;
			update[i]->level[i].span = zsl->length;
		}
		
		zsl->level = level;
	}
	x = zslCreateNode(level, score, obj);
	
	// Update the previous node to points to the new node
	for(i = 0; i < level; i++){
		//This part is much like the you want to insert something
		//into a linklist, you first update the inserted-node point
		//to the next node of the previous node, then update the previous
		//node to point to the inserted node.The different point is
		//the skiplist has mutiple-level.
		x->level[i].forward = update[i]->level[i].forward;
		update[i]->level[i].forward = x;

		//update span covered by update[i] as x is inserted here.
		x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

		update[i]->level[i].span = (rank[0] - rank[i]) + 1;
	}	

	for(i = level; i < zsl->level; i++){
		update[i]->level[i].span++;
	}

	//set the backward pointer for the new node
	x->backward = (update[0] == zsl->header) ? NULL : update[0];
	if(x->level[0].forward)
		x->level[0].forward->backward = x;
	else
		zsl->tail = x;

	zsl->length++;
	
	return x;
}

/**
 * Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank
 * T = O(1)
 * Note in this function update variable is an array of zskiplistNode pointer.
 * Which is the node before the x.
 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update){
	int i;
	for(i = 0; i < zsl->level; i++){
		if(update[i]->level[i].forward == x){
			update[i]->level[i].span += x->level[i].span - 1;
			update[i]->level[i].forward = x->level[i].forward;
		}else{
			update[i]->level[i].span -= 1;
		}
	}

	//update the head and tail pointer
	if(x->level[0].forward){
		x->level[0].forward->backward = x->backward;
	}else{
		zsl->tail = x->backward;
	}
	//update the level and length varaible if necessary
	while(zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL){
		zsl->level--;
	}
	zsl->length--;

}

/**
 * Delete an element with matching score/object from the skiplist
 * T_wrose = O(N^2) T_avg = O(NlogN)
 * If success return 1, otherwise returns 0.
 */
int zslDelete(zskiplist *zsl, double score, robj *obj){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	int i;
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward && 
		      (x->level[i].forward->score < score || 
		      (x->level[i].forward->score == score && 
		       compareStringObject(x->level[i].forward->obj, obj) < 0))){
			x = x->level[i].forward;
		}
		update[i] = x;
	}
	x = x->level[0].forward;
	/**
	 * Delete them only when the score and object is the same as
	 * the require node.
	 */
	if(x && x->score == score && equalStringObject(x->obj, obj)){
		zslDeleteNode(zsl, x, update);
		zslFreeNode(x);
		return 1;
	}else{
		return 0;
	}

	return 0;
}

/**
 * check if given value is greater than the spec range min element
 */
static int zslValueGteMin(double value, zrangespec *spec){
	return spec->minex ? (value > spec->min) : (value >= spec->min);
}

static int zslValueLteMax(double value, zrangespec *spec){
	return spec->maxex ? (value< spec->max) : (value <= spec->max);
}

/**
 * Returns if there is part of the zset is in the range
 */
int zslInRange(zskiplist *zsl, zrangespec *range){
	zskiplistNode *x;

	//Exclude for the bad input 
	if(range->min > range->max || 
	  (range->min == range->max && (range->minex || range->maxex)))
		return 0;

	//check the max element
	x = zsl->tail;
	if(x == NULL || !zslValueGteMin(x->score, range))
		return 0;

	//check the min element
	x = zsl->header;
	if(x == NULL || !zslValueLteMax(x->score, range))
		return 0;
	return 1;
}

/**
 * Find the first node that is contained in the specific range.
 *
 * Return NULL when no element is contained in the range
 *
 * T_wrose = O(N) T_avg = O(logN)
 * */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range){
	int i;
	zskiplistNode *x;
	//Check if it is a valid range
	if(!zslInRange(zsl, range)) return NULL;
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward &&
		      !zslValueGteMin(x->level[i].forward->score, range))
			x = x->level[i].forward;
	}

	/**
	 * Because we already check the zsl is in the range, 
	 * so the x->level[0].forward is always exist
	 */
	x = x->level[0].forward;
	
	//Check to see if score <= max
	if(!zslValueLteMax(x->score, range)) return NULL;
	return x;
}

/**
 * Find the last node that is contained in the specific range
 * Return NULL when no elements is contained in the range.
 *
 * When no element is contained in the range return NULL
 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range){
	int i;
	zskiplistNode *x;

	if(!zslInRange(zsl, range)) return NULL;
	x= zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward && 
		      zslValueLteMax(x->level[i].forward->score, range))
			x = x->level[i].forward;
	}
	if(!zslValueGteMin(x->score, range)) return NULL;
	return x;
}	

/**
 * Delete all the elements with score between min and max from the skiplist.
 *
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 *
 * Note this function takes the reference to hash table view of the sorted set
 * , in order to remove the elements from the hash table too.
 *
 * T = O(N)
 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	unsigned long removed = 0;
	int i ;
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward && (
		      range->minex ?
		      x->level[i].forward->score <= range->min : 
		      x->level[i].forward->score < range->min))
			x = x->level[i].forward;
		update[i] = x;
	}
	//The loop stop at the element which next is greater or greater equals than
	//the range min
	x = x->level[0].forward;
	while(x && (range->maxex ? x->score < range->max : x->score <= range->max )){
		zskiplistNode *next = x->level[0].forward;
		
		//delete the current node from the skiplist
		zslDeleteNode(zsl, x, update);
		//delete this node from the dict
		dictDelete(dict, x->obj);	
		//free current skiplist node
		zslFreeNode(x);
		removed++;

		x = next;
	}
	return removed;
}

unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	unsigned long removed = 0;
	int i;

	x = zsl->header;
	for(i = zsl->level -1; i >=0; i--){
		while(x->level[i].forward && 
		      !zslLexValueGteMin(x->level[i].forward->obj, range))
				x = x->level[i].forward;
		update[i] = x;
	}

	x = x->level[0].forward;
	while(x && zslLexValueLteMax(x->obj, range)){
		zskiplistNode *next = x->level[0].forward;

		zslDeleteNode(zsl, x, update);

		dictDelete(dict, x->obj);

		zslFreeNode(x);

		removed++;

		x = next;
	}

	return removed;
}

/**
 * Delete all the elements with rank between start and end from the skiplist
 *
 * Start and end are inclusive.Note that start adn end need to be 1-based.
 *
 * T = O(N)
 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned end, dict *dict){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	unsigned long tranversed = 0, removed = 0;
	int i;

	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward && (tranversed + x->level[i].span < start)){
			tranversed += x->level[i].span;
			x = x->level[i].forward;
		}
		update[i] = x;
	}
	tranversed++;
	x = x->level[0].forward;
	//Delete all the node in the given rank
	while(x && tranversed <= end){
		zskiplistNode *next = x->level[0].forward;
		zskDeleteNode(zsl, x, update);
		dictDelete(dict, x->obj);
		zslFreeNode(x);
		x = next;
		removed++;
		tranversed++;
	}
}

/**
 * Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element.
 *
 * T_worse = O(N) T_avg = O(logN)
 */
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o){
	zskiplistNode *x;
	int i;
	unsigned long rank = 0;
	
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward &&
		     (x->level[i].forward->score < score || 
		     (compareStringObjects(x->level[i].forward->obj, o) <= 0))){
			rank += x->level[i].span;
			x = x->level[i].forward;
		}
		if(x-> obj && equalsStringObject(x->obj, o))
			return rank;
		return 0;
	}
}

/* Finds an element by its rank. The rank argument needs to be 1-based. 
 * 
 * 根据排位在跳跃表中查找元素。排位的起始值为 1 。
 *
 * 成功查找返回相应的跳跃表节点，没找到则返回 NULL 。
 *
 * T_wrost = O(N), T_avg = O(log N)
 */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    // T_wrost = O(N), T_avg = O(log N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {

        // 遍历跳跃表并累积越过的节点数量
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }

        // 如果越过的节点数量已经等于 rank
        // 那么说明已经到达要找的节点
        if (traversed == rank) {
            return x;
        }

    }

    // 没找到目标节点
    return NULL;
}

/* Populate the rangespec according to the objects min and max. 
 *
 * 对 min 和 max 进行分析，并将区间的值保存在 spec 中。
 *
 * 分析成功返回 REDIS_OK ，分析出错导致失败返回 REDIS_ERR 。
 *
 * T = O(N)
 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;

    // 默认为闭区间
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min->encoding == REDIS_ENCODING_INT) {
        // min 的值为整数，开区间
        spec->min = (long)min->ptr;
    } else {
        // min 对象为字符串，分析 min 的值并决定区间
        if (((char*)min->ptr)[0] == '(') {
            // T = O(N)
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
            spec->minex = 1;
        } else {
            // T = O(N)
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
        }
    }

    if (max->encoding == REDIS_ENCODING_INT) {
        // max 的值为整数，开区间
        spec->max = (long)max->ptr;
    } else {
        // max 对象为字符串，分析 max 的值并决定区间
        if (((char*)max->ptr)[0] == '(') {
            // T = O(N)
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
            spec->maxex = 1;
        } else {
            // T = O(N)
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

/*-----------------------------------------Lexicographic ranges-------------------------------------------*/
/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparision, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. REDIS_OK will be
  * returned.
  *
  * If the string is not a valid range REDIS_ERR is returned, and the value
  * of *dest and *ex is undefined. */
int zslParseLexRangeItem(robj *item, robj **dest, int *ex){
	char *c = item->ptr;

	switch(c[0]){
		case '+':
			if(c[1] != '\0') return REDIS_ERR;
			*ex = 0;
			*dest = shared.maxstring;
			incrRefCount(shared.maxstring);
			return REDIS_OK;
		case '-':
			if(c[1] != '\0') return REDIS_ERR;
			*ex = 0;
			*dest = shared.minstring;
			incrRefCount(shared.minstring);
			return REDIS_OK;
		case '(':
			if(c[1] != '\0') return REDIS_ERR;
			*ex = 1;
			*dest = createStringObject(c+1, sdslen(c) - 1);
			incrRefCount(shared.minstring);
			return REDIS_OK;
		case '[':
			*ex = 0;
			*dest = createStringObject(c+1, sdslen(c) - 1);
			return REDIS_OK;
		default:
			return REDIS_ERR;
	}
}

/**
 * Populate the rangespec accoding to the objects min and max.
 * 
 * Return REDIS_OK on success. On error REDIS_ERR is returned.
 * When REDIS_OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed.
 */ 
static int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec){
	/**The range can't be valid if objects are integer encoded.
	 * Every item must start with ( or [
	 */ 
	if(min->encoding == REDIS_ENCODING_INT || 
	   max->encoding == REDIS_ENCODING_INT) return REDIS_ERR;

	spec->min = spec->max = NULL;
	if(zslParseLexRangeItem(min, &spec->min, &spec->minex) == REDIS_ERR ||
	   zslParseLexRangeItem(max, &spec->max, &spec->maxex) == REDIS_ERR){
		   //If one success and another failed, we just decrRef the success one.
		   if(spec->min) decrRefCount(spec->min);
		   if(spec->max) decrRefCount(spec->max);
	}else{
		return REDIS_OK;
	}
}

/**
 * Free a lex range structure, must be called only after zslParseLexRange()
 * populated the structure with success(REDIS_OK returned).
 */ 
void zslFreeLexRange(zlexrangespec *spec){
	decrRefCount(spec->min);
	decrRefCount(spec->max);
}

/**
 * This is just a wrapper function to compareStringObjects() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of -inf abd
 * +inf for strings.
 */ 
int compareStringObjectsForLexRange(robj *a, robj *b){
	if(a == b) return 0;

	if(a == shared.minstring || b = shared.maxstring) return -1;
	if(a == shared.maxstring || b = shared.minstring) return 1;
	return compareStringObjects(a, b);
}

//Check the current value is greater or greater equals(which depeneds
//on the spec->minex value) to the spec->max
static int zslLexValueGteMin(robj *value, zlexrangespec *spec){
	return spec->minex ? 
		(compareStringObjectsForLexRange(value, spec->min) > 0) :
		(compareStringObjectsForLexRange(value, spec->min) >= 0);
}

static int zslLexValueLteMax(robj *value, zlexrangespec *spec){
	return spec->maxex ? 
	(compareStringObjectsForLexRange(value, spec->max) < 0):
	(compareStringObjectsForLexRange(value, spec->max) <= 0);
}

/**
 * Returns if there is a part of the zset is in the lex range
 */ 
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range){
	zskiplistNode *x;

	/*Validate the input range.*/
	if(compareStringObjectsForLexRange(range->min, range->max) > 0 || 
	   (compareStringObjects(range->min, range->max) == 0 && 
	   (range->minex || range->maxex)))
		return 0;
	x = zsl->tail;
	if(x == NULL || !zslLexValueGteMin(x->obj, range)) return 0;

	x= zsl->header->level[0].forward;
	if(x == NULL || !zslLexValueLteMax(x->obj, range)) return 0;

	return 1;
}

/**
 * Find the first node thatis contained in the specified lex range.
 * Returns NULL when no element in contained in the range.
 */ 
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range){
	zskiplistNode *x;
	int i;
	if(!zslIsInLexRange(zsl, range) == 0) return NULL;
	
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward &&
		      !zslLexValueGteMin(x->level[i].forward->obj, range))
			  x = x->level[i].forward;
	}
	x = x->level[0].forward;
	//This is an inner zskiplistNode, x can not be null
	redisAssert(x != NULL);

	if(!zslLexValueLteMax(x->obj, range)) return NULL;
	return x;
}

/**
 * Find the last node that is contained in the specified range.
 * Returns NULL when no elment is contained in the range.
 */ 
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range){
	zskiplistNode *x;
	int i;

	if(zslIsInLexRange(zsl, range) == 0) return NULL;

	x = zsl->header;

	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward &&
		      !zslLexValueLteMax(x->level[i].forward->obj, range))
			  x = x->level[i].forward;
	}

	/*This is an inner range, so this node can not be NULL*/

	if(!zslLexValueGteMin(x->obj, range)) return NULL;

	return x;
}

/**************************************************************************
 * Ziplist-backed sorted set API                                          *
 **************************************************************************/ 
/**
 * get the sptr pointer pointed score
 */ 
double zzlGetScore(unsigned char *sptr){
	unsigned char *vstr;
	unsigned int vlen;
	long long vlong;
	char buf[128];
	double score;

	redisAssert(sptr != NULL);

	//Get the value from the ziplist
	redisAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));

	if(vstr){
		memcpy(buf, vstr, vlen);
		buf[vlen] = '\0';
		score = strtod(buf, NULL);
	}else{
		//Convert it to a double.
		score = vlong;
	}
	return score;
}

/* Return a ziplist element as a Redis string object.
 * This sample abstraction can be used to simplifies some code at 
 * the cost of some performance.
 */
robj *ziplistGetObject(unsigned char *sptr){
	unsigned char *vstr;
	unsigned int vlen;
	long long vlong;

	redisAssert(sptr != NULL);
	redisAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));
	if(vstr){
		return createStringObject((char *)vstr, vlen);
	}else{
		return createStringObjectFromLongLong(vlong);
	}
}  

/* Compare element in sorted set with given element.
 * compare element of the eptr to the string cstr.
 * 
 * If equals, return 0;
 * If eptr elements greater than cstr elements, returns positive number;
 * otherwise negative number;
 */ 
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen){
	unsigned char *vstr;
	unsigned int vlen;
	long long vlong;
	unsigned char vbuf[128];
	int minlen, cmp;

	redisAssert(eptr != NULL);
	redisAssert(ziplistGet(eptr, &vstr, &vlen, &vlong));

	if(vstr == NULL){
		vlen = ll2string((char*)vbuf, sizeof(vbuf), vlong);
		vstr = vbuf;
	}
	minlen = (vlen < clen) ? vlen : clen;
	cmp = memcmp(vbuf, cstr, minlen);
	if(cmp == 0) return vlen - clen;
	return cmp;
}

/**
 * Get the number of elements from the ziplist
 */ 
unsigned int zzlLength(unsigned char *zl){
	return ziplistLen(zl)/2;
}

/* Move to the next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry.
 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr){
	unsigned char *_eptr, *_sptr;

	redisAssert(*eptr != NULL && *sptr != NULL);

	if((_eptr = ziplistNext(zl, *sptr)) != NULL){
		_sptr = ziplistNext(zl, _eptr);
		redisAssert(_sptr != NULL);
	}else{
		_sptr = NULL;
	}
	*eptr = _eptr;
	*sptr = _sptr;
} 

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry.
 */ 
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr){
	unsigned char *_eptr, *_sptr;
	
	redisAssert(*eptr != NULL && *sptr != NULL);

	if((_sptr = ziplistPrev(zl, *eptr)) != NULL){
		_eptr = ziplistPrev(zl, _sptr);
		redisAssert(_eptr != NULL);
	}else{
		_eptr = NULL;
	}
	*eptr = _eptr;
	*sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used internally
 * by zzlFirstInRange and zzlLastInRange
 */ 
int zzlIsInRange(unsigned char *zl, zrangespec *range){
	unsigned char *p;
	double score;

	//Check the range
	if(range->min < range->max || 
	  (range->min == range->max && (range->minex || range->maxex))) return 0;

	//Get the last score(greatest score node);
	p = ziplistIndex(zl, -1);

	//If this ziplist is empty
	if(p == NULL) return 0;

	score = zzlGetScore(p);
	if(!zslValueGteMin(score, range)) return 0;

	//Get the first element(smallest one) in the skiplist
	p = ziplistIndex(zl, 1);
	redisAssert(p != NULL);
	score = zzlGetScore(p);
	if(!zslValueLteMax(score, range)) return 0;

	return 1;
}

/**
 * Find pointer to the first element contained in the specific range.
 * 
 * Returns NULL when no element is contained in the range.
 */ 
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range){
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
	double score;

	if(!zzlIsInRange(zl, range)) return NULL;

	while(eptr != NULL){
		sptr = ziplistNext(zl, eptr);
		redisAssert(sptr != NULL);
		score = zzlGetScore(sptr);
		if(zslValueGteMin(score, range)){
			if(zslValueLteMax(score, range)) 
				return eptr;
			return NULL;
		}

		eptr = ziplistNext(zl, sptr);
	}	
	return NULL;
}

/* Find pointer to the last element contained in the specific range.
 * Returns NULL when no element is contained in the range.
 */ 
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range){
	unsigned char *eptr = ziplistIndex(zl, -2), *sptr;
	double score;

	if(!zzlIsInRange(zl, range)) return NULL;

	while(eptr != NULL){
		sptr = ziplistNext(zl, eptr);
		redisAssert(sptr != NULL);
		
		score = zzlGetScore(sptr);
		if(zslValueLteMax(score, range)){
			if(zslValueGteMin(score, range)){
				return eptr;
			}
			return NULL;
		}
		sptr = ziplistPrev(zl, eptr);
		//We still have a previous elements
		if(sptr != NULL){
			redisAssert((eptr = ziplistPrev(zl, sptr)) != NULL);
		}else{
			eptr = NULL;
		}
	}
	return NULL;
}

static int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec){
	robj *value = ziplistGetObject(p);
	int ret = zslLexValueGteMin(value, spec);
	decrRefCount(value);
	return ret;
}


static int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec){
	robj *value = ziplistGetObject(p);
	int ret = zslLexValueLteMax(value, spec);
	decrRefCount(value);
	return ret;
}

/**
 * Returns if there is apart of the zset is in range. Should only be used internally
 * be zzlFirstInRange and zzlLastInRange.
 */ 
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range){
	unsigned char *p;

	/*Test for ranges that will always be empty*/
	if(compareStringObjectsForLexRange(range->min, range->max) > 0 ||
	  	(compareStringObjects(range->min, range->max) == 0 &&
	    (range->minex || range->maxex))) return 0;
	
	p = ziplistIndex(zl, -2);
	//This ziplist is empty
	if(p == NULL) return 0;
	if(!zzlLexValueLteMax(p, range)) return 0;

	p = ziplistIndex(zl, 0);
	redisAssert(p != NULL);
	if(!zzlLexValueGteMin(p, range)) return 0;
	return 1;
}

/**
 * Find pointer to the first element contained in the specific lex range.
 * Returns NULL when no element is contained in the range.
 */ 
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range){

	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

	if(!zzlIsInLexRange(zl, range)) return NULL;

	while(eptr != NULL){
		sptr = ziplistNext(zl, eptr);
		redisAssert(sptr != NULL);
		if(zzlLexValueGteMin(eptr, range)){
			if(zzlLexValueLteMax(eptr, range))
				return eptr;
			return NULL;
		}
		eptr = ziplistNext(zl, sptr);
	}
	return NULL;
}

unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range){
	unsigned char *eptr = ziplistIndex(zl, -2), *sptr;

	if(!zzlIsInLexRange(zl, range)) return NULL;

	while(eptr != NULL){
		if(zzlLexValueLteMax(eptr, range)){
			if(zzlLexValueGteMin(eptr, range)) 
				return eptr;
			return NULL;
		}
		sptr = ziplistPrev(zl, eptr);
		if(sptr != NULL){
			redisAssert((eptr = ziplistPrev(zl, sptr)));
		}else{
			//No more element
			eptr = NULL;
		}
	}
	return NULL;
}

/* From the ziplist find the ele member, and store it score into variable `score`.
 * If success return pointer points to ele, otherwise return NULL.
 */
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score){

	//Locate to the first element
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

	ele = getDecodedObject(ele);

	//Loop through the whole ziplist
	while(eptr != NULL){

		redisAssert((sptr = ziplistNext(zl, eptr)) != NULL);
		if(ziplistCompare(zl, ele->ptr, sdslen(ele->ptr))){
			if(score != NULL)
				*score = zzlGetScore(sptr);
			decrRefCount(ele);
			return eptr;
		}

		eptr = ziplistNext(zl, sptr);
	}
	decrRefCount(ele);
	return NULL;
}

/**
 * Delete(element, score) pair from ziplist. Use local copy of eptr because we don't
 * want to modify the one given as argument.
 */ 
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr){
	unsigned char *p = eptr;

	zl = ziplistDelete(zl, &p);
	zl = ziplistDelete(zl, &p);
	return zl;
}

/* Insert given member and score in front of the position eptr points
 * to.If the eptr is NULL, then insert it into the ziplist end.
 * 
 * The function return the `zl`.
 */ 
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score){
	unsigned char *sptr;
	char scorebuf[128];
	int scorelen;
	size_t offset;

	redisAssertWithInfo(NULL, ele, sdsEncodedObject(ele));
	scorelen = d2string(scorebuf, sizeof(scorebuf), score);

	//Insert into the tail
	if(eptr == NULL){
		//Insert data into the tail
		ziplistPush(zl, ele->ptr, sdslen(ele->ptr), ZIPLIST_TAIL);
		//Insert score into the tail
		ziplistPush(zl, (unsigned char *)scorebuf, scorelen, ZIPLIST_TAIL);
	}else{
		//Because the zl may changes when we insert something into the ziplist, 
		//but the relative position never changes. So we remeber the relative 
		//position in order to retrive a element.
		offset = eptr - zl;
		zl = ziplistInsert(zl, eptr, ele->ptr, sdslen(ele->ptr));
		eptr = zl + offset;

		//Insert the score after the element
		redisAssertWithInfo(NULL, ele, (sptr = ziplistNext(zl, eptr)) != NULL);
		zl = ziplistInsert(zl, sptr, (unsigned char *)scorebuf, scorelen); 
	}
	return zl;
}

/* Insert (element, score) pair in ziplist.
 * Put the ele member and score into the ziplist. 
 * ziplist ordered by score.
 * This function assumes the element is not ye present in the list.
 */ 
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score){
	
	unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
	double _score;
	ele = getDecodedObject(ele);

	while(eptr != NULL){
		redisAssert((sptr = ziplistNext(zl, eptr)) != NULL);
		_score = zzlGetScore(sptr);
		
		if(_score > score){
			/**
			 * It means we meet the first element that is bigger then 
			 * current node, we insert the node in front current position.
			 */ 
			zl = zzlInsertAt(zl, eptr, ele, score);
			break;
		}
		/* If the score is the same, then we try to compare the string 
		 * inside the node, to maintain the order.
		 */ 
		if(_score == score){
			if(zzlCompareElements(eptr, ele->ptr, sdslen(ele->ptr)) > 0){
				zl = zzlInsertAt(zl, eptr, ele, score);
				break;
			}
		}
		eptr = ziplistNext(zl, sptr);
	}
	if(eptr == NULL)
		zl = zzlInsertAt(zl, NULL, ele->ptr, sdslen(ele->ptr));
	decrRefCount(ele);
	return zl;
}

/* Delete ziplist score in the given range.
 * When the deleted is not empty, put the number of deleted elements in the *deleted.
 */ 
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted){
	unsigned char *eptr, *sptr;
	double score;
	unsigned long num = 0;

	// Do the init for the varaible.
	if(deleted) *deleted = 0;

	eptr = zzlFirstInRange(zl, range);
	//None of the element
	if(eptr == NULL) return zl;

	/* When the tail of the ziplist is deleted, eptr will point to the sentinel
	 * byte and ziplistNext will return NULL.
	 */ 
	while((sptr = ziplistNext(zl, eptr)) != NULL){
		score = zzlGetScore(sptr);
		if(zslValueLteMax(score, range)){
			zl = ziplistDelete(zl, &eptr);
			zl = ziplistDelete(zl, &eptr);
			num++;
		}else{
			break;
		}
	}
	if(deleted) *deleted = num;
	return zl;
}

unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted){
	unsigned char *eptr, *sptr;
	unsigned long num = 0;

	if(deleted) *deleted = 0;

	eptr = zzlFirstInLexRange(zl, range);
	if(eptr == NULL) return zl;

	while((sptr = ziplistNext(zl, eptr)) != NULL){
		if(zzlLexValueLteMax(eptr, range)){
			zl = ziplistDelete(zl, &eptr);
			zl = ziplistDelete(zl, &eptr);
			num++;
		}else{
			break;
		}
	}
	if(deleted) *deleted = num;
	return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based.
 * If deleted is not NULL, then put the deleted numbers in it.
 */ 
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted){
	unsigned int num = (end - start) + 1;
	if(deleted) *deleted = num;
	/* Each element occupy two node, so the start postion will need mutiply 2.
	 * Because the ziplist is 0 based, and zzl start with 1, so the start position
	 * is start - 1.
	 */ 
	zl = ziplistDeleteRange(zl, 2*(start - 1), 2 *num);
	return zl;
}

/*------------------------------------------------------------------------------
 * Common sorted set API
 *-----------------------------------------------------------------------------*/
unsigned int zsetLength(robj *robj){
	int length = -1;
	if(robj->encoding == REDIS_ENCODING_ZIPLIST){
		length =  zzlLength(robj->ptr);
	}else if(robj->encoding == REDIS_ENCODING_SKIPLIST){
		length = ((zset*)robj->ptr)->zsl->length;
	}else{
		redisPanic("unknown encoding");
	}
	return length;
}

/* Convert the skiplist underlying encoding
 * to the `encoding`.
 */
void zsetConvert(robj *zobj, int encoding){
	zset *zs;
	zskiplistNode *node, *next;
	robj *ele;
	double score;

	//If current encoding is the same as the expected encoding, do nothing.
	if(zobj->encoding == encoding) return;	

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int len;
		long long vlong;

		if(encoding == REDIS_ENCODING_SKIPLIST) redisPanic("Unknown target type");

		//Create a set object
		zs = zmalloc(sizeof(*zs));
		//Create a dictionary
		zs->dict = dictCreate(&zsetDictType, NULL);
		//Create a zskiplist
		zs->zsl = zslCreate();
		//Points to the first element of the ziplist
		eptr = ziplistIndex(zl, 0);
		redisAssertWithInfo(NULL, zobj, eptr != NULL);

		sptr = ziplistNext(zl, eptr);
		redisAssertWithInfo(NULL, zobj, sptr != NULL);
		//Loop through the ziplist
		while(eptr != NULL){
			score = zzlGetScore(sptr);
			ziplistGet(zl, &vstr, &len, &vlong);
			if(vstr){
				ele = createStringObject((char *)vstr, len);
			}else{
				ele = createStringObjectFromLongLong(vlong);
			}

			node = zslInsert(zs->zsl, score, ele);
			redisAssertWithInfo(NULL, zobj, dictAdd(zs->dict, ele, &node->score) == DICT_OK);
			//This place u may have puzzle, why we incrRefCount agin, because the ele is referenced
			//in two places, 1 in the ziplist, 2 in the dictionary, when the ele is created, its
			//refcount is 1, so we only need to incremented it once.
			incrRefCount(ele);
			zzlNext(zl, &eptr, &sptr);
		}

		zfree(zobj->ptr);

		zobj->ptr = zs;
		zobj->encoding = REDIS_ENCODING_SKIPLIST;

	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){

		//Create a new ziplist
		unsigned char *zl = ziplistNew();

		if(encoding != REDIS_ENCODING_ZIPLIST) redisPanic("Unknown target type");

		zs = zobj->ptr;

		//We first release the dictionary, because in the following steps we only need
		//to loop through the skiplist
		dictRelease(zs->dict);

		node = zs->zsl->header->level[0].forward;

		//Free the header of the skiplist
		zfree(zs->zsl->header);
		//After free the header of the skiplist, we can free the zskiplist structure.
		//If the order reverse, we may never free the zls->header.
		zfree(zs->zsl);

		while(node){
			ele = getDecodedObject(node->obj);

			zl = zzlInsertAt(zl, NULL, ele, node->score);
			decrRefCount(ele);

			next = node->level[0].forward;
			zslFreeNode(node);
			node = next;
		}

		zfree(zs);

		zobj->ptr = zl;
		zobj->encoding = REDIS_ENCODING_ZIPLIST;
	}else {
        redisPanic("Unknown sorted set encoding");
    }
}

/*-------------------------------------------------------------------------------------
 * Sorted set commands
 *------------------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY*/
void zaddGenericCommand(redisClient *c, int incr){
	
	static char *nanerr = "resulting score is not a number (NaN)";

	robj *key = c->argv[1];
	robj *ele;
	robj *zobj;
	robj *curobj;
	double score, *scores = NULL, curscore = 0.0;
	int j, elements = (c->argc - 2)/2;	
	int added = 0, updated = 0;

	//Check the number of score and element is paired.
	if(c->argc % 2 != 0){
		addReply(c, shared.syntaxerr);
		return;
	}

	/* Start parsing all the scores, we need to emit any synatx error before
	 * execute additions to the sorted set, as the command should either execute
	 * fully or nothing at all.
	 */
	scores = zmalloc(sizeof(double) * elements);
	for(j = 0; j < elements; j++){
		if(getDoubleFromObjectOrReply(c, c->argv[2 + 2*j], &scores[j], NULL) != REDIS_OK)
			goto  cleanup;
	}

	//Get the zset object
	zobj = lookupKeyWrite(c->db, key);
	//If the zset is not existed, we may create a new zobj accoring to the type.
	if(zobj == NULL){
		//If we need to create a zset which encoding is ziplist
		if(server.zset_max_ziplist_entries == 0 ||
		   server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
			zobj = createZsetObject();
		else
			zobj = createZsetZiplistObject();
		//Added to the databse
		dbAdd(key, zobj);
	}else{
		//If this zobj is existed, we checked the type.
		if(checkType(c, zobj, REDIS_ZSET)) goto cleanup;
	}

	for(j = 0; j < elements; j++){
		score = scores[j];

		if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
			unsigned char *eptr;
			ele = c->argv[3 + 2 * j];
			//If we can find this element in this ziplist
			if((eptr = zzlFind(zl, ele, &curscore)) != NULL){
				
				//If it is a incr operation
				if(incr){
					score += curscore;
					if(isnan(score)){
						addReplyError(c, nanerr);
						goto cleanup;
					}
				}

				if(score != curscore){
					zobj->ptr = zzlDelete(zobj->ptr, eptr);
					zobj->ptr = zzlInsert(zobj->ptr, ele, score);
					server.dirty++;
					//Update the update number of element
					updated++;
				}
			}else{
				//If this element is not existed, then we insert them directly,
				//because the zzlInsert already handle the operation to put them into
				//the right position.
				zobj->ptr = zzlInsert(zobj->ptr, ele, score);
				//Check is we have exceed the limit of the ziplist implements
				if(zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
					zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
				
				//Check if the size of single element exceed the limit of ziplist
				if(sdslen(ele->ptr) > server.zset_max_ziplist_value)
					zsetConvert(zobj, REDIS_ENCODING_SKIPLIST);
				server.dirty++;
				added++;
			}
		}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
			zset *zs = zobj->ptr;
			zskiplistNode *znode;
			dictEntry *de;
			ele = tryObjectEncoding(c->argv[3 + j*2]);

			de = dictFind(zs->dict, ele);
			//Check if the element is existed.
			if(de != NULL){
				curobj = dictGetKey(de);
				curscore = *(double*)dictGetVal(de);

				if(incr){
					score += curscore;
					if(isnan(score)){
						addReplyError(c, nanerr);
						goto cleanup;
					}
				}
				/* Remove and re-insert when score changed. We can safely
				 * deleted the key object from the skiplist, since the dictionary
				 * still has a reference to it.
				 */ 
				if(curscore != score){
					redisAssertWithInfo(c, curobj, zslDelete(zs->zsl, curscore, curobj));
					//Re-insert the element into the zskiplist
					znode = zslInsert(zs->zsl, score, curobj);
					//This is because the zslDelete function will decrRefCount for curobj.
					incrRefCount(curobj);

					//Update the value of the dictEntry, hints here is a detail, we need to
					//pay attention.We update the de->val to the address of znode->score, rather
					//&score, it is really important , because score is auto-variable, after this function
					//finish the memory space which is previously used to hold score will use to save other variable.
					dictGetVal(de) = &znode->score;

					server.dirty++;
					updated++;
				}
			}else{
				znode = zslInsert(zs->zsl, score, ele);
				incrRefCount(ele);
				redisAssertWithInfo(c, NULL, dictAdd(zs->dict, ele, &znode->score) == DICT_OK);
				incrRefCount(ele);

				server.dirty++;
				added++;
			}

		}else{
			redisPanic("Unknown sorted set encoding");
		}
	}

	if(incr)
		addReplyDouble(c, score);
	else
		addReplyLongLong(c, added);
	
	cleanup : 
		zfree(scores);
		if(added || updated){
			signalModifiedKey(c->db, key);
			notifyKeyspaceEvent(REDIS_NOTIFY_ZSET, 
			incr ? "zincr" : "zadd", key, c->db->id);
		} 
}

void zaddCommand(){
	zaddGenericCommand(c, 0);
}

void zincrbyCommand(){
	zaddGenericCommand(c, 1);
}

void zremCommand(redisClient *c){
	robj *key = c->argv[1];
	robj *zobj;
	int deleted = 0, keyremoved = 0; j;

	if((zobj = lookupKeyWriteOrReply(c->db, key, shared.czero)) == NULL ||
		checkType(c, zobj, REDIS_ZSET)) return;

	for(j = 2; j < c->argc; j++){
		if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
			unsigned char *eptr;
			if((eptr = zzlFind(zobj->ptr, c->argv[j], NULL)) != NULL){
				zobj->ptr= zzlDelete(zobj->ptr, eptr);
				deleted++;

				//If the ziplist is empty then we delete it
				if(zzlLength(zobj->ptr)){
					dbDelete(c->db, key);
					keyremoved = 1;
					break;
				}
			}
		}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
			zset *zs = zobj->ptr;
			dict *d = zs->dict;
			dictEntry *de;
			double score;
			/* 你可能会感到好奇 为什么在前面的zadd 操作时，进行dictFind时，我们操作的c->argv[j]是一个进行了tryObjectEncoding 的对象，
			 * 而在这个地方我们调用dictFind的对象只是一个普通的c->argv[j]对象， 事实上对象是否经过tryObjectEncoding并不会影响该对象是能被
			 * dictFind正确的找到，之所以上面zadd针对c->argv[j]进行tryObjectEncoding 是因为要对c->argv[j]中的内容进行存储，而采用zskiplist
			 * 存储时，为了进一步节省空间，会对要存储的元素进行 tryObjectEncoding.
			 */ 
			de = dictFind(d, c->argv[j]);
			score = *(double*)dictGetVal(de);
			if(de != NULL){
				 dictDelete(d, c->argv[j]);

				 redisAssertWithInfo(c,c->argv[j],zslDelete(zs->zsl,score,c->argv[j]));

				 //Check if we need resize the dictionary
				 if(htNeedsResize(dict)) dictResize(dict);

				 if(dictSize(dict) == 0){
					 dbDelete(c->db, key);
					 keyremoved = 1;
					 break;
				 }
			}
		}else{
			redisPanic("Unknown sorted set encoding");
		}

		if(deleted){
			notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
	        signalModifiedKey(c->db,key);
    	    server.dirty += deleted;
		}

		addReplyLongLong(c, deleted);
	}
}

/* Implements ZREMRANGEBYRANK REMRANGEBYSCORE ZREMRANGEBYLEX commands
 */ 
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2

void zremrangeGenericCommand(redisClient *c, int rangeType){
	robj *key = c->argv[1];
	robj *zobj;
	int keyremoved = 0;
	unsigned long deleted;
	zrangespec range;
	zlexrangespec lexrange;
	long start, end, llen;

	if(rangeType == ZRANGE_RANK){
		if((getLongLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK)) || 
		   (getLongLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) 
		return;
	}else if(rangeType == ZRANGE_SCORE){
		if(zslParseRange(c->argv[2], c->argv[3], range) != REDIS_OK){
			addReplyError(c, "min or max is not a float");
			return;
		}
	}else if(rangeType == ZRANGE_LEX){
		if(zslParseLexRange(c->argv[2], c->argv[3], &lexrange) != REDIS_OK){
			addReplyError(c, "min or max is not valid string range item");
			return;
		}
	}

	if((zobj = lookupKeyWrite(c, key, shared.czero)) == NULL ||
	   checkType(c, zobj, REDIS_ZSET)) goto cleanup;

	if(rangeType == ZRANGE_RANK){
		llen = zsetLength(zobj);
		if(start < 0) start += llen;
		if(end < 0) end += llen;
		if(start < 0) start = 0;

		if(start > end || start >= llen) {
			addReply(c, shared.czero);
			goto cleanup;
		}

		if(end >= llen) end = llen - 1;
	}

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		switch (rangetype)
		{
			case ZRANGE_RANK:
				zobj->ptr = zzlDeleteRangeByRank(zobj->ptr, start+1, end+1, &deleted);
				break;
			case ZRANGE_SCORE:
				zobj->ptr = zzlDeleteRangeByScore(zobj->ptr, range, &delete);
				break;
			case ZRANGE_LEX:
				zobj->ptr = zzlDeleteRangeByLex(zobj->ptr, lexrange, &deleted);
				break;
		}
		if(zzlLength(zobj->ptr) == 0){
			dbDelete(c->db, key);
			keyremoved = 1;
		}
	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset zs = zobj->ptr;
		switch (rangetype)
		{
			case ZRANGE_RANK:
				deleted = zslDeleteRangeByRank(zs->zsl, start+1, end+1, zs->dict);
				break;
			case ZRANGE_SCORE:
				deleted = zslDeleteRangeByScore(zs->zsl, range, dict);
				break;
			case ZRANGE_LEX:
				deleted = zslDeleteRangeByLex(zs->zsl, lexrange, dict);
				break;
		}

		if(htNeedsResize(zs->dict)) dictResize(zs->dict);

		if(dictSize(dict) == 0){
			dbDelete(c->db, key);
			keyremoved = 1;
		}
	}else{
		redisPanic("unknown type of zset");
	}

	if(deleted){
		char *event[3] = {"zremrangebyrank", "zremrangebyscore", "zremrangebylex"};
		signalModifiedKey(c->db, key);
		notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,event[rangetype],key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",key,c->db->id);
	}

	server.dirty += deleted;
    // 回复被删除元素的个数
    addReplyLongLong(c,deleted);

	cleanup :
		if(rangeType == ZRANGE_LEX) zslFreeLexRange(lexrange);
}

void zremrangebyrankCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

void zremrangebyscoreCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

void zremrangebylexCommand(redisClient *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

/**
 * The polymorphic iterator.
 */ 
typedef struct{

	//the object being iterate
	robj *subject;
	
	//the type of object
	int type;

	//encoding
	int encoding;

	//weight
	double weight;

	union{
		union _iterset{
			//intset iterators
			struct {
				intset *is;

				int ii;
			}is;
			//dict iterator
			struct{
				dict *dict;

				dictIterator *di;

				dictEntry *de;
			}ht;
		}set;

		//Sorted set iterators
		union _iterzset{
			//ziplist iterator
			struct {
				unsigned char *zl;

				unsigned char *eptr, *sptr;
			}zl;

			//zset iterator
			struct{
				zset *zs;

				zskiplistNode *node;
			}sl;
		}zset;
	}iter;
}zsetopsrc;

/* Use dirty flags for pointers that need to be cleaned up in the next iteration
 * over the zsetopval.
 * 
 * The dirty flag for the long long value is special, since 
 * long long values don't need cleanup.
 * 
 * Instead, it means that we already checked that "ell" holds a long long,
 * or tried to convert another representation into a long long value.
 * 
* When this was successful, OPVAL_VALID_LL is set as well. 
 */ 
#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/**
 * Store value retrieved from the iterator
 */ 
typedef struct{
	
	int flags;

	unsigned char _buf[32]; //Private buffer

	robj *ele;
	unsigned char *estr;
	unisgned int elen;
	long long ell;

	double score;
}zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

/*
 * Init the iterator
 */ 
void zuiInitIterator(zsetopsrc *op){

	//If this subject is NULL, do nothing.
	if(op->subject == NULL) return;

	if(op->type == REDIS_SET){
		iterset *it = &op->iter.set;
		if(op->encoding == REDIS_ENCODING_INTSET){
			it->is.ii = 0;
			it->is.is = op->subject->ptr;
		}else if(op->encoding == REDIS_ENCODING_HT){
			it->ht.dict = op->subject->ptr;
			it->ht.di = dictGetIterator(op->subject->ptr);
			it->ht.de = dictNext(it->ht.di);
		}else{
			redisPanic("Unknown set encoding");
		}
	}else if(op->type == REDIS_ZSET){
		iterzset *it = &op->iter.zset;
		if(op->encoding == REDIS_ENCODING_ZIPLIST){
			it->zl.zl = op->subject->ptr;
			it->zl.eptr = ziplistIndex(it->zl.zl, 0);
			if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
            	redisAssert(it->zl.sptr != NULL);
            }
		}else if(op->encoding == REDIS_ENCODING_SKIPLIST){
			it->sl.zs = op->subject->ptr;
			it->sl.node = it->sl.zs->zsl->header->level[0].forward;
		}else{
			redisPanic("Unknown zset encoding");
		}
	}else{
		redisPanic("Unsupported type");
	}
}

/**
 * clear this iterator.
 */ 
void zuiClearIterator(zsetopsrc *op){

	if(op->subject == NULL){
		return;
	}

	if(op->type == REDIS_SET){
		iterset *it = &op->iter.set;
		if(op->encoding == REDIS_ENCODING_INTSET){
			REDIS_NOTUSED(it);
		}else if(op->encoding == REDIS_ENCODING_HT){
			dictReleaseIterator(it->ht.di);
		}else {
			redisPanic("Unknown set encoding");
		}
	}else if(op->type == REDIS_ZSET){
		iterzset *it = &op->iter.zset;
		if(op->encoding == REDIS_ENCODING_ZIPLIST){
			REDIS_NOTUSED(it);
		}else if(op->encoding == REDIS_ENCODING_SKIPLIST){
			REDIS_NOTUSED(it);
		}else{
			redisPanic("Unknown zset encoding");
		}
	}else{
		redisPanic("Unsupported type");
	}
}

/*
 * Return the length of iterate elements. 
 */ 
int zuiLength(zsetopsrc *op){
	
	if(op->subject == NULL){
		return 0;
	}

	if(op->type == REDIS_SET){
		if(op->encoding == REDIS_ENCODING_INTSET){
			return intsetLen(op->subject->ptr);
		}else if(op->encoding == REDIS_ENCODING_HT){
			dict *ht = op->subject->ptr;
			return dictSize(ht);
		}else{
			redisPanic("Unknown set encoding");
		}
	}else if(op->type == REDIS_ZSET){
		if(op->encoding == REDIS_ENCODING_ZIPLIST){
			return zzlLength(op->subject->ptr);
		}else if(op->encoding == REDIS_ENCODING_SKIPLIST){
			zset *zs = op->subject->ptr;
			return zs->zsl->length;
		}else{
			redisPanic("Unknown zset encoding");
		}
	}else{
		redisPanic("Unsupported type");
	}
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element, return 1.
 * If not valid, this means we have reached the
 * end of the structure and can abort, returns 0.
 */ 
int zuiNext(zsetopsrc *op, zsetopval *val){

	if(op->subject == NULL) return 0;

	//Clean the previous element which hold in this val structure.
	if(val->flags & OPVAL_DIRTY_ROBJ)
		decrRefCount(val->ell);
	
	//Clean the val structure.
	memset(val, 0, sizeof(zsetopval));
	/* 这里有一个很有意思的事情，注意如果操作的是一个intset，那么如果被迭代的元素会直接放入
	 * 到val->ell 中，如果迭代的是一个ziplist，那么他会设置eptr这个字段，其他类型都只会
	 * 操作ele这个元素.注意这个细节对于理解下面的zuiLongLongFromValue 很重要。
	 */ 

	if(op->type == REDIS_SET){

		iterset *it = &op->iter.set;
		if(op->encoding == REDIS_ENCODING_INTSET){
			int64_t ell;

		if(!intsetGet(it->is.is, it->is.ii, &ell)) return 0;
		
		val->ell = ell;
		//Default score 
		val->score = 1.0;

		it->is.ii++;

		}else if(op->encoding == REDIS_ENCODING_HT){
			if(it->ht.de == NULL) return 0;

			val->ele = dictGetVal(it->ht.de);

			val.score = 1.0;

			it->ht.de = dictNext(it->ht.di);
		}else{
			redisPanic("Unknown set encoding");
		}

	}else if(op->type == REDIS_ZSET){
		iterzset *it = &op->iter.zset;
		if(op->encoding == REDIS_ENCODING_ZIPLIST){
			//No more element to iterate
			if(it->zl.eptr == NULL || it->zl.sptr == NULL){
				return 0;
			}

			redisAssert(ziplistGet(it->zl.eptr, &val->estr, &val->elen, &val->ell));
			val->score = zzlGetScore(it->zl.sptr);

			//Move to the next elements pair
			zzlNext(it->zl.zl, &it->zl.eptr, &it->zl.sptr);
		}else if(op->encoding == REDIS_ENCODING_SKIPLIST){
			if(it->sl.node == NULL) return 0;

			val->ele =  it->sl.node->obj;
			val->score = it->sl.node->score;

			it->sl.node = it->sl.node->level[0].forward;
		}else{
			redisPanic("Unknown zset encoding");
		}
	}else{
		redisPanic("Unsupported type");
	}

	return 1;
}

/* From the val get the long long value and put the result 
 * into the val->ell.
 */ 
int zuiLongLongFromValue(zsetopval *val){
	if(!(val->flags & OPVAL_DIRTY_LL)){

		//Open the DIRTY LL
		val->flags |= OPVAL_DIRTY_LL;

		//Get the object 
		if(val->ele != NULL){
			//Get the integer from the INT encoding.
			if(val->ele->encoding == REDIS_ENCODING_INT){
				val->ell = (long)val->ele->ptr;
				val->flags |= OPVAL_VALID_LL;
			}else if(sdsEncodedObject(val->ele)){
				if(string2ll(val->ele->ptr, sdslen(val->ele->ptr, &val->ell)))
					val->flags |= OPVAL_VALID_LL;
			}else{
				redisPanic("Unsupported element encoding");
			}
		}else if(val->estr != NULL){
			//Convert the node from string to integer.
			if(string2ll((char*)val->estr, val->elen, &val->ell))
				val->flags |= OPVAL_VALID_LL; 
		}else{
			//This is the case where we deal with the intset.
			val->flags |= OPVAL_VALID_LL;
		}
	}
	//Check if it has the OPVAL_VALID_LL flag, which means success convert to a long 
	//long.
	return val->flags & OPVAL_VALID_LL;
}

/* Get the value from val then create a object, and put the 
 * result into the val->ele variable.
 */ 
robj *zuiObjectFromValue(zsetopval *val){

	if(val->ele == NULL){
		if(val->estr != NULL){
			val->ele = createStringObject((char *)val->vstr, val->elen);
		}else{
			val->ele = createStringObjectFromLongLong(val->ell);			
		}
	}

	val->flags |= OPVAL_DIRTY_ROBJ;
	return val->ele;
}

/* Get the string from the val and put into the val->estr.
 */ 
int zuiBufferFromValue(zsetopval *val){
	if(val->estr == NULL){
		if(val->ele != NULL){
			if(val->ele->encoding == REDIS_ENCODING_INT){
				val->elen = ll2string((char *)val->_buf, sizeof(val->_buf), (long)val->ele->ptr);
				val->estr = val->_buf;
			}else if(sdsEncodedObject(val->ele)){
				val->estr = val->ele->ptr;
				val->elen = sdslen(val->ele->ptr);
			}else{
				redisPanic("Unsupport element encoding");
			}
		}else{
			val->elen = ll2string((char*)val->_buf, sizeof(val->_buf), val->ell);
			val->estr = val->_buf;
		}
	}
	return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise.
 */ 
int zuiFind(zsetopsrc *op, zsetopval *val, double *score){
		if(op == NULL) return 0;

		if(op->type == REDIS_SET){
			if(op->encoding == REDIS_ENCODING_INTSET){
				if(zuiLongLongFromValue(val && intsetFind(op->subject->ptr, val->ell)){
					if(score) *score = 1.0;
					return 1;
				}else{
					return 0;
				}
			}else if(op->encoding == REDIS_ENCODING_HT){
				zuiObjectFromValue(val);
				dict *d = op->subject->ptr;
				if(dictFind(d, val->ele) != NULL){
					if(score) *score = 1.0;
					return 1;
				}else{
					return 0;
				}
			}else{
				redisPanic("Unknown set encoding");
			}
		}else if(op->type == REDIS_ZSET){
			zuiObjectFromValue(val);
			if(op->encoding == REDIS_ENCODING_ZIPLIST){
				if(zzlFind(op->subject->ptr, val->ele, score) != NULL){
					return 1;
				}else{
					return 0;
				}
			}else if(op->encoding == REDIS_ENCODING_SKIPLIST){
				zset *zs = op->subject->ptr;
				dictEntry *de;
				
				if((de = dictFind(zs->dict, val->ele)) != NULL){
					if(score) *score = *(double*)dictGetVal(de);
					return 1;
				}else{
					return 0;
				}
			}else{
				redisPanic("Unknown zset encoding");
			}
		}else{
			redisPanic("Unsupported type");
		}
}

/* Compare the cardinality of two object.
 */ 
int zuiCompareByCardinality(const void *s1, const void *s2){
	return zuiLength((zsetopsrc *)s1) - zuiLength((zsetopsrc *)s2);
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/* 
 * Acccoding to the aggregate value, to determine how to perform caculate on *target and val.
 */
inline static void zunionIntegerAggregate(double *target, double val, int aggregate){

	//sum
	if(aggregate == REDIS_AGGR_SUM){
		*target = *target + val;
		/* The result of adding two doubles is NaN when one variable
		 * is +inf and the other is -inf. When these number are added,
		 * we maintain the convention of the result being 0.0.
		 */
		if(isnan(*target)) *target = 0.0;
	}else if(aggregate == REDIS_AGGR_MIN){
		*target = *target < val ? *target : val;
	}else if(aggregate == REDIS_AGGR_MAX){
		*target = *target > val ? *target : val;
	}else{
		redisPanic("Unknown ZUNION/INTER aggregate type");
	}
}

void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op){
	int i, j;
	long setnum;
	int aggregate = REDIS_AGGR_SUM;
	zsetopsrc *src;
	zsetopval zval;
	robj *tmp;
	unsigned int maxelelen = 0;
	robj *dstobj;
	zset *dstzset;
	zskiplistNode *znode;
	int touched = 0;

	/*expect setnum input keys to be given*/
	if(getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != REDIS_OK) return;

	if(setnum < 1){
		addReplyError(c, 
		"at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
		return;
	}

	/*test if the expected number of keys would overflow */
	if(setnum > c->argc - 3){
		addReply(c, shared.syntaxerr);
		return;
	}

	/*read keys to be used for input*/
	src = zcalloc(sizeof(zsetopsrc) * setnum);
	for(i = 0, j =3; i < setnum; i++; j++){

		//get the robj object
		robj *obj = lookupKeyWrite(c->db, c->argv[j]);
		//Create iterator
		if(obj != NULL){
			if(obj->type != REDIS_ZSET && obj->type != REDIS_SET){
				zfree(src);
				addReply(c, shared.wrongtypeerr);
				return;
			}

			src[i].subject = obj;
			src[i].type = obj->type;
			src[i].encoding = obj->encoding;
		}else{
			src[i].subject = NULL;
		}

		/*Default optional extra argument */
		src[i].weight = 1.0;
	}

	//May be the arguments will follow (weight 1.0 3.0) etc.., parse them.
	if(j < c->argc ){
		int remaining = c->argc - j;

		while(remaining){
			if(remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr, "weights")){
				j++, remaining--;
				//parse the weight parameter
				for(i = 0; i < setnum; i++, j++, remaining--){
					if(getDoubleFromObjectOrReply(c, c->argv[j], &src[i].weight,
					   "weight value is not a float") != REDIS_OK){
						   zfree(src);
						   return;
					}
				}
			}else if(remaining >= 2 && !strcasecmp(c->argv[j]->ptr, "aggregate")){
				j++, remaining--;
				if(!strcasecmp(c->argv[j]->ptr, "sum")){
					aggregate = REDIS_AGGR_SUM;
				}else if(!strcasecmp(c->argv[j]->ptr, "min")){
					aggregate = REDIS_AGGR_MIN;
				}else if(!strcasecmp(c->argv[j]->ptr, "max")){
					aggregate = REDIS_AGGR_MAX;
				}else{
					zfree(src);
					addReply(c, shared.syntaxerr);
					return;
				}
				j++; remaining--;
			}else{
				zfree(src);
				addReply(c, shared.syntaxerr);
				return;
			}
		}
	}

	//Sort this set by cardinality
	qsort(src, setnum, sizeof(zsetopsrc), zuiCompareByCardinality);
	//Create result set object
	dstobj = createZsetObject();
	dstzset = dstobj->ptr;
	memset(&zval, 0, sizeof(zval));

	//ZINTERSTORE
	if(op == REDIS_OP_INTER){	

		//Skip everything if the smallest set is empty
		if(zuiLength(&src[0]) > 0){
			/* Prediction: as src[0] is non-empty and the inputs are ordered 
			 * by size, all src[i > 0] are non-empty too.
			 */
			zuiInitIterator(&src[0]);
			while(zuiNext(&src[0], &zval)){
				double score, value;

				//caculate score
				score = src[0].weight * zval.score;
				if(isnan(score)) score = 0;

				for(j = 1; j < setnum; j++){
					/* It is not safe to access the zset we are
					 * iterating, so explicitly check for equal object.
					 */
					if(src[j].subject == src[0].subject){
						value = zval.score * src[j].weight;
						zunionIntegerAggregate(&score, value, aggregate);
					}else if(zuiFind(&src[j], &zval, &value)){
						value *= src[j].weight;
						zunionIntegerAggregate(&score, value, aggregate);
					}else{
						break;
					}
				}

				//Only continue when presetn in every input.
				if(j == setnum){
					//Get the object value
					tmp = zuiObjectFromValue(&zval);
					//insert into the sorted-set
					znode = zslInsert(dstzset->zsl, score, tmp);
					incrRefCount(tmp);
					//Insert into the dict
					dictAdd(dstzset->dict, tmp, &znode->score);
					incrRefCount(tmp);

					//update string object length
					if(sdsEncodedObject(tmp)){
						if(sdslen(tmp->ptr) > maxelelen)
							maxelelen = sdslen(tmp->ptr);
					}
				}
			}
			zuiClearIterator(&src[0]);
		}
	}else if(op == REDIS_OP_UNION){

		for(i = 0; i < setnum; i++){

			//Skip the empty set
			if(zuiLength(&src[i]) == 0) continue;
			zuiInitIterator(&src[i]);
			/* This while loop just do one thing: it loop through the whole zset,
			 * and do the aggregate for the element that is the same.
			 */
			while(zuiNext(&src[i], &zval)){
				double score, value;
				//If this element is already exist in the union set we skip it.
				if(dictFind(dstzset->dict, zuiObjectFromValue(&zval)) != NULL) 
					continue;
				//Init the score
				score = src[i].weight * zval.score

				if(isnan(score)) score = 0;

				for(j = (i+1); j < setnum; j++){
					
					if(src[j].subject == src[i].subject){
						value = zval.score * src[j].weight;
						zunionIntegerAggregate(&score, value, aggregate);
					}else if(zuiFind(&src[j], &zval, &value)){
						value *= src[j].weight;
						zunionIntegerAggregate(&score, value, aggregate);
					}
				}

				//Get the member
				tmp = zuiObjectFromValue(zval);
				//Insert the union set element into the skiplist
				znode = zslInsert(dstzset->zsl, score, tmp);
				incrRefCount(zval.ele);
				//Add the element into the dictionary
				dictAdd(dstzset->dict, tmp, &znode->score);
				incrRefCount(zval.ele);

				//Update the longest string
				if(sdsEncodedObject(tmp)){
					if(sdslen(tmp->ptr) > maxelelen){
						maxelelen = sdslen(tmp->ptr);
					}
				}
			}
			zuiClearIterator(&src[0]);
		}
	}else{
		redisPanic("Unknown operator");
	}

	//If the dstkey is existed, delete it.
	if(dbDelete(c->db, dstkey)){
		signalModifiedKey(c->db, dstkey);
		touched = 1;
		server.dirty++;
	}

	if(dstzset->zsl->length){
		//Check if we need to convert this zset implementation from the skiplist to ziplist
		if (dstzset->zsl->length <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
			zsetConvert(dstobj, REDIS_ENCODING_ZIPLIST);
		//Add the collection to the database
		dbAdd(c->db, dstkey, dstobj);

		//Reply the length of the collection
		addReplyLongLong(c, zsetLength(dstobj));

		if (!touched) signalModifiedKey(c->db,dstkey);

        notifyKeyspaceEvent(REDIS_NOTIFY_ZSET,
            (op == REDIS_OP_UNION) ? "zunionstore" : "zinterstore",
            dstkey,c->db->id);

        server.dirty++;
	}else{
		decrRefCount(dstobj);

		addReply(c, shared.czero);

		if(touched)
			notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",dstkey,c->db->id);
	}
	zfree(src);
}

void zunionstoreCommand(redisClient *c){
	zunionInterGenericCommand(c, c->argv[1], REDIS_OP_UNION);
}

void zinterstoreCommand(redisClient *c){
	zunionInterGenericCommand(c, c->argv[1], REDIS_OP_INTER);
}

/*This command use to implemented ZRANGE and ZREVRANGE */
void zrangeGenericCommand(redisClient *c, int reverse){
	robj *key = c->argv[1];
	robj *zobj;
	int withscores = 0;
	long start;
	long end;
	int llen;
	int rangelen;

	//get the start and end parameter
	if(getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK || 
	   getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK) return;

	//Check if we need to show the scores
	if(c->argc == 5 && !strcasecmp(c->argv[4]->ptr, "withscores")){
		withscores = 1;
	}else if(c->argc >= 5){
		addReplyError(c, shared.syntaxerr);
		return;
	}

	//Get the sorted set object
	if((zobj = lookupKeyReadOrReply(c, key, shared.emptymultibulk)) == NULL || 
	   checkType(c, zobj, REDIS_ZSET)) return;

	//Convert all the negative indexes into the positive
	llen = zsetLength(zobj);
	if(start < 0) start += llen;
	if(end < 0) end += llen;
	if(start < 0) start = 0;

	/* After the previous operation, the start is >= 0, so when the end is
	 * < start, the end is < 0. 
	 */
	if(start > end || start >= llen){
		addReply(c, shared.emptymultibulk);
		return;
	}
	if(end >= llen) end = llen - 1;
	rangelen = (end - start) + 1;

	/*Return the result in form of a multi-bulk reply */
	addReplyMutiBulkLen(c, withscores ? (rangelen * 2) : rangelen);

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;


		/* Consider the iterate direction, when the order is reverse order,
		 * we convert this index as (-start - 1) * 2, what does that means?
		 * first when we convert start to -start, means we want to iterate 
		 * from the tail start th element, but that is not enough, remind that
		 * we start from head 0 ,1 2, 3.... we start from tail -1, -2, -3, -3...
		 * so the right mapping is start ~ (-start - 1), because the ziplist as zset
		 * it use two slot per element, so we need mutiply by 2.
		 */
		if(reverse)
			eptr = ziplistIndex(zl, -2 - (start*2));
		else
			eptr = ziplistIndex(zl, start * 2);

		redisAssertWithInfo(c, zobj, eptr != NULL);
		sptr = ziplistNext(zl, eptr);

		//get the element
		 while(rangelen--){
			redisAssertWithInfo(c, zobj, eptr != NULL && sptr != NULL);
			redisAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
			if(vstr == NULL){
				 addReplyBulkLongLong(c, vlong);
			}else{
				 addReplyBulkCBuffer(c, vstr, vlen);
			}
			if(withscores)
				addReplyDouble(c, zzlGetScore(sptr));
			if(reverse)
				zzlPrev(zl, &eptr, &sptr);
			else
				zzlNext(zl, &eptr, &sptr);
		 }

	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		zskiplistNode *ln;
		robj *ele;

		//Check the start point
		if(reverse){
			ln = zs->zsl->tail;
			if(start > 0)
				ln = zslGetElementByRank(zsl, llen - start);
		}else{
			ln = zsl->header->level[0].forward;
			if(start > 0)
				//zsl start from 1
				ln = zslGetElementByRank(zsl, start+1);
		}

		while(rangelen--){
			redisAssertWithInfo(c, zobj, ln != NULL);
			ele = ln->obj;
			addReplyBulk(c, ele);
			if(withscores)
				addReplyDouble(c, ln->score);
			ln = reverse ? ln->backward : ln->level[0].forward;
		}

	}else{
		redisPanic("Unknown sotred set encoding");
	}
}

void zrangeCommand(redisClient *c){
	zrangeGenericCommand(c, 0);
}

void zrevrangeCommand(redisClient *c){
	zrangeGenericCommand(c, 1);
}

/* You may want to know we already have the zrange why we need
 * a function that can do similar things.
 */
void genericZrangebyscoreCommand(redisClient *c, int reverse){
	zrangespec range;
	robj *key = c->argv[1];
	robj *zobj;
	long offset = 0, limit = -1;
	int withscores = 0;
	unsigned long rangelen = 0;
	void *replylen = NULL;
	int minidx, maxidx;

	/*Because the reverse order and the normal order is min and max is different */
	if(reverse){
		maxidx = 2; minidx = 3;
	}else{
		minidx = 2; maxidx = 3;
	}

	//Analysis the input range
	if(zslParseRange(c->argv[min], c->argv[max], &range) != REDIS_OK){
		addReplyError(c, "min or max is not a float");
		return;
	}

	/* Parse optional extra arguments. Note that ZCOUNT will exactly have
	 * 4 arguments, so we'll never enter the following code path.
	 */
	if(c->argc > 4){
		int remaining = c->argc - 4;
		int pos = 4;

		while(remaining){
			if(remaining >= 1 && !strcasecmp(c->argv[pos]->ptr, "withscores")){
				pos++, remaining--;
				withscores = 1;
			}else if(remaining >= 3 && !strcasecmp(c->argv[pos], "limit")){
				if((getLongFromObjectOrReply(c, c->argv[pos+1]->ptr, &offset, NULL)) != REDIS_OK ||
				   (getLongFromObjectOrReply(c, c->argv[pos+2]->ptr, &limit, NULL)) != REDIS_OK) return;
				pos += 3; remaining -= 3;
			}else{
				addReply(c, shared.sytaxerr);
				return;
			}
		}
	}

	/*Ok, lookup the key and get the range */
	if((zobj = lookupKeyReadOrReply(c, key, shared.emptymultibulk)) == NULL ||
	   checkType(c, zobj, REDIS_ZSET)) return;
	
	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;
		double score;

		if(reverse){
			eptr = zzlLastInRange(zl, &range);
		}else{
			eptr = zzlFirstInRange(zl, &range);
		}

		//No element in the given range
		if(eptr == NULL){
			addReply(c, shared.emptymutibulk);
			return;
		}

		redisAssertWithInfo(c, zobj, eptr != NULL);
		sptr = ziplistNext(zl, eptr);

		/* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

		/* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 跳过 offset 指定数量的元素
		while(eptr && offset--){
			if(reverse){
				zzlPrev(zl, &eptr, &sptr);
			}else{
				zzlNext(zl, &eptr, &sptr);
			}
		}

		while(eptr && limit--){

			score = zzlGetScore(sptr);

			//Abort when the score is no longer in this range
			if(reverse){
				if(!zslValueGteMin(score, &range)) break;
			}else{
				if(!zslValueLteMax(score, &range)) break;
			}

			redisAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

			rangelen++;
			if(vstr == NULL){
				addReplyBulkLongLong(c, vlong);
			}else{
				addReplyBulkCBuffer(c, vstr, vlen);
			}
		
			if(withscores)
				addReplyDouble(c, score);
			/*Move to the next element */
			if(reverse)
				zzlPrev(zl, &eptr, &sptr);
			else
				zzlNext(zl, &eptr, &sptr);
		}

	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		zskiplistNode *zn;

		if(reverse)
			zn = zslFirstInRange(zsl, &range);
		else
			zn = zslLastInRange(zsl, &range);
		
		if(zn == null){
			addReply(c, shared.emptymultibulk);
			return;
		}

		replylen = addDeferredMultiBulkLength(c);
		while(zn && offset--){
			if(reverse)
				zn = zn->backward;
			else
				zn = zn->level[0].forward;
		}

		while(ln && limit--){
			
			if(reverse)
				if(!zslValueGteMin(ln->score, &range)) break;
			else
				if(!zslValueLteMax(ln->score, &range)) break;
		
			rangelen++;
			addReplyBulk(c, zn->obj);

			if(withscores)
				addReplyDouble(c, ln->score);

			if(reverse)
				zn = zn->backward;
			else
				zn = zn->level[0].forward;
		}
	}else{
		redisPanic("Unknown sotred set encoding");
	}

	if(withscores) rangelen *= 2;

	setDeferredMultiBulkLength(c, replylen, rangelen);
}

void zrangebyscoreCommand(redisClient *c){
	genericZrangebyscoreCommand(c, 0);
}

void zrevrangebyscoreCommand(redisClient *c){
	genericZrangebyscoreCommand(c, 1);
}

void zcountCommand(redisClient *c){
	robj *key = c->argv[1];
	robj *zobj;
	zrangespec range;
	int count = 0;

	//Parse the range arguments
	if(zslParseRange(c->argv[2], c->argv[3], &range) != REDIS_OK){
		addReplyError(c, "min or max is not a float");
		return;
	}

	/*Look up the sorted set */
	if((zobj = lookupKeyReadOrReply(c, key, shared;czero)) == NULL || 
	   checkType(c, zobj, REDIS_ZSET)) return;

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		double score;

		eptr = zzlFirstInRange(zl, &range);

		//No element in this range
		if(eptr == NULL){
			addReply(c, shared.czero);
			return;
		}

		sptr = zzlNext(zl, eptr);
		score = zzlGetScore(sptr);
		redisAssertWithInfo(c, zobj, zslValueLteMax(score, &range));
		
		while(eptr){
			score = zzlGetScore(sptr);
			if(!zslValueLteMax(score, &range)){
				break;
			}else{
				count++;
				zzlNext(zl, &eptr, &sptr);
			}
		}

	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){	
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		zskiplistNode *zn;
		unsigned long rank;

		//Locate to the first element that is in the range
		zn = zslFirstInRange(zsl, &range);

		//If the zn is not NULL it means at least there is a element in the range
		if(zn != NULL){
			rank = zslGetRank(zsl, zn->score, zn->obj);

			count = (zsl->length - (rank - 1));

			//Find the last element in the range
			zn = zslLastInRange(zsl, &range);

			/*Use rank of the element, if any, to determine the actual count */
			if(zn != NULL){
				rank = zslGetRank(zsl, zn->score, zn->obj);

				//Caculate the distance between frist element in range and last element in range
				count -= (zsl->length - rank);
			}
		}
		/* 	不得不说在跳表删除元素这里有很多的小细节需要注意,其实他也可以通过，获取到第一个
		 *  在区间内的元素开始，依次遍历，直到找到一个不再范围内的元素为止。 但是这里他用了另一种方式
		 *  来完成这个任务。首先他获取到第一个在这个区间内元素的rank， rank是从1开始的。
		 *  那么从 该元素开始到结尾 有多少个元素呢？ zsl->length - (rank - 1) eg: zsl->length = 5, rank = 1;
		 *  那么其实就是说 在计算从当前元素开始到结尾有多少元素时，需要的是rank - 1，随后我们只要找到最后一个在这个区间
		 *  中的元素两者做减法就ok了不是吗？由于在求解最后一个在该区间rank值，到结尾有多少个元素，不包括最后一个在该区间
		 *  的元素本身, zsl->length - (rank - 1) + 1，减1的理由和之前一样， 加1是因为当前元素不再区间内。 
		 */
	}else{
		redisPanic("Unknown sotred set encoding");
	}

	addReplyLongLong(c, count);
}

void zlexcountCommand(redisClient *c){
	robj *key = c->argv[1];
	robj *zobj;
	zlexrangespec range;
	int count = 0;

	//Parse the input into zlexrangespec
	if(zslParseLexRange(c->argv[2], c->argv[3], &range) != REDIS_OK){
		addReplyError(c, "min or max not vaild string range item");
		return;
	}

	if((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
	    checkType(c, zobj, REDIS_ZSET)){
			//Recall that the zslParseLexRange will create string object,
			//so remember to free them after u use range
			zslFreeLexRange(&range);
			return;
		}

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;

		eptr = zzlFirstInLexRange(zl, &range);

		//No element in this range
		if(eptr == NULL){
			zslFreeLexRange(&range);
			addRely(c, shared.czero);
			return;
		}

		sptr = ziplistNext(zl, eptr);
		redisAssertWithInfo(c, zobj, zzlLexValueLteMax(eptr, &range));

		while(eptr){
			if(!zzlLexValueLteMax(eptr, &range))
				break;
			count++;
			zzlNext(zl, &eptr, &sptr);
		}
	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset *zs = zobj->ptr;
		zskiplistNode *zn;
		zskiplist *zsl = zs->zsl;
		unsigned long rank;

		zn = zslFirstInLexRange(zsl, &range);

		if(zn != NULL){
			rank = zslGetRank(zsl, zn->score, zn->obj);
			count = zsl->length - (rank - 1);

			zn = zslLastInLexRange(zsl, &range);

			if(zn != NULL){
				rank = zslGetRank(zsl, zn->score, zn->obj);
				count -= (zsl->length - rank);
			}
		}

	}else{
		redisPanic("Unknown sotred set encoding");
	}	
	zslFreeLexRange(&range);
	addReplyLongLong(c, count);
}

void genericZrangebylexCommand(redisClient *c, int reverse){
	zlexrangespec range;
	robj *key = c->argv[1];
	robj *zobj;
	long offset = 0, limit = -1;
	unsigned long rangelen = 0;
	void *replylen = NULL;
	int minidx, maxidx;

	/* Parse the range arguments*/
	if(reverse){
		maxidx = 2; minidx = 3;
	}else{
		minidx = 2; maxidx = 3;
	}

	if(zslParseLexRange(c->argv[minidx], c->argv[maxidx], &range) != REDIS_OK){
		addReplyError(c, "min or max not a vaild string range item");
		return;
	}

	/* Parse additional parameters */
	if(c->argc > 4){
		int remaining = c->argc - 4;
		int pos = 4;
		while(remaining){
			if(remaining >= 3 && !strcasecmp(c->argv[pos]->ptr, "limit")){
				if((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) || 
				   (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
					pos += 3, remaining -= 3;				
			}else{
				zslFreeLexRange(&range);
				addReplyError(c, shared.syntaxerr);
				return;
			}
		}
	}

	/* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET))
    	{
        	zslFreeLexRange(&range);
        	return;
   		}
	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *zl = zobj->ptr;
		unsigned char *eptr, *sptr;
		unsigned char *vstr;
		unsigned int vlen;
		unsigned long long vlong;

		if(reverse)
			eptr = zzlLastInLexRange(zl, &range);
		else
			eptr = zzlFirstInLexRange(zl, &range);

		if(eptr == NULL){
			addReply(c, shared.emptymultibulk);
			zslFreeLexRange(&range);
			return;
		}

		redisAssertWithInfo(c, zobj, eptr != NULL);
		sptr = ziplistNext(zl, eptr);

		/* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

		while(eptr && offset--){
			if(reverse)
				zzlPrev(zl, &eptr, &sptr);
			else
				zzlNext(zl, *eptr, &sptr);
		}	

		while(eptr && limit--){
			if(reverse)
				if(!zzlLexValueGteMin(eptr, &range)) break;
			else
				if(!zzlLexValueLteMax(eptr, &range)) break;

			redisAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

			rangelen++;
			if(vstr == NULL)
				addReplyBulkLongLong(c, vlong);
			else
				addReplyBulkCBuffer(c, vstr, vlen);	

			if(reverse)
				zzlPrev(zl, &eptr, &sptr);
			else
				zzlNext(zl, &eptr, &sptr);
		}

	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset *zs = zobj->ptr;
		zskiplist *zsl = zs->zsl;
		zskiplistNode *zn;

		if(reverse)
			zn = zslLastInLexRange(zsl, &range);
		else
			zn = zslFirstInLexRange(zsl, &range);

		if(zn == NULL){
			addReply(c, shared.emptymultibulk);
			zslFreeLexRange(&range);
			return;
		}
		replylen = addDeferredMultiBulkLength(c);

		while(zn && offset--){
			if(reverse)
				zn = zn->backward;
			else
				zn = zn->level[0].forward;
		}

		while(zn && limit--){
			if(reverse)
				if(!zslValueGteMin(zn->obj, &range)) break;
			else
				if(!zslValueLteMax(zn->obj, &range)) break;

			rangelen++;
			addReplyBulk(c, zn->obj);

			if(reverse)
				zn = zn->backward;
			else
				zn = zn->level[0].forward;
		}

	}else{
		redisPanic("Unknown sotred set encoding");
	}
	zslFreeLexRange(&range);
	setDeferredMultiBulkLength(c, replylen, rangelen);
}

void zrangebylexCommand(redisClient *c){
	genericZrangebylexCommand(c, 0);
}

void zrevrangebylexCommand(redisClient *c){
	genericZrangebylexCommand(c, 1);
}

void zcardCommand(redisClient *c){
	robj *key = c->argv[1];
	robj *zobj;

	if((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
	   checkType(c, zobj, REDIS_ZSET)) return;

	addReplyLongLong(c, zsetLength(zobj));
}

void zscoreCommand(redisClient *c){
	robj *key = c->argv[1];
	robj *zobj;
	double score;


	if((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
	   checkType(c, zobj, REDIS_ZSET)) return;

	if(zobj->encoding == REDIS_ENCODING_ZIPLIST){
		if(zzlFind(zobj->ptr, c->argv[2], &score)){
			addReplyDouble(c, score);
		}else{
			addReply(c, shared.nullnulk);
			return;
		}
	}else if(zobj->encoding == REDIS_ENCODING_SKIPLIST){
		zset *zs = zobj->ptr;
		dictEntry *de;

		c->argv[2] = tryObjectEncoding(c->argv[2]);

		de = dictFind(zs->dict, c->argv[2]);

		if(de != NULL){
			score = *(double *)dictGetVal(de);
			addReply(c, score);
		}else{
			addReply(c, shared.nullnulk);
			return;
		}
	}else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrankGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    unsigned long llen;
    unsigned long rank;

    // 有序集合
    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // 元素数量
    llen = zsetLength(zobj);

    redisAssertWithInfo(c,ele,sdsEncodedObject(ele));

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,sptr != NULL);

        // 计算排名
        rank = 1;
        while(eptr != NULL) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            // ZRANK 还是 ZREVRANK ？
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        // 从字典中取出元素
        ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,ele);
        if (de != NULL) {

            // 取出元素的分值
            score = *(double*)dictGetVal(de);

            // 在跳跃表中计算该元素的排位
            rank = zslGetRank(zsl,score,ele);
            redisAssertWithInfo(c,ele,rank); /* Existing elements always have a rank. */

            // ZRANK 还是 ZREVRANK ？
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }

    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrankCommand(redisClient *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(redisClient *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(redisClient *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == REDIS_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,REDIS_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}































































