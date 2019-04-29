#include "redis.h"

static int zsLexValueGteMin(robj *value, zlexrangespec *spec);
static int zsLexValueLteMax(robj *value, zlexrangespec *spec);

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
	while((random() & 0xFFFF) < (ZSKIP_LIST_P * 0xFFFF))
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
			update[i].level[i].span += x->level[i].span - 1;
			update[i].level[i].forward = x->level[i].forward;
		}else{
			update[i].level[i].span -= 1;
		}
	}

	//update the head and tail pointer
	if(x->level[0].forward){
		x->level[0].forward->backward = x->level[0]->backward;
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
void zslDeleteNode(zskiplist *zsl, double score, robj *obj){
	zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
	int i;
	x = zsl->head;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward && 
		      (x->level[i].forward->score < score) || 
		      (x->level[i].forward->score == score && 
		       compareStringObject(x->level[i].forward->obj, obj) < 0)){
			x = x->forward;
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
	zskiplisNode *x;

	//Exclude for the bad input 
	if(range->min > range->max || 
	  (range->min == range->max && (range->minex || range->maxex)))
		return 0;

	//check the max element
	x = zsl->tail;
	if(x == NULL || !zslValueGteMin(x->score, range))
		return 0;

	//check the min element
	x = zsl->head;
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
	x = zsl->head;
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
			x = x.level[i].forward;
		update[i] = x;
	}
	//The loop stop at the element which next is greater or greater equals than
	//the range min
	x = x.level[0].forward;
	while(x && (range->maxex ? x->score < range->max : x->score <= range->max )){
		zskiplistNode *next = x->level[0].forward;
		
		//delete the current node from the skiplist
		zskDeleteNode(zsl, x, update);
		//delete this node from the dict
		dictDelete(dict, x->obj);	
		//free current skiplist node
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
		dictDelete(dict, x->robj);
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
	zskiplistNode x;
	int i;
	unsigned long rank = 0;
	
	x = zsl->header;
	for(i = zsl->level - 1; i >= 0; i--){
		while(x->level[i].forward &&
		     (x->level[i].forward->score < score || 
		     (compareStringObjects(x->level[i].forward->robj, o) <= 0))){
			rank += x->level[i].span;
			x = x->level[i].forward;
		}
		if(x-> obj && equalsStringObject(x->obj, o))
			return rank;
		return 0;
}





