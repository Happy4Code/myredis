#include "redis.h"

void signalListAsReady(redisClient *c, robj *key);

/*-------------------------------------------------------------------------------------------
 *					List API
 *------------------------------------------------------------------------------------------*/

/**
 * Check the arguent length to see if it requires us to convert the ziplist into an 
 * double-linked list.Only Check raw-encoded objects because interger encoded objects are never
 * to long.
 *
 * In this function we check if the input argument 'value' exceed the predefine maximum node size.
 */
void listTypeTryConversion(robj *subject, robj *value){
	
	//Make sure the subject encoding is ZIP_LIST
	if(subject->encoding != REDIS_ENCODING_ZIPLIST) return;

	if(sdsEncodedObject(value) && sdslen(value->ptr) > server.list_max_ziplist_value)
		//Convert to the double-linked list
		listTypeConvert(subject, REDIS_ENCODING_LINKEDLIST);
}

/**
 * This function pushes an element to the specified list object 'subject', at head
 * or tail position as specified by 'where'.
 *
 * Add the head or the tail depeneding on the where.
 *
 * when the where is REDIS_HEAD, it means add to the header.
 * when the wehre is REDIS_TAIL, it means add to the tail.
 *
 * There is no need for the caller to increment the refcount of 'value' as the function
 * takes care of it if needed.
 */
void listTypePush(robj *subject, robj *value, int where){
	
	/*Before we do the insert we do the convert check*/
	listTypeTryConversion(subject, value);
	//After we check the size of the input parameter, we also need to 
	//check the total numbers of the entries of the subject is it exceed
	//the maximum number of the ziplist.
	if(subject->encoding == REDIS_ENCODING_ZIPLIST && ziplistLen(subject->ptr) >= server.list_max_ziplist_entries)
		listTypeConvert(subject, REDIS_ENCODING_LINKEDLIST);
	//ZIPLIST
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		int pos = (where== REDIS_HEAD) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
		//Because the ziplist can only hold the integer and string, so before we insert into the ziplist,
		//we need to apply some conversion to the 'value' parameter to make sure it can be insert into ziplist
		value = getDecodeObject(value);
		subject->ptr = ziplistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
		//I think we need to pay some attention to the decrRefCount function, why it is here?
		//As we know the redis use the count-reference method to mark the object for GC.
		//Why we use decrRefCount here is the ziplist will copy the content of value to the
		//ziplist entry, so when we after push, we no longer need to hold this value object.
		//So we decrRef the value object counter.
		decrRefCount(value);	
	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		if(where == REDIS_HEAD)
			listAddNodeHead(subject->ptr, value);
		else
			listAddNodeTail(subject->ptr, value);
		//But for the linked list, it is different, linked list entry has an value field, 
		//which never do the copy work, they point to the same value object, so we need 
		//to increment the value object counter. 
		incrRefCount(value);
	}else{
		//at current version, the list has no other implemention.
		redisPanic("Unknown list encoding...");
	}
}

/**
 * Pop a entry from the list.
 * The parameter where determine where the pop procedure start.
 * -REDIS_HEAD pop from the list head
 * -REIDS_TAIL pop from the list tail
 */
robj *listTypePop(robj *subject, int where){
	
	robj *value = NULL;

	//ZIPLIST
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		//Determine the pop position
		int pos = (where == REDIS_HEAD) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
		p = ziplistIndex(subject->ptr);
		if(ziplistGet(p, &vstr, &vlen, &vlong)){
			if(vstr){
				//If it is a string
				value = createStringObject(vstr, vlen);
			}else{
				//If it is a number
				value = createStringObjectFromLongLong(vlong);
			}
			//We only need to delete an element when it exists.
			subject->ptr = ziplistDelete(subject->ptr, &p);
		}

	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		list *list = subject->ptr;

		listNode *ln;
		
		if(where == REDIS_HEAD){
			ln = listFirst(list);
		}else{
			ln = listLast(list);
		}

		if(ln != NULL){
			value = listNodeValue(ln);
			incrRefCount(value);
			listDelNode(list, ln);
		}
	}else{
		redisPanic("Unknown list encoding");	
	}
	return value;
}

/**
 * Return the number of nodes
 */
unsigned long listTypeLength(robj *subject){

	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		return ziplistLen(subject->ptr);
	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		return listlength((list*)subject->ptr);
	}else{
		redisPanic("Unknown list encoding");	
	}
}

/**
 * Initialize an iterator at the specific index
 * Parameter 'index' indicate where to start
 * Parameter 'direction' indicate which direction to walk
 */
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction){
	listTypeIterator *it = zmalloc(sizeof(listTypeIterator));
	it->subject = subject;	
	it->encoding = subject->encoding;
	it->direction = direction;
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		it->zi = listIndex(subject->ptr, index);
	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		it->ln = listIndex(subject->ptr, index);
	}else{
		redisPanic("Unknown list encoding");	
	}
	return li;
}

/**
 * Clean up the iteator
 */
void listTypeReleaseIterator(listTypeIterator *li){
	zfree(li);
}

/**
 * Stores pointer to current the entry in the provided entry structure and 
 * advances the position of the iterator. Returns 1 when the current entry
 * is in fact an entry, 0 otherwise.
 */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry){
	/*Protect from converting when iterating*/
	redisAssert(li->subject->encoding == li->encoding);

	entry->li = li;
	if(li->encoding == REDIS_ENCODING_ZIPLIST){
		//Record current position
		entry->zi = li->zi;
		//Move the iterator pointer
		if(entry->zi != NULL){
			if(li->direction == REDIS_TAIL){
				zi = ziplistNext(li->subject->ptr, li->zi);
			}else{
				zi = ziplistNext(li->subject->ptr, li->zi);
			}
			return 1;
		}
	}else if(list->encoding == REDIS_ENCODING_LINKEDLIST){
		//record current node
		entry->ln = li->ln;

		//move the iterator pointer
		if(entry->ln != NULL){
			if(li->direction == REDIS_TAIL){
				li->ln = li->ln->next;
			}else{
				li->ln = li->ln->prev;
			}
			return 1;
		}	
	
	}else{
		redisPanic("Unknown list encoding");	
	}
	return 0;
}

/**
 * Return Entry or NULL at the current position of the iterator
 */
robj *listTypeGet(listTypeEntry *entry){
	
	listTypeIterator *li = entry->li;

	robj *value = NULL;
	//Use the listTypeIterator encoding to determine type of the entry
	if(li->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;
		redisAssert(entry->zi != NULL);
		if(ziplistGet(entry->zi, &vstr, &vlen, &vlong)){
			if(vstr){
				value = createStringObject((char *)vstr, vlen);
			}else{
				value = createStringObjectFromLongLong(vlong);
			}
		}
	}else if(li->encoding == REDIS_ENCODING_LINKEDLIST){
		redisAssert(entry->ln != NULL);
		value = listNode(entry->ln);
		incrRefCount(value);
	}else{
		redisPanic("Unknown list encoding");
	}
	return value;
}

/**
 * Insert the value object before or after the given node.
 * Where indicate the position.
 * - REDIS_HEAD before the given node
 * - REDIS_TAIL after the given node.
 */
void listTypeInsert(listTypeEntry *entry, robj *value, int where){
	listTypeIterator *li =	entry->li;

	if(li->encoding == REDIS_ENCODING_ZIPLIST){
		
		//Return decoded value object
		value = getDecodedObject(value);
		
		if(where == REDIS_TAIL){
			unsigned char *next = ziplistNext(entry->subject->ptr, entry->zi);
			
			/**
			 * When we inset after the current element, but the current element is the tail of the
			 * list, we need to do a push.
			 */
			if(next == NULL){
				entry->subject->ptr = zipPush(entry->subject->ptr,value->ptr, sdslen(value->ptr), REDIS_TAIL);
			}else{
				entry->subject->ptr = ziplistInsert(entry->subject->ptr, next, value->ptr, sdslen(value->ptr));
			}
		}else{
			subject->ptr = ziplistInsert(entry->subject->ptr, entry->zi, value->ptr, sdslen(value->ptr));
		}
		decrRefCount(value);
	}else if(li->encoding == REDIS_ENCODING_LINKEDLIST){
		if(where == REDIS_HEAD){
			listInsertNode(li->subject->ptr, entry->ln, value, 0);
		}else{
			listInsertNode(i->subject->ptr, entry->ln, value, 1);
		}
		incrRefCount(value);
	}else{
		redisPanic("Unknown list encoding");	
	}
}

/**
 * Compare the given object with entry at the current position
 * If equals returns 1 otherwise reutrn 0.
 */
int listTypeEqual(listTypeEntry *entry, robj *o){
	
	if(o->encoding == REDIS_ENCODING_LINKEDLIST){
		return equalStringObject(listNodeValue(o->ptr), o);	
	}else if(o->encoding == REDIS_ENCODING_ZIPLIST){
		o = getDecodeObject(o);
		return ziplistCompare(entry->zi, o->ptr, sdslen(o->ptr));
	}else{
		redisPanic("Unknown list encoding");	
	}

}

/**
 * Delete the element pointed to
 */
void listTypeDelete(listTypeEntry *entry){
	
	listTypeIterator *li = entry->li;
	robj *subject = li->subject;

	if(li->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p = entry->zi;
		
		subject->ptr = ziplistDelete(subject->ptr, &p);

		/*
		 * Update position of the iterator depengding on the direction
		 */
		if(li->direction == REDIS_TAIL){
			li->zi = p;
		}else{
			li->zi = zipPrev(subject->ptr, p);
		}

	}else if(li->encoding == REDIS_ENCODING_LINKEDLIST){
		
		//Record the next node
		listNode *next;
		if(li->direction == REDIS_TAIL){
			next = entry->ln->next;
		}else{
			next = entry->ln->prev;
		}
		//Remove current node
		listDelNode(subject->ptr, entry->ln);
		ln = ln->next;
	}else{
		redisPanic("Unknown list encoding");	
	}
}

/**
 * Convert the encoding from ziplist to linked-list
 */
void listTypeConvert(robj *subject, int enc){
	
	listTypeIterator *li;
	
	listTypeEntry entry;

	redisAssertWithInfo(NULL, subject, subject->type == REDIS_LIST);
	
	//convert to double-linked list
	if(enc == REDIS_ENCODING_LINKEDLIST){
		
		list *l = listCreate();
		
		listSetFreeMethod(l, decrRefCountVoid);
		
		//ListType Get returns a robj with increment refcount
		li = listTypeInitIterator(subject, 0, REDIS_TAIL);
		while(listTypeNext(li, &entry)) listAddNodeTail(l, listTypeGet(&entry));
		listTypeReleaseIterator(li);

		//Update the coding
		subject->encoding = enc;
		//Free the original ziplist
		zfree(subject->ptr);

		subject->ptr = l;

	}else{
		redisPanic("Unknown list encoding");	
	}

}

/*---------------------------------------------------------
 *		     List   Command
 * --------------------------------------------------------*/

void pushGenericCommand(redisClient *c, int where){
	int j, waiting = 0, pushed = 0;

	//Get the value object
	robj *lobj = lookupKeyWrite(c->db, c->argv[1]);

	//If the value object is not existed, may be some other client
	//wait this list to come out.
	int may_have_waiting_clients = (lobj == NULL);

	if(lobj && lobj->type != REDIS_LIST){
		addReply(c, shared.wrongtypeerr);		
		return;
	}

	//Set the list status to ready(May be i think this is used to
	//notify all the client that are blocking wating for the list)
	if(may_have_waiting_clients) signalListAsReady(c, c->argv[1]);

	//Add all the value into the list
	for(j = 2; j < c->argc; j++){
		
		//Encode this value object
		c->argv[j] = tryObjectEncoding(c->argv[j]);
		
		//If the list object is not existed, then create one
		if(!lobj){
			lobj = createZiplistObject();
			dbAdd(c->db, c->argv[1], lobj);
		}

		//Push the value into the list
		listTypePush(lobj, c->argv[j], where);
		
		pushed++;
	}
	//Add some reply
	addReplyLongLong(c, waiting + (lobj ? listTypeLength(lobj) : 0));

	//If at least one value pushed successful
	if(pushed){
		char *event = (where == REDIS_HEAD) ? "lpush" : "rpush";

		//Send the key modify signal
		signalModifiedKey(c->db, c->argv[1]);

		//Send event notification
		notifyKeySpaceEvent(REDIS_NOTIFY_LIST, event, c->argv[1], c->db->id);
	}
	server.dirty += pushed;
}

void lpushCommand(redisClient *c){
	pushGenericCommand(c, REDIS_HEAD);
}

void rpushCommand(redisClient *c){
	pushGenericCommand(c, REDIS_TAIL);
}

/**
 * Pushed a value to an existed list.
 */
void pushxGenericCommand(redisClient *c, robj *refval, robj *val, int where){
	robj *subject;
	listTypeIterator *iter;
	listTypeEntry entry;
	int inserted = 0;

	//Get the key object
	if((subject = lookupKeyReadOrReply(c, c->argv[1], shared.zero)) == NULL || 
			!checkType(c, subject, REDIS_LIST)) return;
	//If execute the linsert command
	if(refval){
		/**
		 * We are not sure if this value can be inserted yet, but we cannot
		 * convert the list inside the iterator. We don't want to loop over
		 * the list twice(once to see if the value can be inserted and once
		 * to do the actual insert), so we assume this value can be inserted 
		 * and convert the ziplist to a regular list if necessary.
		 */
		listTypeTryConversion(subject, val);

		iter = listTypeInitIterator(subject, 0, REDIS_TAIL);
		while(listTypeNext(iter, &entry)){
			if(listTypeEqual(&entry, refval)){
				//Find that, do the insert operation.
				listTypeInsert(&entry, val, where);
				inserted = 1;
				break;
			}
		}
		listTypeReleaseIterator(iter);

		if(insert){
			//Check after the insert if the list need to convert to a double-linked list
			if(subject->encdoing == REDIS_ENCODING_ZIPLIST && ziplistLen(subject->ptr) > server.list_max_ziplist_entries)
				listTypeConvert(subject, REDIS_ENCODING_LINKEDLIST);

			signalModifiedKey(c->db, c-argv[1]);

			notifyKeySpaceEvent(REDIS_NOTIFY_LIST, "linsert", c->argv[1], c->db->id);

			server.dirty++;
		}else{
			//Notify client of failed insert
			addReply(c, shared.cnegone);
			return;
		}
	}else{
		char *event = (where == REDIS_HEAD) ? "lpush" : "rpush";

		listTypePush(subject, val, where);
		
		signalModifiedKey(c->db, c->argv[1]);

		notifyKeySpaceEvent(REDIS_NOTIFY_LIST, event, c->argv[1], c->db->id);

		server.dirty++;
	}

	addReplyLongLong(c, listTypeLength(subject));
}

void lpushxCommand(redisClient *c){
	c->argv[2] = tryObjectEncoding(c->argv[2]);
	pushxGenericCommand(c, NULL, c->argv[2], REDIS_HEAD);
}
	
void rpushxCommand(redisClient *c){
	c->argv[2] = tryObjectEncoding(c->argv[2]);
	pushxGenericCommand(c, NULL, c->argv[2], REDIS_TAIL);
}

void linsertCommand(redisClient *c){
	
	//Encoding the refval object
	c->argv[4] = tryObjectEncoding(c->argv[4]);

	if(strcasecmp("before", c->argv[2]->ptr) == 0){
		pushGenericCommand(c, c->argv[3], c->argv[4], REDIS_HEAD);
	}else if(strcasecmp("after", c->argv[2]->ptr) == 0){
		pushGenericCommand(c, c->argv[3], c->argv[4], REDIS_TAIL);
	}else{
		addReply(c, shared.syntaxerr);
	}
}

void llenCommand(redisClient *c){
	robj *subject;
	if((subject = lookupKeyReadOrReply(c, c->argv[1], shared.zero)) == NULL || 
			!checkType(c, subject, REDIS_LIST)) return;
	addRepplyLongLong(c, listTypeLength(subject));
}

void lindexCommand(redisClient *c){
	
	robj *subject = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk);
	robj *value;
	if(subject == NULL || checkType(c, subject, REDIS_LIST)) return;

	long index;
	//get the index
	if(getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK){
		return;
	}

	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p;
		unsigned char *vstr;
		unsigned int vlen;
		long long vlong;

		p = ziplistIndex(subject->ptr, index);
		if(ziplistGet(p, &vstr, &vlen, &vlong)){
			if(vstr){
				value = createStringObject((char *)vstr, vlen);
			}else{
				value = createStringObjectFromLongLong(vlong);
			}
			addReplyBulk(c, value);
			decrRefCount(value);
		}else{
			addReply(c, shared.nullbulk);
		}
	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		listNode n = listIndex(subject->ptr, index);
		if(n == NULL){
			addReply(c, shared.nullbulk);
		}else{
			value = listNodeValue(n);	
			addReplyBulk(c, value);
		}
	}else{
		redisPanic("Unknown list encoding");	
	}
}

void lsetCommand(redisClient *c){
	
	robj *subject;
	
	subject = lookupKeyWriteOrReply(c, c->argv[1], shared.nokeyerr);

	if(subject == NULL || checkType(c, subject, REDIS_LIST)) return;

	long index;

	if(getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK) return;

	robj *value = (c->argv[3] = tryObjectEncoding(c->argv[3]));

	if(index >= listTypeLength(subject)){
		addReply(c, shared.outofrangeerr);
		return;
	}
	
	//check if it needs encoding conversion
	listTypeTryConversion(subject, value);

	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p;
		//find the index
		p = ziplistIndex(zl, index);
		//Delete the orignal value
		subject->ptr = ziplistDelete(subject->ptr, &p);
		//Insert the new vakue
		value = getDecodedObject(value);
		subject->ptr = ziplistInsert(subject->ptr, value->ptr, sdslen(value->ptr));
		decrRefCount(value);

		addReply(c, shared.ok);
		signalModifiedKey(c->db, c->argv[1]);
		notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "lset", c->argv[1], c->db->id);
		server.dirty++;

	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		listNode ln = listIndex(subject->ptr, index);
		decrRefCount((robj*)listNodeValue(ln));

		//point to the new object
		listNodeValue(ln) = value;
		incrRefCount(value);

		addReply(c, shared.ok);
		signalModifiedKey(c->db, c->argv[1]);
		notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "lset", c->argv[1], c->db->id);
		server.dirty++;
	}else{
		redisPanic("Unknown list encoding");	
	}
}

/**
 * This is a generic pop command method for the list.
 */
void popGenericCommand(redisClient *c, int where){
	
	robj *subject = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk);
	if(subject == NULL || checkType(c, subject, REDIS_LIST)) return;
	/**
	 * If this key is existed, then we need to figure out which kind of 
	 * implemetation.
	 */
	robj *value;
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p, *vstr;
		unsigned long vlen;
		long vlong;

		p = ziplistIndex(subject->ptr, where == REDIS_HEAD ? 0 : -1);
		if(ziplistGet(p, &vstr, &len, &vlong)){
				if(vstr){
					//If it is a string in this ziplist entry
					value = createStringObject((char *)vstr, vlen);
				}else{
					//If it is a long value in the ziplist
					value = createStringObjectFromLongLong(vlong);
				}
		}else{
			//If there is nothing to pop;
			addReply(c, shared.nullbulk);
			return;
		}
		

	}else if(subject->encoding == REDIS_ENCDOING_LINKEDLIST){
			listNode *ln;
			if(ln = listIndex(subject->ptr, where == REDIS_HEAD ? 0 : -1){
				value = listNodeValue(ln);
				incrRefCount(value);
				listDelNode(list, ln);
			}else{
				//Nothing in the linkedlist
				addReply(c. shared.nullbulk);
				return;
			}
	}else{
		redisPanic("Unknown list encoding");	
	}
	//At this part notice, we leave the value refcount to here 
	//and do the decrRef work after we pass the value to addReply func.
	addReply(c, value);
	decrRefCount(value);

	char *event = (where == REDIS_HEAD) ? "lpop" : "rpop";
	notifyKeyspaceEvent(REDIS_NOTIFY_LIST, event, c->argv[1], c->db->id);
	if(listTypeLength(subject) == 0){
		notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1]. c->db->id);
		dbDelete(c->db, c->argv[1]);
	}
	signalModifiedKey(c->db, c->argv[1]);
	server.dirty++;
}

void lpopCommand(redisClient *c){
	popGenericCommand(c, REDIS_HEAD);
}

void rpopCommand(redisClient *C){
	popGenericCommand(c, REDIS_TAIL);
}

void lrangeCommand(redisClient *c){
	
	long start, end, llen, rangelen;
	robj *subject = lookupKeyReadOrReply(c->argv[1], shared.nullbulk);
	
	if(subject == NULL || checkType(c, subject, REDIS_LIST)) return;

	//get the start and end
	if((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) || 
	   (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

	llen = listTypeLength(subject);

	/*convert negative number*/
	if(start < 0) start += llen;
	if(end < 0) end += llen;
	//If the start still less than 0, make it start at index 0.
	if(start < 0) start = 0;

	/**
	 * Invariant: start >= 0, so the test will be true when end < 0.
	 * The range is empty when start > end or start >= length
	 */
	if(start >= end || start >= llen){
		addReply(c, shared.emptymultibulk);
		return;
	}

	if(end >= llen) end = llen - 1;	
	//The rangelen determine the iteration times
	ranglen = end - start + 1;

	/*Return the result in form of a multi-bulk reply*/
	addReplyMutiBulkLen(c, rangelen);

	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p, *vstr;
		long vlen;
		long long vlong;
		robj *value;
		p = ziplistIndex(subject->ptr, start);
		while(rangelen--){
			ziplistGet(p, &vstr, &vlen, &vlong);		
			if(vstr){
				value = createStringObject((char *)vstr, vlen);
			}else{
				value = createStringObjectFromLongLong(vlong);
			}	
			addReply(c, value);
			p = ziplistNext(subject->ptr, p);
		}
	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		listNode *ln;
		//If the start point start near the end,
		//then we start out loop from the tail.
		if(start > llen/2) start -= llen;
		ln = listIndex(subject->ptr, start);
		while(rangelen--){
			addReplyBulk(c, ln->value);
			ln = ln->next;
		}
	}else{
		redisPanic("Unknown list encoding");	
	}
}

void ltrimCommand(redisClient *c){

	long start, end, llen, j, ltrim, rtrim; 
	robj *subject = lookupKeyWriteOrReply(c, c->argv[1], shared.ok);
	
	if(subject == NULL || checkType(c, subject, REDIS_LIST)) return;

	if((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) || 
	   (getLongFrmObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

	llen = listTypeLength(subject);
	
	if(start < 0) start += llen;
	if(end < 0) end += llen;
	if(start < 0) start = 0;

	if(start > end || start >= llen){
		//Remove the whole list
		ltrim = llen;
		rtrim = 0;
	}else{
		if(end >= llen) end = llen - 1;
		ltrim = start;
		rtrim = llen - end - 1;
	}
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		unsigned char *p;
		//Delete left elements
		o->ptr = ziplistDeleteRange(zl, 0, ltrim);
		//Delete right elements
		o->ptr = ziplistDeleteRange(zl, -rtrim, rtrim);

	}else if(subject->encoding == REDIS_ENCODING_LINKEDLIST){
		listNode *next;
		while(ltrim--){
			ln = listFirst(subject->ptr);
			listDelNode(subject->ptr, ln);
		}
		while(rtrim--){
			ln = listLast(subject->ptr);
			listDelNode(subject->ptr, ln);
		}
	}else{
		redisPanic("Unknown list encoding");	
	}

	//Notify 
	notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "ltrim", c->argv[1], c->db->id);

	//if the list is empty, delete it
	if(listTypeLength(subject) == 0){
		dbDelete(c->db, c->argv[1]);
		notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
	}

	signalModifiedKey(c->db, c->argv[1]);

	server.dirty++;

	addReply(c, shared.ok);
}

void lremCommand(redisClient *c){

	long index;
	robj *o, *subject = lookupKeyWriteOrReply(c, c->argv[1], shared.ok);
	long removed = 0;
	listTypeEntry entry;

	if(subject == NULl || checkType(c, subject, REDIS_LIST)) return;

	if(getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != REDIS_OK) return;
	
	o = (c->argv[3] = tryObjectEncoding(c->argv[3]));

	/**
	 * Make sure this is raw when we use ziplist 
	 */
	if(subject->encoding == REDIS_ENCODING_ZIPLIST){
		o = getObjectDecode(o);
	}

	listTypeIterator *li;

	if(index < 0){
		index = -index;
		li = listTypeInitIterator(subject, 0, REDIS_HEAD);
	}else{
		li = listTypeInitIterator(subject, -1, REDIS_TAIL);
	}

	while(listTypeNext(li, &entry)){
		if(listTypeEqual(&entry, o)){
			listTypeDelete(&entry);
			server.dirty++;
			removed++;
			//If the removed element reach the required
			if(index && toremoved == index) break;
		}	
	}
	listTypeReleaseIterator(li);

	//If this list is empty then remove it
	if(listTypeLength(subject) == 0) dbDelete(c->db, c->argv[1]);

	addReplyLongLong(c, removed);
	if(removed) signalModifiedKey(c->db, c->argv[1]);
}

void rpoplpushHandlePush(redisClient *c, robj *dstkey, robj *dstobj, robj *value){
	/* Create the destination list if it is not existed*/
	if(!dstobj){
		dstobj = createZiplistObject();
		dbAdd(c->db, dstkey, dstobj);
		signalListAsReady(c, dstkey);
	}

	signalModifiedKey(c->db, dstkey);

	//push value into the dst list
	listTypePush(dstobj, value, REDIS_HEAD);

	notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "lpush", dstkey, c->db->id);

	addReply(c, value);
}

/**
 * This is the semantic of this command
 *   RPOPLPUSH srclist dstlist
 *   	IF LLEN(srclist) > 0
 *   		element = RPOP srclist
 * 			LPUSH dstlist element
 * 	  	ELSE
 * 			return NULL
 *  	END
 *	 END  
 *   
 *   The idea is to be able to get an element from a list in a reliable way since
 *   the element is not just returned but pushed against another list as well.
 */
void rpoplpushCommand(redisClient *c){
	robj *sobj, *value;

	//Get the src list
	if((sobj = lookupKeyWriteOnlyOrReply(c, c->argv[1], shared.nullbulk)) == NULL || 
	   checkType(c, sobj, REDIS_LIST)) return;

	//If source list is empty
	if(listTypeLength(sobj) == 0){
		addReply(c, shared.nullbulk);
	}else{

		//The dst object
		robj *dobj = lookupKeyWrite(c->db, c->argv[2]);
		robj *touchedkey = c->argv[1];

		//check if the dst object existed && is not list
		if(dobj && checkType(sobj, REDIS_LIST)) return;

		value = listTypePop(sobj, REDIS_TAIL);
		/**
		 * We save the c->argv[2] and also incr the ref-count, 
		 * That's because in the early version of redis, there
		 * is no function like notifyKeyspaceEvent, the early
		 * impletataion of the similiar function will change the
		 * client arguement vector, so we save the c->argv[2] before
		 * we enter the rpoplpushHandlePush Function.
		 */
		rpoplpushHandlePush(c, c->argv[2], dobj, value);

		/*listTypePop returns an object with its refcount decrement*/
		decrRefCount(value);

		/*Delete the source list when it is empty*/
		notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "rpop", touchedkey, c->db->id);

		//If the source is empty, then delete it
		if(listTypeLength(sobj) == 0){
			dbDelete(c->db, touchedkey);
			notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", touchedkey, c->db->id);
		}
		signalModifiedKey(c->db, touchedkey);
		decrRefCount(touchedkey);
		server.dirty++;
	}
}

/*----------------------------------------------------------------------*
 *  Blocking POP operation
 *----------------------------------------------------------------------*/

/**
 * This is how the current blocking POP works, we use BLPOP as example.
 * 
 * We use the BLPOP as an example.
 * 
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP 
 *   if blocking is not required.
 * 
 * - If instead BLPOP is called and the key does not exists or the list is 
 *   empty we need to block. In order to do so we remove the notification 
 *   for new data to read in the client socket(so that we'll not serve new 
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary(db->blocking_keys)  mapping keys to a list of clients
 *   blocking fo this keys.  
 * 
 * - If a PUSH operation against a key with blocked clients waiting is performed,
 *   we mark this key as "ready", and after the current command, MULTI/EXEC blocks,
 *   or scriptm is executed, we serve all the clients waiting for this list, from
 *   the one that blocked first, to the last, accordingly to the number of elements 
 *   we have in the ready list.  
 */

/**
 * Set a client in blocking mode for the specified key, with the specified timeout.
 * keys:   arbitary number of keys.
 * numkeys: number of keys
 * timeout: the blocking time.
 * target: when the blocking is canceled, save the result to the key object. 
 */
void blockForKeys(redisClient *c, robj **keys, int numkeys, mstime_t timeout, robj *target){
	dictEntry *de;
	list *l;
	int j;

	//set the blocking status adn the target
	c->bpop.timeout = timeout;

	//target is used when the RPOPLPUSH is executed
	c->bpop.target = target;

	if(target != NULl) incrRefCount(target);

	//Associate the info with the client.
	//Here we use two mapping:
	//Map key: client -> value : all the keys that are blocking
	//Map key: key -> value: client that block by this key.
	for(j = 0; j < numkeys; j++){
		/*If the key already exist in the dict then ingore it*/
		if(dictAdd(c->bpop.keys, keys[j], NULL) != REDIS_OK) continue;
		//The key object is referenced in the c->bpop map value part,
		//we incr the reference count.
		incrRefCount(keys[j]);

		de = dictFind(c->db->blocking_keys, keys[j]);
		if(de == NULL){
			//The linked list is not existed, create a new one.
			l = listCreate();
			retval = dictAdd(c->db->blocking_keys, keys[j], l);
			incrRefCount(keys[j]);
			redisAssertWithInfo(c, keys[j], retval == DICT_OK);
		}else{
			l = dictGetVal(de);
		}
		//Add the client to the key blocking list
		listAddNodeTail(l, c);
	}
	blockClient(c, REDIS_BLOCKED_LIST);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP 
 * You should never call this function directly but unblockClient() instead 
 */
void unblockClientWaitingData(redisClient *c){
	dictEntry *de;
	dictIterator *di;
	list *l;

	redisAssertWithInfo(c, NULL, dictSize(c->bpop.key) != 0);
	/*This client may be waiting for mutpile keys, so we unlock it for ever key*/
	di = dictGetIterator(c->bpop.keys);

	while((de = dictNext(di)) != NULL){
		robj *key = dictGetKey(de);

		//Remove this client from the list of clients wating for this key
		l = dictFetchValue(c->db->blocking_keys, key);

		redisAssertWithInfo(c, key, l != NULL);

		//Delete client from the wating list
		listDelNode(l, listSearchKey(l, c)); 
		//If the list is empty we need to remove it to avoid memory waste.
		if(listLength(l) == 0)
			dictDelete(c->db->blocking_keys, key);
	}
	dictRelease(di);

	/*Cleanup the client structure*/
	dictEmpty(c->bpop.keys, NULL);
	if(c->bpop.target){
		decrRefCount(c->bpop.target);
		c->bpop.target = NULL;
	}
}

/**
 * If the specificed key has clients blocked waiting for list push, this 
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of mutiple pushes made by
 * script or in the context of MULTI/EXEC
 */
void signalListAsReady(redisClient *c, robj *key){
	readyList *rl;

	/*If there is no any client block for this key, return*/
	if(dictFind(c->db->blocking_keys, key) == NULL) return;

	/*If this key was already signaled? No need to queue it again*/
	if(dictFind(c->db->ready_keys, key) != NULL) return;

	/*Now we need to queue this key into server.readyList
     *We need to create a readList.
	 *Init this readyList.
	 *Add this readyList to server.ready_keys.
	 */
	rl = zmalloc(sizeof(*rl));
	rl->key = key;
	rl->db = c->db;
	//This is because the key is referenced by rl->key
	incrRefCount(key);
	listAddNodeTail(server.ready_keys, rl);
	/**
	 * We also add the key in the db->ready_keys dictionary 
	 * to avoid adding it mutiple times into a list with a
	 * simple O(1) check.
	 */
	incrRefCount(key);
	redisAssert(dictAdd(c->db->ready_keys, key, NULL) == REDIS_OK);
}

/**
 * This is a helper function for handleClientsBlockedOnList(). It's work
 * is to serve a specific client that is blocked on 'key' in the context
 * of the specified  'db', doing the following:
 * 
 * 1) provide the client with the 'value' element
 * 
 * 2) If the dstkey is not NULL(we are serving a BRPOPLPUSH) also push the 
 *    'value' element on the destination list(the LPUSH side of command).
 * 
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 * 
 * The arguement 'where' is REDIS_TAIL or REDIS_HEAD, and indicates if the 'value'
 * element was poped from the head(BLPOP) or tail(BRPOP)
 * 
 * This function returns REDIS_OK  if we are able to serve the client, otherwise
 * REDIS_ERR is returned to signal the caller that the list POP operation should
 * be undone as the client was not served: This only happens for BRPOPLPUSH that
 * fails to push the value to the destination key as it is of the wrong type.
 */
int serveClientBlockedOnList(redisClient *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where){
	robj *argv[3];

	//If we execute the BLPOP or BRPOP
	if(dstkey == NULL){
		/*Propagate the [L|R]POP operation*/
		argv[0] = (where == REDIS_HEAD) ? shared.lpop : shared.rpop;

		argv[1] = key;
		propagate((where == REDIS_HEAD) ? 
					server.lpopCommand : server.rpopCommand,
					db->id, argv, 2 REDIS_PROPAGATE_AOF|REDIS_PROPAGATE_REPL);
		/*BRPOP/BLPOP*/
		addReplyMultiBulkLen(receiver, 2);
		addReplyBulk(receiver, key);
		addReplyBulk(receiver, value);
	}else{
		//Here execute the BRPOPLPUSH

		//Get the target object
		rob *dstobj = lookupKeyWrite(receiver->db, dstkey);
		if(!(dstobj && checkType(receiver, dstobj, REDIS_LIST))){
			/*Propagate the RPOP operation*/
			argv[0] = shared.pop;
			argv[1] = key;
			propagate(server.rpopCommand, 
					  db->id, argv, 2
					  REDIS_PROPAGATE_AOF|
					  REDIS_PROPAGAGTE_REPL);

			//Push the value into dstobj, if the dstobj is not existed,
			//then create a new one.
			rpoplpushHandlePush(receiver, dstkey, dstobj, value);
			//Propagate the LPUSH operation
			propagate(server.lpushCommand, 
					  db->id, argv, 3
					  REDIS_PROPAGATE_AOF|
					  REDIS_PROPAGATE_REPL);
		}else{
			/*
			 *BRPOPLPUSH failed because of wrong destination type
			 */
			return REDIS_ERR;
		}
	}
	return REDIS_OK;
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * 这个函数会在 Redis 每次执行完单个命令、事务块或 Lua 脚本之后调用。
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. 
 *
 * 对所有被阻塞在某个客户端的 key 来说，只要这个 key 被执行了某种 PUSH 操作
 * 那么这个 key 就会被放到 serve.ready_keys 去。
 * 
 * 这个函数会遍历整个 server.ready_keys 链表，
 * 并将里面的 key 的元素弹出给被阻塞客户端，
 * 从而解除客户端的阻塞状态。
 *
 * 函数会一次又一次地进行迭代，
 * 因此它在执行 BRPOPLPUSH 命令的情况下也可以正常获取到正确的新被阻塞客户端。
 */
void handleClientsBlockedOnLists(void){

	//Loop through the ready key list
	while(listLength(server.ready_keys) != 0){
		list *l;
		/**
		 * Point server.ready_keys to a fresh list and save the current
		 * one locally.This way as we run the old list we are free to call
		 * signalListAsReady() that may push new elements in server.ready_keys
		 * when the client handling clients blocked into BRPOPLPUSH
		 */ 
		l = server.ready_keys;
		server.ready_keys = listCreate();

		while(listLength(l) != 0){
			
			//Get the first node from the ready_keys
			listNode *ln = listFirst(l);

			//Point to the readyList structure
			readyList *rl = ln->value;

			/*
			 *First of all remove this key from db->ready_keys so that
			 *we can safely call signalListAsReady() against this key. 
			 */
			dictDelete(rl->db->ready_keys, rl->key);

			/*If the key exists and it is a list, serve blocked clients with data*/
			robj *o = lookupKeyWrite(rl->db, rl->key);
			if(o!= NULL && o.type == REDIS_LIST){
				/*Now we need to unblock all the clients which is blocked by this list
				 *All the blocked clients is arrange in the db->blocking_keys value part,
				 *which is a list of client.
				 */
				dictEntry *de;
				de = dictFind(rl->db->blocking_keys, rl->key);
				if(de){
					list *clients = dictGetVal(de);
					int numclients = listLength(clients);

					while(numclients--){
						//Get the client
						listNode *clientNode = listFirst(clients);
						redisClient *receiver = listNodeValue(clientNode);

						//get this target object
						robj *dstkey = receiver->bpop.target;

						//pop the element from the list
						//Where to pop according to the BLPOP or BRPOP or BRPOPLPUSH
						int where = (receiver->lastcmd &&
						             receiver->kastcmd->proc == blpopCommand) ? 
									 REDIS_HEAD : REDIS_TAIL;
						robj *value = listTypePop(o, where);

						//If there is any element to pop
						if(value){
							/**
							 * Protect receiver->bpop.target, that will be 
							 * freed by the next unblockClient().
							 */
							if(dstKey) incrRefCount(dstKey);

							unblockClient(receiver);

							//put the value to the receiver blocking key
							if(serveClientBlockedOnList(receiver, rl->key,
							                             dstkey,rl->db, value,
														 where) == REDIS_ERR)
							{
								/**
								 * If we failed serving the client we need to
								 * also undo the POP operation.
								 */
								 listTypePush(o, value, where);
							}
							if(dstKey) decrRefCount(dstKey);
							decrRefCount(value);
						}else{
							//When comes here is means may have other clients
							//still need to be blocking for the next PUSH.
							break;
						}
					}
				}
				//If the list is empty then delete it.
				if(listTypeLength(o) == 0) dbDelete(rl->db, rl->key);
				/**
				 * We do not call signalModifiedKey() as it was already
				 * called when an elments was push on the list.
				 */ 
			}
			/*Free this item*/
			decrRefCount(rl->key);
			zfree(rl);
			listDelNode(l, ln);
		}
		listRelease(l);
	}
}

void blockingPopGenericCommand(redisClient *c, int where){
	robj *o;
	mstime_t timeout;
	int j;

	//Get the timeout
	if(getTimeoutFromObjectOrReply(c, c->argv[c->argc-1], &timeout, UNIT_SECONDS) != REDIS_OK) return;

	//Loop through all the key
	for(j = 1; j < c->argc - 1; j++){
		//Get the list key
		o = lookupKeyWrite(c->db, c->argv[j]);

		if(o != NULL){
			if(o->type == REDIS_LIST){
				addReply(c, shared.wrongtypeerr);
				return;
			}else{
				//If we meet a non empty list, we pop it and treat this command
				//like R|LPOP
				if(listTypeLentgh(o) != 0){
					//Non empty list, this is like a non normal
					char *event = (where == REDIS_HEAD) ? "lpop" : "rpop";

					//Pop value
					robj *value = listTypePop(o, where);

					redisAssert(value != NULL);
					//Reply to the client;
					addReplyMultiBulkLen(c, 2);
					//Add the pop element list
					addReplyBulk(c, c->argv[j]);
					//reply the pop value
					addReplyBulk(c, value);

					decrRefCount(value);

					notifyKeyspaceEvent(REDIS_NOTIFY_LIST, event, c->argv[j], c->db->id);

					//Delete the empty list
					if(listTypeLength(o) == 0){
						dbDelete(c->db, c->argv[j]);
						notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c->argv[j], c->db->id);
					}

					signalModifiedKey(c->db, c->argv[j]);
					server.dirty++;

					/*Replicate it as an [LR]pop instead of B[LR]POP*/
					rewriteClientCommandVector(c, 2, 
								(where == REDIS_HEAD) ? shared.lpop : shared.rpop, 
								c->argv[j]);
					return;
				}
			}
		}
	}

	//If we are inside a MUTI/EXEC and the list is empty the only thing we can do
	//is treating it as a timeout.
	if(c->flags & REDIS_MULTI){
		addReply(c, shared.nullmutibulk);
		return;
	}

	/*If the list is empty or the key does not exists we must block*/
	blockForKeys(c, c->argv + 1, c->argc - 2, timeout, NULL);
}

void blpopCommand(redisClient *c){
	blockingPopGenericCommand(c, REDIS_HEAD);
}

void brpopCommand(redisClient *c){
	blockingPopGenericCommand(c, REDIS_TAIL);
}

void brpoplpushCommand(redisClient *c){
	mstime_t timeout;

	//Get the timeout parameter
	if(getTimeoutFromObjectOrReply(c, c->argv[3], &timeout, UNIT_SECONDS) != REDIS_OK) return;

	//get the list key
	robj *key = lookupKeyWrite(c->db, c->argv[1]);

	//If the key is empty then block.
	if(key == NULL){
		if(key == NULL){
			if(c->flags & REDIS_MULTI){

			}else{
				blockForKeys(c, c->argv + 1, 1, timeout, c->argv[2]);
			}
		}
	}else{
		if(key->type != REDIS_LIST){
			/**
			 * Blocking against an empty list in a multi state returns immediately
			 */ 
			addReply(c, shared.nullbulk);
		}else{
			//Treat is as a normal rpoplpushCommand
			redisAssertWithInfo(c, key, listTypeLength(key) > 0);

			rpoplpushCommand(c);
		}
	}
}


