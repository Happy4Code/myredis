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
	if(subjet->encoding != REDIS_ENCODING_ZIPLIST) return;

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
	if(sub->encoding == REDIS_ENCODING_ZIPLIST && ziplistLen(subject->ptr) >= server.list_max_ziplist_entries)
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
		//But fo the linked list, it is different, linked list entry has an value field, 
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
	signalListAsReady(c, c->argv[1]);

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

void popGenericCommand(redisClient *c, int where){
	
}

void lpopCommand(redisClient *c){
	popGenericCommand(c, REDIS_HEAD);
}

void rpopCommand(redisClient *C){
	popGenericCommand(c, REDIS_TAIL);
}






















