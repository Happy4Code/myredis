#include "redis.h"
#include <math.h>

/*
 **************************************************************
 *		   String Commond    			      *
 **************************************************************
 */

#define REDIS_SET_NO_FLAGS 0
#define REDIS_SET_NX (1 << 0)   //If this key not exists
#define REDIS_SET_XX (1 << 1)   //If this key exists

/**
 * Check given string length exceed 512MB
 * If exceed return REDIS_ERR, otherwise return REDIS_OK 
 */
static int checkStringLength(redisClient *c, long long size){
	if(size > 512 * 512 * 1024){
		addReplyError("string exceeds maximum allowed size (512MB)");
		return REDIS_ERR;
	}
	return REDIS_OK
}

/**
 * The setGenericCommand function implements the SET operation with different
 * options adn variant. This function is called in order to implement the following
 * commands: SET, SETNX, SETXX, PSETEX.
 *
 * 'flags' changes the behavior of the command(Which can be NX or XX)
 *
 * expire represent an expire to set in form of a Redis Object as passed by the
 * use.It is interpred accoring tp the specific unit.
 *
 * ok_repky and abort_reply is what the function will reply to the client if the operation
 * is performed, or when it is not because of NX or XX flags.
 *
 * If ok_reply is a NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used.
 */

void setGenericCommand(redisClient *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply){
	
	long long milliseconds = 0; //Initialized to aviod any harmness warning

	//If the expire time existed, then extract them.
	if(expire){
		
		//Get the long long value corresponding to the expire object
		if(getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != REDIS_OK)
			return;

		//When the milliseconds parameter is incorrect
		if(milliseconds <= 0){
			addReplyError(c, "Invaild expire time in SETEX");
			return;
		}
		
		//No matter the expire input is seconds or milliseconds
		//Redis save them all as milliseconds
		if(unit == UNIT_SECONDS) milliseconds *= 1000;
	}

	//If set with the NX or XX option, then to check the key meet 
	//the options.
	if((flags & REDIS_SET_NX && lookupKeyWrite(c->db, key) != NULL) ||
	   (flags & REDIS_SET_XX && loopupKeyWrite(c->db, key) == NULL)){
		addReply(c, abort_reply ? abort_reply : shared.nullbulk);
		return;
	}

	//Set the key to database
	setKey(c->db, key, val);

	//set the database flag as dirty
	server.dirty++;

	if(expire) setExpire(c->db, key, mstime() + milliseconds);

	//Send the notification
	notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "set", key, c->db->id);

	//send event notification
	if(expire) notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "expire", key, c->db->id);

	//Set success, reply to the client
	addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* Set key value [NX] [XX] [EX <secongd>] [PX <milliseconds>]*/
void setCommand(redisClient *c){
	int j;
	robj *expire = NULL;
	int unit = UNIT_SECONDs;
	int flags = REDIS_SET_NO_FLAGS;

	//Set optional parameters
	//For a set command
	//argc[0]  set
	//argc[1]  key
	//argc[2]  value
	//.....options
	
	//Notice the argv is an array of robj
	for(j = 3; j < argc - 1; j++){
		char *a = c->argv[j]->ptr;
		robj *next = (j == c->argc - 1) ? NULL : c->argv[j+1];

		if((a[0] == 'n' || a[0] == 'N') &&
		   (a[1] == 'x' || a[1] == 'X' && a[2] == '\0')){
			flags |= REDIS_SET_NX;
		}else if((a[0] == 'x' || a[0] == 'X') && 
			 (a[1] == 'x' || a[1] == 'X') && a[2] == '\0'){
			flags | REDIS_SET_XX;
		}else if((a[0] == 'e' || a[0] == 'E') &&
			 (a[1] == 'x' || a[1] == 'X') && next){
			unit = UNIT_SECONDS;
			expire = next;
			j++;
		}else if((a[0] == 'p' || a[0] == 'P') &&
			 (a[1] == 'x' || a[1] == 'X')){
			unit = UNIT_MILLISECONDS;
			expire = next;
			j++;
		}else{
			addReply(c, shared.syntaxerr);
			return;
		}
	}
	//Try to encoding for the input in order to save space.
	c->argv[2] = tryObjectEncoding(c->argv[2]);

	setGenericCommand(c, flags, c->argv[1], c->argv[2], expire, unit, NULL, NULL);
}


/* Set not existed command */
void setnxCommand(redisClient *c){
	c->argv[2] = tryObjectEncoding(c->argv[2]);
	setGenericCommand(c, REDIS_SET_NX, c->argv[1], c->argv[2], NULL, 0, shared.cone, shared.zero);
}

/* Set existed command */
void setexCommand(redisClient *c){
	c->argv[2] = tryObjectEncoding(c->argv[2]);
	setGenericCommand(c, REDIS_SET_NO_FLAGS, c->argv[1], c->argv[3], c->argv[2], UNIT_SECONDS, NULL, NULL);

}

/** Milliseconds set command**/
void psetexCommand(redisClient *c){
	c->argv[3] = tryObjectEncoding(c->argv[2]);
	setGenericCommand(c, REDIS_SET_NO_FLAGS, c->argv[1], c->argv[3], c->argv[2], UNIT_MILLSECONDS, NULL, NULL);
}

int getGenericCommand(redisClient *c){
	robj *o;

	//Try to get the value corresponding to the key
	//If the value is not existed, return NULL
	if((o = lookupKeyReadOrReply(c, c->argv[1], shared.nullbulk)) == NULL) 
		return REDIS_OK;

	//The value is existed
	if(o->type != REDIS_STRING){
		addReply(c, shared.wrongtypeerr);
		return REDIS_ERR;
	}else{
		addReplyBulk(c, o);
		return REDIS_OK;
	}
}

/**
 * Get command
 */
void getCommand(redisClient *c){
	getGenericCommand(c);
}

void getsetCommand(redisClient *c){
	
	//Get and return the key corresponding value object
	if(getGenericCommand(c) == REDIS_ERR) return;

	//Set the new value
	c->argv[2] = tryObjectEncoding(c->argv[2]);

	setKey(c->db, c->argv[1], c->argv[2]);

	//send the notify
	notifyKeySpaceEvent(REDIS_NOTIFY_STRING, "set", c->argv[1], c->db->id);

	server,dirty++;
}

void setrangeCommand(redisClient *c){
	
	robj *o;
	long offset;

	sds value = c->argv[3]->ptr;
	
	//Get the offset parameters
	if(getLongLongFromObjectOrReply(c, c->argv[2], &offset, NULL) != REDIS_OK)
		return;
	
	//Check offset parameter
	if(offset < 0){
		addReplyError(c, "offset is out of range");
		return;
	}
	//Here we use lookupKey and not lookupKeyReadOrReply, the difference between
	//them is the lookupKey will not modify the miss time or hit time, but the 
	//lookupKeyReadOrReply does.
	o = lookupKey(c->db, c->argv[1]);
	
	if(o == NULL){
		
		//Return 0 when setting nothing on a non-existing string
		if(sdslen(value) == 0){
			addReply(c, shared.zero);
			return;
		}

		//Return when the resulting string exceeds allowed size
		if(checkStringLength(REDIS_STRING, offset + sdslen(value)) != REDIS_OK)
			return;
		
		//If the value has no problem, then create an empty object
		o = createObject(REDIS_STRING, sdsempty());
		dbAdd(c->db, c->argv[1], o);
	}else{
		size_t olen;

		if(checkType(c, o, REDIS_STRING)){
			return;
		}
		//Returns existing string length when setting nothing
		olen = stringObjectLen(o);

		if(sdslen(value) == 0){
			addReplyLongLong(c, olen);
			return;
		}
		
		/**
		 * Return when the resulting string exceeds allowed size
		 */
		if(checkStringLength(c, offset+sdslen(value)) != REDIS_OK)
			return;
		//Create a copy when the object is shared or encoded
		o = dbUnshareStringValue(c->db, c->argv[1], o);
	}

	if(sdslen(value) > 0){
		//extend the string object
		o->ptr = sdsgrowzero(o->ptr, oofset+sdslen(value));
		//copy the value to the specific position
		memcpy((char *)o->ptr + offset, value, sdslen(value));

		//send the key modify signal to database
		signalModifiedKey(c->db, c->argv[1]);

		//Send the event notification
		notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "setrange", c->argv[1], c->db->id);
		
		server.dirty++;
	}
	//Returns the total length of o->ptr
	addReplyLongLong(c, sdslen(o->ptr));
}

void getrangeCommand(redisClient *c){
	
	robj *o;
	long start, end;
	char *str, llbuf[32];
	size_t strlen;

	//Get the start parameter
	if(getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) return;

	//Get the end parameter
	if(getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK) return;

	//Look up value by specific key
	if((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptybulk)) == NULL || checkType(c, o, REDIS_STRING)) return;
	//deal with the value object according to encoding
	if(o->encoding == REDIS_ENCODING_INT){
		str = llbuf;
		strlen = ll2string(llbuf, sizeof(llbuf), (long)o->ptr);
	}else{
		str = o->ptr;
		strlen = sdslen(str);
	}
	
	//Convert negative indexes
	if(start < 0) start += strlen;
	if(end < 0) end += strlen;

	if(start < 0) start = 0;
	if(end < 0) end = 0;
	
	if((unsigned)end >= strlen) end = strlen - 1;

	if(start > end){
		addReply(c, shared.emptybulk);
	}else{
		addReplyBulkBuffer(c, (char *)start + str, end - start + 1);
	}
}

void mgetCommand(redisClient *c){
	int j;

	addReplyMultiBulkLen(c, c->argc-1);
	for(j = 1; j < c->argc; j++){
		//find the value by key
		robj *o = lookupKeyRead(c->db, c->argv[j]);
		if(o == NULL){
			//The value is not exist, send reply to client
			addReply(c, shared.nullbulk);
		}else{
			if(o->type != REDIS_STRING){
				addReply(c, shared.nullbulk);
			}else{
				addReply(c, o);
			}
		}
	}
}

void msetGenericCommand(redicClient *c, int nx){
	int j, busykeys = 0;

	//Check input format: mset key1 value1 key2 value2 key3 value3.....
	if((c->argc % 2) == 0){
		addReplyError(c, "wrong number of arguments for MSET");
		return;
	}
	/**
	 * Handle the NX flag. The MSETNX semantic is to return zero and don't
	 * set anything at all if at least one already key exists.
	 */
	if(nx){
		for(j = 1; j < argc; j += 2){
			if(lookupKeyWrite(c->db, c->argv[j]) != NULL){
				busykeys++;
			}
		}
		
		//If the keys existed, do reply 0
		if(busykeys){
			addReply(c, shared.czero);
			return;
		}
	}

	//Set all the key-value pairs
	for(j = 1; j < c->argc; j+=2){
		
		//Encoding fo the object
		c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);

		//Set keys
		setKey(c->db, c->argv[j], c->argv[j+1]);

		notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "set", c->argv[j], c->db->id);
	}

	server.dirty += (c->argc - 1) / 2;

	//Set success
	//mset return ok, msetnx returns 1
	addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(redisClient *c){
	msetGenericCommand(c, 0);
}

void msetnxCommand(redisClient *c){
	msetGenericCommand(c, 1);
}

void incrDecrCommand(redisClient *c, long long incr){
	long long value, oldvalue;
	robj *o, *new;

	o = lookupKeyWrite(c->db, c->argv[1]);
	//get the value object
	if(o != NULL && checkType(c, o, REDIS_STRING)) return;

	//Get value object and put it into the value variable.If failed then return error msg
	if(getLongLongFromObjectOrReply(c, o ,&value, NULL) != REDIS_OK) return;

	//to do the operation check to prevent from overflow
	oldvalue = value;
	if((incr < 0 && oldvalue < 0 && incr <(LLONG_MIN - oldvalue)) || 
	   (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX - oldvalue))){
		addReplyError(c, "increment or decrement would overflow");
		return;
	}

	//If the operation would not cause overflow then do the operation
	value += incr;
	new = createStringObjectFromLongLong(value);
	if(o){
		doOverwrite(c->db, c->argv[1], new);
	}else{
		dnAdd(c->db, c->argv[1], new);
	}
	
	//Send msg to the databse
	signalModifiedKey(c->db, c->argv[1]);

	//Send the event notify
	notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "incrby", c->argv[1], c->db->id);

	//set the server to be dirty
	server.dirty++;

	//Reply
	addReply(c, shared.colon);
	addReply(c, new);
	addReply(c, shared.crlf);
}

void incrCommand(redisClient *c){
	incrDecrCommand(c, 1);
}

void decrCommand(redisClient *c){
	incrDecrCommand(c, -1);
}

void incrbyCommand(redisClient *c){
	long long incr;

	if(getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
	incrDecrCommand(c, incr);
}

void decrbyCommand(redisClient *c){
	long long incr;

	if(getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;
	
	incrDecrCommand(c, -incr);
}

void incrbyfloatCommand(redisClient *c){
	long double incr, value;
	robj *o, *new, *aux;

	//get the value object
	o = lookupKeyWrite(c->db, c->argv[1]);

	//Check the value type and the type
	if(o != NULL && checkType(c, o, REDIS_STRING)) return;

	//put the value object in the 'value'
	if(getLongDoubleFromObjectOrReply(c, o, &value, NULL) != REDIS_OK || 
	   getLongDoubleFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK)
		return;

	//do the add operation
	value += incr;
	if(isnan(value) || isinf(value)){
		addReplyError(c, "increment would product Nan or Infinity");
		return;
	}
	
	//Use a new value object to replace current value object.
	new = createStringObjectFromLongDouble(value);
	if(o){
		dbOverWrite(c->db, c->argv[1], new);
	}else{
		dbAdd(c->db, c->argv[1], new);
	}

	signalModifiedKey(c->db, c->argv[1]);

	//send the event notification
	notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "incrbyfloat", c->argv[1], c->db->id);

	//set the server to dirty
	server.dirty++;

	addReplyBulk(c, new);

	/**
	 * Always replicate INCRBYFLOAT as a SET command with the final value in order to
	 * make sure that differences in float precision or formatting
	 * will not create differences in replicas or after an AOF restart.
	 */
	aux = createStringObject("SET", 3);
	rewriteClientCommandArgument(c, 0, aux);
	decrRefCount(aux);
	rewriteClientCommandArgument(c, 2, new);
}

void appendCommand(redisClient *c){
	size_t totlen;
	robj *o, *append;

	//Get the corresponding value object
	o = lookupKeyWrite(c->db, c->argv[1]);
	if(o == NULL){
		//the key-value is not existed
		c->argv[2] = tryObjectEncoding(c->argv[2]);
		dbAdd(c->db, c->argv[1], c->argv[2]);
		incrRefCount(c->argv[2]);
		totlen = stringObjectLen(c->argv[2]);
	}else{
		// the key is exist
		if(checkType(c, o, REDIS_STRING)) return;
		
		//Check after the append will this key exceed the limit of string
		append = c->argv[2];
		totlen = stringObjectLen(o) + sdslen(append->ptr);
		if(checkStringLength(c, totlen) != REDIS_OK) return;

		/*Append the value*/
		o = dbUnshareStringValue(c->db, c->argv[1], o);
		o->ptr = sdscatLen(o->ptr, append->ptr, sdslen(append->ptr));
		totlen = sdslen(o->ptr);
	}

	//send the signal to the db
	signalModifiedKey(c->db, c->argv[1]);

	//send the event notification
	notifyKeyspaceEvent(REDIS_NOTIFY_STRING, "append", c->argv[1], c->db->id);

	//set the database to dirty
	server.dirty++;

	//send reply
	addReplyLongLong(c, totlen);
}

void strlenCommand(redisClient *c){
	robj *o;

	//get the value object
	if((o = lookupKeyReadOrReply(c, c->argv[1], shared.czero)) == NULL || checkType(c, o, REDIS_STRING)) return;

	//return the length of the stirng
	addReplyLongLong(c, stringObjectLen(o));
}










