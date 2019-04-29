#include "sds.h"
#include "zmalloc.h"

sds sdsnewlen(const void *init, size_t initlen){
	struct sdshdr *sh;
	/**
	 * Check if there is any meanningful init string.
	 */
	if(init){
		sh = zmalloc(sizeof(struct sdshdr) + initlen + 1);
	}else{
		sh = zcalloc(sizeof(struct sdshdr) + initlen + 1);
	}

	if(sh == NULL)
		return NULL;
	/**
	 * if this memory allocation process finished, we 
	 * init the inner variable to proper value.And if
	 * the init contains any meaningful value we copy
	 * them to the sh->buf(Note we don't use strcpy, 
	 * because the sdshdr structure can not only hold
	 * text string but also can hold some other format
	 * data, so we treat them as byte array, not matter
	 * which specific format they are).
	 */
	sh->len = initlen;
	sh->free = 0;

	if(init){
		memcpy(sh->buf, init, initlen);
	}
	sh->buf[initlen] = '\0';
	return sh->buf;
}

/**
 *Use a given string to init the sds structure
 */
sds sdsnew(const char *init){
	size_t len = init == NULL ? 0 : strlen(init);
	return sdsnewlen(init, len);
}

sds sdsempty(void){
	return sdsnewlen("", 0);
}

/**
 *Duplicate the sds s
 */
sds sdsdup(const sds s){
	return sdsnewlen(s, sdslen(s));
}

/**
 *Free the data structure hold by s
 */
void sdsfree(sds s){
	if(s != NULL) 
		zfree(s - sizeof(struct sdshdr));
}

/**
 *Grows the sds to have the specific length.Bytes that
 *were not part of the original will be set to zero.
 *
 * If the specific length is smaller than the original
 * length, no operation is performed. 
 *
 */
sds sdsgrowzero(sds s, size_t len){
	
	struct sdshdr *sh;
	size_t curlen = sdslen(s);
	if(len <= curlen) return s;
	
	/**
	 *Recall that the sdsMakeRoomFor function only 
	 *change the variable free in the sds data 
	 *structure, so the len remains the same.
	 */
	s = sdsMakeRoomFor(s, len - curlen);
	
	if(s == NULL) return NULL;
	sh = (void *)(s - sizeof(struct sdshdr));
	memset(s + curlen, 0, len -curlen + 1);
	sh->free = sh->free + sh->len - len;
	sh->len = len;
	return s;
}

/**
 *Concat a Object t with length len to the end of sds.
 *I use the word Object here because we can not treat
 *the t as a string, recall the redis SDS data structure
 *is designed to store the any format data, not just string. 
 */
sds sdscatlen(sds s, const void *t, size_t len){
	
	struct sdshdr *sh;
	int curlen = sdslen(s);
	//when the concat operation take place we need to enlarge the space in
	//current s, to avoid additional memory allocation in the future.
	s = sdsMakeRoomFor(s, len);

	//what if the allocation failed
	if(s == NULL) return NULL;

	sh = (void *)(s - (sizeof(struct sdshdr)));
	memcpy(s+curlen, t, len);
	sh->free = sh->free - len;
	sh->len = curlen + len;
	s[curlen+len] = '\0';
	return s;
}

/**
 *This function is used to resize the sds data structure,
 *when the free space is not enough to fit the next object.
 *Notice that the len is used to represent the current used
 *length, the free is used to indicate how many space is 
 *avaiable.
 */
sds sdsMakeRoomFor(sds s, size_t addlen){
	
	struct sdshdr *sh, *newsh;
	size_t free = sdsavail(s);
	size_t len, newlen;

	if(free >= addlen) return s;

	len = sdslen(s);
	newlen = len + addlen;
	sh = (void *)(s - sizeof(struct sdshdr));
	if(newlen < SDS_MAX_PREALLOC)
		newlen *= 2;
	else
		newlen += SDS_MAX_PREALLOC;
	newsh = zrealloc(sh, sizeof(struct sdshdr) + newlen + 1);
	
	if(newsh == NULL) return NULL;
	
	newsh->free = newlen - len;	
	return newsh->buf;
}












