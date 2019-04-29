#include <sys/types.h>
#include <stdarg.h>
#include <stdio.h>
typedef char *sds;
#define SDS_MAX_PREALLOC (1024*1024)
/*
 *In redis we use a custome data structure
 *to implement a string.The idea is pretty
 *like java implement the String class.
 *We use variable len and variable free to 
 *keep the length and avaliable space of the
 *buf, prevent us from iterate the buf to find 
 *the result.Also this data structure can auto-resize
 *, which c char array can not handle.
 *
 */
struct sdshdr{
	//it is the length of the sdshdr represent string
	int len;
	//the avaliable space for the sdshdr
	int free;
	//data space
	char buf[];
};

/**
 * The s is a char pointer which is the value of buf variable 
 * in struct sdshdr.When we create the sdshdr, we allocate a
 * block of memory space which is enough for the variable len
 * variable free and a buf array, so if we have the address of
 * buf minus size of len and size of free, we will get the beginning
 * address of the sdshr, but why we minus struct sdshdr.That is becasuse
 * the buf[] hasn't allocated any memory so when we use size of to the 
 * data structure, it doesn't take up any space.
 */
static inline size_t sdslen(const sds s){
	struct sdshdr *sh = (void*)(s - sizeof(struct sdshdr));
	return sh->len;
}


static inline size_t sdsavail(const sds s){
	struct sdshdr *sh = (void*)(s - sizeof(struct sdshdr));
	return sh->free;
}

sds sdsnewlen(const void *init, size_t initlen);
sds sdsnew(const char *init);
sds sdsempty(void);
sds sdsdup(const sds s);
void sdsfree(sds s);
size_t sdsgrowzero(sds s, size_t len);
sds sdscatlen(sds s, const void *t, size_t len);

sds sdsMakeRoomFor(sds s, size_t addlen);
