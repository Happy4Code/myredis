
#ifndef __ADLIST_H__
#define __ADLIST_H__

typedef struct listNode{

	struct listNode *prev;

	struct listNOde *next;

	void *val;
}listNode;

/**
 *Double linklist iterator
 */
typedef struct listIter{

	listNode *next;

	int direction;
}listIter;

/**
 *Double linklist
 */
typedef struct list{
	
	listNode *head;

	listNode *tail;
	
	unsigned long len;

	void *(*dup)(void *ptr);

	void *(*free)(void *ptr);

	int (*match)(void *ptr, void *key);

}list;

#define listLength(l) ((l)->len)
#define listFirst(l) ((l)->head)
#define listLast(l) ((l)->last)
#define listPrevNode(l) ((l)->prev)
#define listNextNode(l) ((l)->next)
#define listNodeValue(l) ((l)->val)
#define listSetDupMethod(l, m) ((l)->dup = (m))
#define listSetFreeMethod(l, m) ((l)->free = (m))
#define listSetMatchMethod(l, m) ((l)->match = (m))
#define listGetDupMethod(l) ((l)->dup)
#define listGetFreeMethod(l) ((l)->free)
#define listGetMatchMethod(l) ((l)->match)

/*
 *Function prototype
 */

list *listCreate(void);
void listRelease(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
void listDelNode(list *list, listNode *node);
listIter *listGetIterator(list *list, int direction);
listNode *listNext(listIter *iter);
void listReleaseIterator(listIter *iter);
list *listDup(list *orig);
listNode *listSearchKey(list *list, void *key);
listNode *listIndex(list *list, long index);
void listRewind(list *list, listIter *li);
void listRewindTail(list *list, listIter *li);
void listRotate(list *list);

#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif 
