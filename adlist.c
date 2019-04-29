#include "adlist.h"
#include "zmalloc.h"

/**
 * Create an empty double linklist
 */
list *listCreate(void){
	
	list *listp = zmalloc(sizeof(struct list));
	
	if(listp == NULL) return NULL;
	
	listp->head = listp->tail = NULL;
	listp->len = 0;
	listp->dup = NULL;
	listp->free = NULL;
	listp->match = NULL;
	
	return listp;	
}

/**
 *Free the whole list
 *This function can't failed.
 */
void listRelease(list *list){
	
	if(list == NULL) return ;

	unsigned long len = list->len;
	listNode *cur, *next;
	while(len--){
		next = cur->next;
		if(list->free) list->free(cur->value);
		zfree(cur);
		cur = next;
	}

	//when each node is freed then we free the list pointer.
	zfree(list);
}

/**
 *Add a new node to the list, to head, containing the specific pointer
 *'value'.
 *
 *On error, NULL is returned and no operation is performed, the list remains
 *unchanged.
 *On success the 'list' pointer you pass to the function is returned.
 *
 */
list *listAddNodeHead(list *list, void *value){
	
	*listNode node = zmalloc(sizeof(listNode));
	if(node == NULL) return list;
	node->value = value;
	//for an empty list
	node->prev = NULL;
	if(list->len == 0){
		list->head = list->tail = node;
		node->next = NULL;
	}else{
		node->next = list->head;
		list->head->prev = node;
		list->head = node;
	}
	node->len++;
	return list;	
}

/**
 *Add a new node to the list, to tail, containing the specific pointer 'value'
 *
 * On error, NULL is returned and no operation is performed, the list remains
 * unchanged.
 * On success the list pointer you pass to the function is returned, and the new node
 * is added to the tail.
 */
list *listAddNodeTail(list *list, void *value){
	
	*listNode node = zmalloc(sizeof(listNode));
	if(node == NULL) return list;
	
	node->value = value;
	node->next = NULL;
	
	if(list->len){
		node->prev = list->tail;
		list->tail->next = node;
		list->tail = node;
	}else{
		list->head = list->tail = node;
		node->prev = NULL;
	}
	list->len++;
	return list;
}

/**
 *Create a node which value is value pointer points to.Then insert it before
 *or after the old_node, before or after depends on the after value.
 *
 *If after is 0, insert before old_node
 *If after is 1, insert after new_node
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after){
	
	*listNode node = zmalloc(listNode);
	if(node == NULL) return list;
	node->value = value;
	if(after){
		node->next = old_node->next;
		if(old_node->next)
			old_node->next->prev = node;
		
		node->prev = old_node;
		old_node->next = node;
		if(list->tail == old_node)
			list->tail = node;
	}else{
		node->prev = old_node->prev;
		if(old_node->prev)
			old_node->prev->next = node;
		node->next = old_node;
		old_node->prev = node;
		if(old_node == list->head)
			list->head = node;
	}
	list->len++;
	return list;
}

/**
 *Remove the specific node from the specific list.
 *It's up to the caller to free the private value of the node
 *
 * This function can't fail.
 */
void listDelNode(list *list, listNode *node){
	
	if(node->prev)
		node->prev->next = node->next;
	else
		list->head = node->next;

	if(node->next)
		node->next->prev = node->prev;
	else
		list->tail = node->prev;

	list->free(node);
	zfree(node);
	list->len--;
}

/**
 *Returns a list iterator pointer.After initialization every
 *call to listNext() will return the next element of the list
 *
 *The function Can't fail.
 *The direction is used to indicate the direction of the itertion
 */
listIter *listGetIterator(list *list, int direction){
	
	listIter *iter = zmalloc(sizeof(listIter));
	
	if(iter == NULL) return NULL;

	if(direction == AL_START_TAIL)
		iter->next = list->tail;
	else
		iter->next = list->head;
	
	iter->direction = direction;
	return iter;
}

listNode *listNext(listIter *iter){
	
	listNode *node = iter->next;
	if(node){
		if(iter->direction){
			iter->next = node->prev;	
		}else{
			iter->next = node->next;
		}
	}
	return node;
}

/**
 *Release the iterator
 */
void listReleaseIterator(listIter *iter){
	if(iter) zfree(iter);
}

/**
 *Duplicate the whole list.On out of memory the NULL is return.
 *On success a copy of the original list is returned.
 *
 *The 'dup' method set with listSetDupMethod() function is used
 *to copy the node value.Otherwise the same pointer value of 
 *the original node is used as value of the copied node.
 *
 *The original list both on success or error is never modified.
 */
list *listDup(list *orig){

	list *copy;
	ListIter *it;
	ListNode *node;

	if((copy = listCreate()) == NULL) retunrn NULL;
	
	copy->dup = orig->dup;
	copy->free = orig->free;
	copy->match = orig->match;

	if((it = listGetIterator(list, AL_START_HEAD)) == NULL) {
		listRelease(copy);	
		return NULL;
	}
	while((node = listNext(it)) != NULL){
		void *value;
		if(copy->dup != NULL){
			value = list->dup(node->value);
			if(value == NULL){
				listReleaseIterator(it);
				listRelease(copy);
				return NULL;
			}
		}else{
			value = node->value;
		}
		if(listAddNodeTail(copy, value) == NULL){
			listReleaseIterator(it);
			listRelease(copy);	
		}
	}
	listReleaseIterator(it);
	return copy;
}

/**
 *Search the list for a node matching a given key.
 *The match is performed using match method, set with 
 *listSetMatchMethod(). If no 'match' method is set, the
 *value pointer of every node is directly compared with
 *the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (searching start from head).If no matching exists ,NULL
 * is returned.
 *
 */
listNode *listSearchKey(list *list, void *key){
	
	listIter *it;
	if((it = listGetIterator(list, AL_START_HEAD)) == NULL) return NULL;
	
	listNode *cur;
	while((cur = listNext(it)) != NULL){
		if(list->match){
			if(list->match(cur)){
				listReleaseIterator(it);
				return cur;
			}
		}else{
			if(key == cur->value){
				listReleaseIterator(it);
				return cur;
			}
		}
	}
	listReleaseIterator(it);
	return NULL;
}

/***
 *
 *	while((cur = listNext(it)) != NULL){
		 if(list->match){
			if(list->match(cur)){
				break;
			}
		}else{
			if(key == cur->value){
				break;
			}
		}
	}
	listReleaseIterator(it);
	return cur;
 *
 *
 */


/**
 *Return the element at the specific zero-based index
 *where 0 is the head, 1 is the elment next to the head 
 *and so on.Negative integers are used to count from the
 *tail, -1 is the last element, -2 penultimate from the
 *tail and so on.If the index is out of range then NULL
 *is returned.
 */
listNode *listIndex(list *list, long index){
	listNode *node;
	if(index < 0){
		index = (-index) - 1;
		node = list->tail;
		while(index-- && node) node = node->prev;
	}else{
		node = list->head;
		while(index-- && node) node = node->next;
	}
	return node;
}

/**
 * Rewind the list iterator to start from the head
 * of the list, and set it's iterate direction from
 * the AL_START
 */
void listRewind(list *list, listIter *li){
	
	li->direction = AL_START_HEAD;
	li->next = list->head;
}

/**
 *
 */
void listRewindTail(list *list, listIter *li){
	
	li->direction = AL_START_TAIL;
	li->next = list->tail;
}

/**
 *
 */
void list Rotate(list *list){
	if(listLength(list) <= 1) return;

	listNode *node = list->tail;
	
	node->prev->next = NULL;
	list->tail = node->prev;
	node->prev = NULL;
	
	list->head->prev = node;
	node->next = list->head;
	list->head = node;
}

