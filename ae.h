#ifndef __AE_H__
#define __AE_H__

/* The status of the event exectue result*/
#define AE_OK 0
#define AE_ERR -1

/* The status of the file event status */
#define AE_NONE 0
#define AE_READABLE 1
#define AE_WRITEABLE 2

/* The flag of the event handler */
//File event
#define AE_FILE_EVENTS 1
//Time event
#define AE_TIME_EVENTS 2
//All the event(both file event and the time event)
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
//Non-blocking and not wait
#define AE_DONE_WAIT 4

/* The flag determine if the time event is continue to execute */
#define AE_NOMORE -1

/* Macros for avoid compiler warning */
#define AE_NOTUSED(V) ((void)V)

/* The status of event handler */
struct aeEventLoop;

/* --------------------Types and data structures---------------------- */
//The event interface
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct  aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure */
typedef struct aeFileEvent{
    /* 
     * The mask type for the listening event type
     * The value may be AE_READABLE ， AE_WRITEABLE
     * or AE_READABLE|AE_WRITEABLE
     */
    int mask;

    //The readEvent handle function
    aeFileProc *rfileProc;
    
    //The writeEvent handle fucntion
    aeFileProc *wfileProc;

    //The mutiplexing-IO private data
    void *clientData;

}aeFileEvent;

/* Time event structure */
typedef struct aeTimeEvent{

    //The id of the time event
    long long id;   

    //When this time event arrive
    long when_sec; // In the seconds format
    long when_ms;  //In milliseconds format

    //The timeEvent handler
    aeTimeProc *timeProc;

    //The event release function
    aeEventFinalizerProc *finalizerProc;

    //The mutiplexing-IO private data
    void *clientData;

    //The pointer to the next time event struct
    struct aeTimeEvent *next;
}aeTimeEvent;


/* A fired event */
typedef struct aeFireEvent{
    
    //The ready filedescriptor
    int fd;

    /* The event type
     * The value can be AE_READABLE or
     * AE_WRITEABLE or both
     */
    int mask;
}aeFireEvent;

/* The state of an event based program */
typedef struct aeEventLoop{
    //The highest file descriptor currently registered
    int maxfd;
    
    //The max number of file descriptor tracked
    int setsize;

    //The variable used to generate time-event id
    long long timeEventNextId;

    // The last time event execute time
    /* Used to detect system clock skew 
     * 这个地方解释一下，细心的话会发现time_t所属的头文件<time.h>在该头文件中
     * 并没有被引入，那会不会在编译项目的时候造成问题？ 事实上是不会的，原因在于
     * 我们去看ae.c的文件发现我们先引入了<time.h> 随后在引入ae.h, 再回忆一下
     * 文件预处理的过程，编译器一个接一个的解析我们引入的头文件，并且将他们的内容
     * 放置在引入他的地方，那么根据上面说的ae.c的头文件引入顺序，在来到ae.h头文件
     * 时，我们已经引入了time.h头文件，其中包含了time_t的定义，所以不会有问题。
     * 
     * 这么做的目的，在于我们只在一个地方引入了头文件，缩短了最后文件的体积，我们当然
     * 可以在这里再次引入一遍<time.h> 但造成的问题是 <time.h>这个头文件在ae.h和
     * ae.c两个头文件中 被重复引用。
     * */
    time_t lastTime;

    //Already registered file event
    aeFileEvent *events;

    //Ready file event
    aeFireEvent *fired;

    //Time events
    aeTimeEvent *timeEventHead;

    //the switch for the evnet handler
    int stop;

    //Mutiplexing-IO private data
    void *apiData;  /* This is used for polling API specific data */

    //The function need to execute before handle the event
    aeBeforeSleepProc *beforesleep;

}aeEventLoop;

/* Function prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask, 
    aeFileEvent *proc, void *clientData);
void aeDeleteFileEvnet(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);

#endif