#ifndef __REDIS_H
#define __REDIS_H

#include "fmacros.h"
#include "config.h"

#if defined(__sun)
#include "solarisfixes.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <signal.h>

#include "ae.h"
#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "zmalloc.h"
#include "anet.h"
#include "ziplist.h"
#include "intset.h"
#include "version.h"
#include "util.h"

/*Error code*/
#define REDIS_OK  0
#define REDIS_ERR -1

/* Static server configuration */
/* 默认的服务器配置值 */
#define REDIS_DEFAULT_HZ        10      /* Time interrupt calls/sec. */
#define REDIS_MIN_HZ            1
#define REDIS_MAX_HZ            500 
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_TCP_BACKLOG       511     /* TCP listen backlog */
#define REDIS_MAXIDLETIME       0       /* default client timeout: infinite */
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_CONFIGLINE_MAX    1024
#define REDIS_DBCRON_DBS_PER_CALL 16
#define REDIS_MAX_WRITE_PER_EVENT (1024*64)
#define REDIS_SHARED_SELECT_CMDS 10
#define REDIS_SHARED_INTEGERS 10000
#define REDIS_SHARED_BULKHDR_LEN 32
#define REDIS_MAX_LOGMSG_LEN    1024 /* Default maximum length of syslog messages */
#define REDIS_AOF_REWRITE_PERC  100
#define REDIS_AOF_REWRITE_MIN_SIZE (64*1024*1024)
#define REDIS_AOF_REWRITE_ITEMS_PER_CMD 64
#define REDIS_SLOWLOG_LOG_SLOWER_THAN 10000
#define REDIS_SLOWLOG_MAX_LEN 128
#define REDIS_MAX_CLIENTS 10000
#define REDIS_AUTHPASS_MAX_LEN 512
#define REDIS_DEFAULT_SLAVE_PRIORITY 100
#define REDIS_REPL_TIMEOUT 60
#define REDIS_REPL_PING_SLAVE_PERIOD 10
#define REDIS_RUN_ID_SIZE 40
#define REDIS_OPS_SEC_SAMPLES 16
#define REDIS_DEFAULT_REPL_BACKLOG_SIZE (1024*1024)    /* 1mb */
#define REDIS_DEFAULT_REPL_BACKLOG_TIME_LIMIT (60*60)  /* 1 hour */
#define REDIS_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */
#define REDIS_BGSAVE_RETRY_DELAY 5 /* Wait a few secs before trying again. */
#define REDIS_DEFAULT_PID_FILE "/var/run/redis.pid"
#define REDIS_DEFAULT_SYSLOG_IDENT "redis"
#define REDIS_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define REDIS_DEFAULT_DAEMONIZE 0
#define REDIS_DEFAULT_UNIX_SOCKET_PERM 0
#define REDIS_DEFAULT_TCP_KEEPALIVE 0
#define REDIS_DEFAULT_LOGFILE ""
#define REDIS_DEFAULT_SYSLOG_ENABLED 0
#define REDIS_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR 1
#define REDIS_DEFAULT_RDB_COMPRESSION 1
#define REDIS_DEFAULT_RDB_CHECKSUM 1
#define REDIS_DEFAULT_RDB_FILENAME "dump.rdb"
#define REDIS_DEFAULT_SLAVE_SERVE_STALE_DATA 1
#define REDIS_DEFAULT_SLAVE_READ_ONLY 1
#define REDIS_DEFAULT_REPL_DISABLE_TCP_NODELAY 0
#define REDIS_DEFAULT_MAXMEMORY 0
#define REDIS_DEFAULT_MAXMEMORY_SAMPLES 5
#define REDIS_DEFAULT_AOF_FILENAME "appendonly.aof"
#define REDIS_DEFAULT_AOF_NO_FSYNC_ON_REWRITE 0
#define REDIS_DEFAULT_ACTIVE_REHASHING 1
#define REDIS_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC 1
#define REDIS_DEFAULT_MIN_SLAVES_TO_WRITE 0
#define REDIS_DEFAULT_MIN_SLAVES_MAX_LAG 10
#define REDIS_IP_STR_LEN INET6_ADDRSTRLEN
#define REDIS_PEER_ID_LEN (REDIS_IP_STR_LEN+32) /* Must be enough for ip:port */
#define REDIS_BINDADDR_MAX 16
#define REDIS_MIN_RESERVED_FDS 32

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 20 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Protocol and I/O related defines */
#define REDIS_MAX_QUERYBUF_LEN  (1024*1024*1024) /* 1GB max query buffer. */
#define REDIS_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define REDIS_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define REDIS_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
#define REDIS_MBULK_BIG_ARG     (1024*32)
#define REDIS_LONGSTR_SIZE      21          /* Bytes needed for long -> str */
// 指示 AOF 程序每累积这个量的写入数据
// 就执行一次显式的 fsync
#define REDIS_AOF_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */
/* When configuring the Redis eventloop, we setup it so that the total number
 * of file descriptors we can handle are server.maxclients + RESERVED_FDS + FDSET_INCR
 * that is our safety margin. */
#define REDIS_EVENTLOOP_FDSET_INCR (REDIS_MIN_RESERVED_FDS+96)

/* Hash table parameters */
#define REDIS_HT_MINFILL        10      /* Minimal hash table fill 10% */

/* Log levels */
#define REDIS_DEBUG 0
#define REDIS_VERBOSE 1
#define REDIS_NOTICE 2
#define REDIS_WARNING 3
#define REDIS_LOG_RAW (1<<10) /* Modifier to log without timestamp */
#define REDIS_DEFAULT_VERBOSITY REDIS_NOTICE

/*Anti-warning macro*/
#define REDIS_NOTUSED(V) ((void) V)

#define ZSKIPLIST_MAXLEVEL	32
#define ZSKIPLIST_P	0.25

/*Object type
*/
#define REDIS_STRING 0
#define REDIS_LIST	1
#define REDIS_SET	2
#define REDIS_ZSET	3
#define REDIS_HASH	4

/**
 * Object encoding.The object in redis even for the same type, they 
 * can have different underlying impletation. 
 */
#define REDIS_ENCODING_RAW 0
#define REDIS_ENCODING_INT 1
#define REDIS_ENCODING_HT  2
#define REDIS_ENCODING_ZIPMAP 3
#define REDIS_ENCODING_LINKEDLIST 4
#define REDIS_ENCODING_ZIPLIST 5
#define REDIS_ENCODING_INTSET 6
#define REDIS_ENCODING_SKIPLIST 7
#define REDIS_ENCODING_EMBSTR 8

/*List related stuff*/
#define REDIS_HEAD 0
#define REDIS_TAIL 1

/*Set operations codes*/
#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2


/* We can print the stacktrace, so our assert is defined this way: */
#define redisAssertWithInfo(_c,_o,_e) ((_e)?(void)0 : (_redisAssertWithInfo(_c,_o,#_e,__FILE__,__LINE__),_exit(1)))
#define redisAssert(_e) ((_e)?(void)0 : (_redisAssert(#_e,__FILE__,__LINE__),_exit(1)))
#define redisPanic(_e) _redisPanic(#_e,__FILE__,__LINE__),_exit(1)
/*--------------------------------------------------------------
 *			Data Structure
 * -----------------------------------------------------------*/

typedef long long mstime_t; /*millisecond time type*/

#define REDIS_LRU_BITS 24
#define REDIS_LRU_CLOCK_MAX ((1<<REDIS_LRU_BITS) - 1)
#define REDIS_LRU_CLOCK_RESOLUTION 1000 //LRU clock resolution in ms

typedef struct redisObject{
	
	//type
	unsigned type:4;
	//encoding
	unsigned encoding:4;
	//last access time
	unsigned lru:REDIS_LRU_BITS;
	//reference counter
	int refcount;
	//The actual pointer
	void *ptr;
}robj;

/* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
typedef struct redisDb {
	//database key space holds all the key-value pairs
	dict *dict;
	//time out keys with a timeout set
	dict *expires;
	//blocked keys which waiting for data(BLPOP)
	dict *blocking_keys;
	//blocked keys that received a PUSH
	dict *ready_keys;
	//keys from MULTI/EXEC 
	dict *watched_keys;

	int id;

	long long avg_ttl;


} redisDb;

/*
 * 事务命令
 */
typedef struct multiCmd {

    // 参数
    robj **argv;

    // 参数数量
    int argc;

    // 命令指针
    struct redisCommand *cmd;

} multiCmd;

/*
 * 事务状态
 */
typedef struct multiState {

    // 事务队列，FIFO 顺序
    multiCmd *commands;     /* Array of MULTI commands */

    // 已入队命令计数
    int count;              /* Total number of MULTI commands */
    int minreplicas;        /* MINREPLICAS for synchronous replication */
    time_t minreplicas_timeout; /* MINREPLICAS timeout as unixtime. */
} multiState;

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
// 阻塞状态
typedef struct blockingState {

    /* Generic fields. */
    // 阻塞时限
    mstime_t timeout;       /* Blocking operation timeout. If UNIX current time
                             * is > timeout then the operation timed out. */

    /* REDIS_BLOCK_LIST */
    // 造成阻塞的键
    dict *keys;             /* The keys we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    // 在被阻塞的键有新元素进入时，需要将这些新元素添加到哪里的目标键
    // 用于 BRPOPLPUSH 命令
    robj *target;           /* The key that should receive the element,
                             * for BRPOPLPUSH. */

    /* REDIS_BLOCK_WAIT */
    // 等待 ACK 的复制节点数量
    int numreplicas;        /* Number of replicas we are waiting for ACK. */
    // 复制偏移量
    long long reploffset;   /* Replication offset to reach. */

} blockingState;

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
// 记录解除了客户端的阻塞状态的键，以及键所在的数据库。
typedef struct readyList{
	redisDb *db;
	robj *key;
}readyList;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a liked list.
 *
 * 因为 I/O 复用的缘故，需要为每个客户端维持一个状态。
 *
 * 多个客户端状态被服务器用链表连接起来。
 */
typedef struct redisClient {
	
	//socket file-descriptor
	int fd;

	//use database
	redisDb *db;

	//Current usedatabase id
	int dictid;

	//The name of the client
	robj *name;

	sds querybuf;

	size_t querybuf_peak;  /*Recent peak of querybuf size */

	//number of arguments
	int argc;

	//Parameter array
	robj **argv;

	struct redisCommand *cmd, *lastcmd;

	//Request type: inline command or muti-command
	int reqtype;
	
    // 剩余未读取的命令内容数量
    int multibulklen;       /* number of multi bulk arguments left to read */

    // 命令内容的长度
    long bulklen;           /* length of bulk argument in multi bulk request */

    // 回复链表
    list *reply;

	unsigned long reply_bytes; //Tot bytes of objects in reply list

    // 已发送字节，处理 short write 用
    int sentlen;            /* Amount of bytes already sent in the current
                               buffer or object being sent. */

    // 创建客户端的时间
    time_t ctime;           /* Client creation time */

    // 客户端最后一次和服务器互动的时间
    time_t lastinteraction; /* time of the last interaction, used for timeout */

    // 客户端的输出缓冲区超过软性限制的时间
    time_t obuf_soft_limit_reached_time;

    // 客户端状态标志
    int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */

    // 当 server.requirepass 不为 NULL 时
    // 代表认证的状态
    // 0 代表未认证， 1 代表已认证
    int authenticated;      /* when requirepass is non-NULL */

    // 复制状态
    int replstate;          /* replication state if this is a slave */
    // 用于保存主服务器传来的 RDB 文件的文件描述符
    int repldbfd;           /* replication DB file descriptor */

    // 读取主服务器传来的 RDB 文件的偏移量
    off_t repldboff;        /* replication DB file offset */
    // 主服务器传来的 RDB 文件的大小
    off_t repldbsize;       /* replication DB file size */
    
    sds replpreamble;       /* replication DB preamble. */

    // 主服务器的复制偏移量
    long long reploff;      /* replication offset if this is our master */
    // 从服务器最后一次发送 REPLCONF ACK 时的偏移量
    long long repl_ack_off; /* replication ack offset, if this is a slave */
    // 从服务器最后一次发送 REPLCONF ACK 的时间
    long long repl_ack_time;/* replication ack time, if this is a slave */
    // 主服务器的 master run ID
    // 保存在客户端，用于执行部分重同步
    char replrunid[REDIS_RUN_ID_SIZE+1]; /* master run id if this is a master */
    // 从服务器的监听端口号
    int slave_listening_port; /* As configured with: SLAVECONF listening-port */

    // 事务状态
    multiState mstate;      /* MULTI/EXEC state */

    // 阻塞类型
    int btype;              /* Type of blocking op if REDIS_BLOCKED. */
    // 阻塞状态
    blockingState bpop;     /* blocking state */

    // 最后被写入的全局复制偏移量
    long long woff;         /* Last write global replication offset. */

    // 被监视的键
    list *watched_keys;     /* Keys WATCHED for MULTI/EXEC CAS */

    // 这个字典记录了客户端所有订阅的频道
    // 键为频道名字，值为 NULL
    // 也即是，一个频道的集合
    dict *pubsub_channels;  /* channels a client is interested in (SUBSCRIBE) */

    // 链表，包含多个 pubsubPattern 结构
    // 记录了所有订阅频道的客户端的信息
    // 新 pubsubPattern 结构总是被添加到表尾
    list *pubsub_patterns;  /* patterns a client is interested in (SUBSCRIBE) */
    sds peerid;             /* Cached peer ID. */

    /* Response buffer */
    // 回复偏移量
    int bufpos;
    // 回复缓冲区
    char buf[REDIS_REPLY_CHUNK_BYTES];

} redisClient;

// 服务器的保存条件（BGSAVE 自动执行的条件）
struct saveparam {

    // 多少秒之内
    time_t seconds;

    // 发生多少次修改
    int changes;
};

//To reduce the memory fragment and reduce the time-consumed shared object.
struct sharedObjectStruct{
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
    *lpush, *emptyscan, *minstring, *maxstring,
    *select[REDIS_SHARED_SELECT_CMDS],
    *integers[REDIS_SHARED_INTEGERS],
    *mbulkhdr[REDIS_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[REDIS_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
};

typedef struct zskiplistNode{

	robj *obj;

	double score;

	struct zskiplistNode *backward;

	struct zskiplistLevel{
		struct zskiplistNode *forward;

		unsigned int span;
	}level[];

}zskiplistNode;

typedef struct zskiplist{

	struct zskiplistNode *header, *tail;

	unsigned long length;

	int level;

}zskiplist;

typedef struct zset{
	//dictionary the key is the element, value is the score
	dict *dict;
	// skiplist, sorted element by the score
	// Use the avarge O(logN) time complexity to
	// find a specific element or find a bunch of
	// elements.
	zskiplist *zsl;

}zset;

/*-----------------------------------------------------------------------------
 * Global server state
 *----------------------------------------------------------------------------*/

struct clusterState;

struct redisServer {

    /* General */

    // 配置文件的绝对路径
    char *configfile;           /* Absolute config file path, or NULL */

    // serverCron() 每秒调用的次数
    int hz;                     /* serverCron() calls frequency in hertz */

    // 数据库
    redisDb *db;

    // 命令表（受到 rename 配置选项的作用）
    dict *commands;             /* Command table */
    // 命令表（无 rename 配置选项的作用）
    dict *orig_commands;        /* Command table before command renaming. */

    // 事件状态
    aeEventLoop *el;

    // 最近一次使用时钟
    unsigned lruclock:REDIS_LRU_BITS; /* Clock for LRU eviction */

    // 关闭服务器的标识
    int shutdown_asap;          /* SHUTDOWN needed ASAP */

    // 在执行 serverCron() 时进行渐进式 rehash
    int activerehashing;        /* Incremental rehash in serverCron() */

    // 是否设置了密码
    char *requirepass;          /* Pass for AUTH command, or NULL */

    // PID 文件
    char *pidfile;              /* PID file path */

    // 架构类型
    int arch_bits;              /* 32 or 64 depending on sizeof(long) */

    // serverCron() 函数的运行次数计数器
    int cronloops;              /* Number of times the cron function run */

    // 本服务器的 RUN ID
    char runid[REDIS_RUN_ID_SIZE+1];  /* ID always different at every exec. */

    // 服务器是否运行在 SENTINEL 模式
    int sentinel_mode;          /* True if this instance is a Sentinel. */


    /* Networking */

    // TCP 监听端口
    int port;                   /* TCP listening port */

    int tcp_backlog;            /* TCP listen() backlog */

    // 地址
    char *bindaddr[REDIS_BINDADDR_MAX]; /* Addresses we should bind to */
    // 地址数量
    int bindaddr_count;         /* Number of addresses in server.bindaddr[] */

    // UNIX 套接字
    char *unixsocket;           /* UNIX socket path */
    mode_t unixsocketperm;      /* UNIX socket permission */

    // 描述符
    int ipfd[REDIS_BINDADDR_MAX]; /* TCP socket file descriptors */
    // 描述符数量
    int ipfd_count;             /* Used slots in ipfd[] */

    // UNIX 套接字文件描述符
    int sofd;                   /* Unix socket file descriptor */

    int cfd[REDIS_BINDADDR_MAX];/* Cluster bus listening socket */
    int cfd_count;              /* Used slots in cfd[] */

    // 一个链表，保存了所有客户端状态结构
    list *clients;              /* List of active clients */
    // 链表，保存了所有待关闭的客户端
    list *clients_to_close;     /* Clients to close asynchronously */

    // 链表，保存了所有从服务器，以及所有监视器
    list *slaves, *monitors;    /* List of slaves and MONITORs */

    // 服务器的当前客户端，仅用于崩溃报告
    redisClient *current_client; /* Current client, only used on crash report */

    int clients_paused;         /* True if clients are currently paused */
    mstime_t clients_pause_end_time; /* Time when we undo clients_paused */

    // 网络错误
    char neterr[ANET_ERR_LEN];   /* Error buffer for anet.c */

    // MIGRATE 缓存
    dict *migrate_cached_sockets;/* MIGRATE cached sockets */


    /* RDB / AOF loading information */

    // 这个值为真时，表示服务器正在进行载入
    int loading;                /* We are loading data from disk if true */

    // 正在载入的数据的大小
    off_t loading_total_bytes;

    // 已载入数据的大小
    off_t loading_loaded_bytes;

    // 开始进行载入的时间
    time_t loading_start_time;
    off_t loading_process_events_interval_bytes;

    /* Fast pointers to often looked up command */
    // 常用命令的快捷连接
    struct redisCommand *delCommand, *multiCommand, *lpushCommand, *lpopCommand,
                        *rpopCommand;


    /* Fields used only for stats */

    // 服务器启动时间
    time_t stat_starttime;          /* Server start time */

    // 已处理命令的数量
    long long stat_numcommands;     /* Number of processed commands */

    // 服务器接到的连接请求数量
    long long stat_numconnections;  /* Number of connections received */

    // 已过期的键数量
    long long stat_expiredkeys;     /* Number of expired keys */

    // 因为回收内存而被释放的过期键的数量
    long long stat_evictedkeys;     /* Number of evicted keys (maxmemory) */

    // 成功查找键的次数
    long long stat_keyspace_hits;   /* Number of successful lookups of keys */

    // 查找键失败的次数
    long long stat_keyspace_misses; /* Number of failed lookups of keys */

    // 已使用内存峰值
    size_t stat_peak_memory;        /* Max used memory record */

    // 最后一次执行 fork() 时消耗的时间
    long long stat_fork_time;       /* Time needed to perform latest fork() */

    // 服务器因为客户端数量过多而拒绝客户端连接的次数
    long long stat_rejected_conn;   /* Clients rejected because of maxclients */

    // 执行 full sync 的次数
    long long stat_sync_full;       /* Number of full resyncs with slaves. */

    // PSYNC 成功执行的次数
    long long stat_sync_partial_ok; /* Number of accepted PSYNC requests. */

    // PSYNC 执行失败的次数
    long long stat_sync_partial_err;/* Number of unaccepted PSYNC requests. */


    /* slowlog */

    // 保存了所有慢查询日志的链表
    list *slowlog;                  /* SLOWLOG list of commands */

    // 下一条慢查询日志的 ID
    long long slowlog_entry_id;     /* SLOWLOG current entry ID */

    // 服务器配置 slowlog-log-slower-than 选项的值
    long long slowlog_log_slower_than; /* SLOWLOG time limit (to get logged) */

    // 服务器配置 slowlog-max-len 选项的值
    unsigned long slowlog_max_len;     /* SLOWLOG max number of items logged */
    size_t resident_set_size;       /* RSS sampled in serverCron(). */
    /* The following two are used to track instantaneous "load" in terms
     * of operations per second. */
    // 最后一次进行抽样的时间
    long long ops_sec_last_sample_time; /* Timestamp of last sample (in ms) */
    // 最后一次抽样时，服务器已执行命令的数量
    long long ops_sec_last_sample_ops;  /* numcommands in last sample */
    // 抽样结果
    long long ops_sec_samples[REDIS_OPS_SEC_SAMPLES];
    // 数组索引，用于保存抽样结果，并在需要时回绕到 0
    int ops_sec_idx;


    /* Configuration */

    // 日志可见性
    int verbosity;                  /* Loglevel in redis.conf */

    // 客户端最大空转时间
    int maxidletime;                /* Client timeout in seconds */

    // 是否开启 SO_KEEPALIVE 选项
    int tcpkeepalive;               /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled;      /* Can be disabled for testing purposes. */
    size_t client_max_querybuf_len; /* Limit for client query buffer length */
    int dbnum;                      /* Total number of configured DBs */
    int daemonize;                  /* True if running as a daemon */
    // 客户端输出缓冲区大小限制
    // 数组的元素有 REDIS_CLIENT_LIMIT_NUM_CLASSES 个
    // 每个代表一类客户端：普通、从服务器、pubsub，诸如此类
    clientBufferLimitsConfig client_obuf_limits[REDIS_CLIENT_LIMIT_NUM_CLASSES];


    /* AOF persistence */

    // AOF 状态（开启/关闭/可写）
    int aof_state;                  /* REDIS_AOF_(ON|OFF|WAIT_REWRITE) */

    // 所使用的 fsync 策略（每个写入/每秒/从不）
    int aof_fsync;                  /* Kind of fsync() policy */
    char *aof_filename;             /* Name of the AOF file */
    int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */

    // 最后一次执行 BGREWRITEAOF 时， AOF 文件的大小
    off_t aof_rewrite_base_size;    /* AOF size on latest startup or rewrite. */

    // AOF 文件的当前字节大小
    off_t aof_current_size;         /* AOF current size. */
    int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */

    // 负责进行 AOF 重写的子进程 ID
    pid_t aof_child_pid;            /* PID if rewriting process */

    // AOF 重写缓存链表，链接着多个缓存块
    list *aof_rewrite_buf_blocks;   /* Hold changes during an AOF rewrite. */

    // AOF 缓冲区
    sds aof_buf;      /* AOF buffer, written before entering the event loop */

    // AOF 文件的描述符
    int aof_fd;       /* File descriptor of currently selected AOF file */

    // AOF 的当前目标数据库
    int aof_selected_db; /* Currently selected DB in AOF */

    // 推迟 write 操作的时间
    time_t aof_flush_postponed_start; /* UNIX time of postponed AOF flush */

    // 最后一直执行 fsync 的时间
    time_t aof_last_fsync;            /* UNIX time of last fsync() */
    time_t aof_rewrite_time_last;   /* Time used by last AOF rewrite run. */

    // AOF 重写的开始时间
    time_t aof_rewrite_time_start;  /* Current AOF rewrite start time. */

    // 最后一次执行 BGREWRITEAOF 的结果
    int aof_lastbgrewrite_status;   /* REDIS_OK or REDIS_ERR */

    // 记录 AOF 的 write 操作被推迟了多少次
    unsigned long aof_delayed_fsync;  /* delayed AOF fsync() counter */

    // 指示是否需要每写入一定量的数据，就主动执行一次 fsync()
    int aof_rewrite_incremental_fsync;/* fsync incrementally while rewriting? */
    int aof_last_write_status;      /* REDIS_OK or REDIS_ERR */
    int aof_last_write_errno;       /* Valid if aof_last_write_status is ERR */
    /* RDB persistence */

    // 自从上次 SAVE 执行以来，数据库被修改的次数
    long long dirty;                /* Changes to DB from the last save */

    // BGSAVE 执行前的数据库被修改次数
    long long dirty_before_bgsave;  /* Used to restore dirty on failed BGSAVE */

    // 负责执行 BGSAVE 的子进程的 ID
    // 没在执行 BGSAVE 时，设为 -1
    pid_t rdb_child_pid;            /* PID of RDB saving child */
    struct saveparam *saveparams;   /* Save points array for RDB */
    int saveparamslen;              /* Number of saving points */
    char *rdb_filename;             /* Name of RDB file */
    int rdb_compression;            /* Use compression in RDB? */
    int rdb_checksum;               /* Use RDB checksum? */

    // 最后一次完成 SAVE 的时间
    time_t lastsave;                /* Unix time of last successful save */

    // 最后一次尝试执行 BGSAVE 的时间
    time_t lastbgsave_try;          /* Unix time of last attempted bgsave */

    // 最近一次 BGSAVE 执行耗费的时间
    time_t rdb_save_time_last;      /* Time used by last RDB save run. */

    // 数据库最近一次开始执行 BGSAVE 的时间
    time_t rdb_save_time_start;     /* Current RDB save start time. */

    // 最后一次执行 SAVE 的状态
    int lastbgsave_status;          /* REDIS_OK or REDIS_ERR */
    int stop_writes_on_bgsave_err;  /* Don't allow writes if can't BGSAVE */


    /* Propagation of commands in AOF / replication */
    redisOpArray also_propagate;    /* Additional command to propagate. */


    /* Logging */
    char *logfile;                  /* Path of log file */
    int syslog_enabled;             /* Is syslog enabled? */
    char *syslog_ident;             /* Syslog ident */
    int syslog_facility;            /* Syslog facility */


    /* Replication (master) */
    int slaveseldb;                 /* Last SELECTed DB in replication output */
    // 全局复制偏移量（一个累计值）
    long long master_repl_offset;   /* Global replication offset */
    // 主服务器发送 PING 的频率
    int repl_ping_slave_period;     /* Master pings the slave every N seconds */

    // backlog 本身
    char *repl_backlog;             /* Replication backlog for partial syncs */
    // backlog 的长度
    long long repl_backlog_size;    /* Backlog circular buffer size */
    // backlog 中数据的长度
    long long repl_backlog_histlen; /* Backlog actual data length */
    // backlog 的当前索引
    long long repl_backlog_idx;     /* Backlog circular buffer current offset */
    // backlog 中可以被还原的第一个字节的偏移量
    long long repl_backlog_off;     /* Replication offset of first byte in the
                                       backlog buffer. */
    // backlog 的过期时间
    time_t repl_backlog_time_limit; /* Time without slaves after the backlog
                                       gets released. */

    // 距离上一次有从服务器的时间
    time_t repl_no_slaves_since;    /* We have no slaves since that time.
                                       Only valid if server.slaves len is 0. */

    // 是否开启最小数量从服务器写入功能
    int repl_min_slaves_to_write;   /* Min number of slaves to write. */
    // 定义最小数量从服务器的最大延迟值
    int repl_min_slaves_max_lag;    /* Max lag of <count> slaves to write. */
    // 延迟良好的从服务器的数量
    int repl_good_slaves_count;     /* Number of slaves with lag <= max_lag. */


    /* Replication (slave) */
    // 主服务器的验证密码
    char *masterauth;               /* AUTH with this password with master */
    // 主服务器的地址
    char *masterhost;               /* Hostname of master */
    // 主服务器的端口
    int masterport;                 /* Port of master */
    // 超时时间
    int repl_timeout;               /* Timeout after N seconds of master idle */
    // 主服务器所对应的客户端
    redisClient *master;     /* Client that is master for this slave */
    // 被缓存的主服务器，PSYNC 时使用
    redisClient *cached_master; /* Cached master to be reused for PSYNC. */
    int repl_syncio_timeout; /* Timeout for synchronous I/O calls */
    // 复制的状态（服务器是从服务器时使用）
    int repl_state;          /* Replication status if the instance is a slave */
    // RDB 文件的大小
    off_t repl_transfer_size; /* Size of RDB to read from master during sync. */
    // 已读 RDB 文件内容的字节数
    off_t repl_transfer_read; /* Amount of RDB read from master during sync. */
    // 最近一次执行 fsync 时的偏移量
    // 用于 sync_file_range 函数
    off_t repl_transfer_last_fsync_off; /* Offset when we fsync-ed last time. */
    // 主服务器的套接字
    int repl_transfer_s;     /* Slave -> Master SYNC socket */
    // 保存 RDB 文件的临时文件的描述符
    int repl_transfer_fd;    /* Slave -> Master SYNC temp file descriptor */
    // 保存 RDB 文件的临时文件名字
    char *repl_transfer_tmpfile; /* Slave-> master SYNC temp file name */
    // 最近一次读入 RDB 内容的时间
    time_t repl_transfer_lastio; /* Unix time of the latest read, for timeout */
    int repl_serve_stale_data; /* Serve stale data when link is down? */
    // 是否只读从服务器？
    int repl_slave_ro;          /* Slave is read only? */
    // 连接断开的时长
    time_t repl_down_since; /* Unix time at which link with master went down */
    // 是否要在 SYNC 之后关闭 NODELAY ？
    int repl_disable_tcp_nodelay;   /* Disable TCP_NODELAY after SYNC? */
    // 从服务器优先级
    int slave_priority;             /* Reported in INFO and used by Sentinel. */
    // 本服务器（从服务器）当前主服务器的 RUN ID
    char repl_master_runid[REDIS_RUN_ID_SIZE+1];  /* Master run id for PSYNC. */
    // 初始化偏移量
    long long repl_master_initial_offset;         /* Master PSYNC offset. */


    /* Replication script cache. */
    // 复制脚本缓存
    // 字典
    dict *repl_scriptcache_dict;        /* SHA1 all slaves are aware of. */
    // FIFO 队列
    list *repl_scriptcache_fifo;        /* First in, first out LRU eviction. */
    // 缓存的大小
    int repl_scriptcache_size;          /* Max number of elements. */

    /* Synchronous replication. */
    list *clients_waiting_acks;         /* Clients waiting in WAIT command. */
    int get_ack_from_slaves;            /* If true we send REPLCONF GETACK. */
    /* Limits */
    int maxclients;                 /* Max number of simultaneous clients */
    unsigned long long maxmemory;   /* Max number of memory bytes to use */
    int maxmemory_policy;           /* Policy for key eviction */
    int maxmemory_samples;          /* Pricision of random sampling */


    /* Blocked clients */
    unsigned int bpop_blocked_clients; /* Number of clients blocked by lists */
    list *unblocked_clients; /* list of clients to unblock before next loop */
    list *ready_keys;        /* List of readyList structures for BLPOP & co */


    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    int sort_store;


    /* Zip structure config, see redis.conf for more information  */
    size_t hash_max_ziplist_entries;
    size_t hash_max_ziplist_value;
    size_t list_max_ziplist_entries;
    size_t list_max_ziplist_value;
    size_t set_max_intset_entries;
    size_t zset_max_ziplist_entries;
    size_t zset_max_ziplist_value;
    size_t hll_sparse_max_bytes;
    time_t unixtime;        /* Unix time sampled every cron cycle. */
    long long mstime;       /* Like 'unixtime' but with milliseconds resolution. */


    /* Pubsub */
    // 字典，键为频道，值为链表
    // 链表中保存了所有订阅某个频道的客户端
    // 新客户端总是被添加到链表的表尾
    dict *pubsub_channels;  /* Map channels to list of subscribed clients */

    // 这个链表记录了客户端订阅的所有模式的名字
    list *pubsub_patterns;  /* A list of pubsub_patterns */

    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of REDIS_NOTIFY... flags. */


    /* Cluster */

    int cluster_enabled;      /* Is cluster enabled? */
    mstime_t cluster_node_timeout; /* Cluster node timeout. */
    char *cluster_configfile; /* Cluster auto-generated config file name. */
    struct clusterState *cluster;  /* State of the cluster */

    int cluster_migration_barrier; /* Cluster replicas migration barrier. */
    /* Scripting */

    // Lua 环境
    lua_State *lua; /* The Lua interpreter. We use just one for all clients */
    
    // 复制执行 Lua 脚本中的 Redis 命令的伪客户端
    redisClient *lua_client;   /* The "fake client" to query Redis from Lua */

    // 当前正在执行 EVAL 命令的客户端，如果没有就是 NULL
    redisClient *lua_caller;   /* The client running EVAL right now, or NULL */

    // 一个字典，值为 Lua 脚本，键为脚本的 SHA1 校验和
    dict *lua_scripts;         /* A dictionary of SHA1 -> Lua scripts */
    // Lua 脚本的执行时限
    mstime_t lua_time_limit;  /* Script timeout in milliseconds */
    // 脚本开始执行的时间
    mstime_t lua_time_start;  /* Start time of script, milliseconds time */

    // 脚本是否执行过写命令
    int lua_write_dirty;  /* True if a write command was called during the
                             execution of the current script. */

    // 脚本是否执行过带有随机性质的命令
    int lua_random_dirty; /* True if a random command was called during the
                             execution of the current script. */

    // 脚本是否超时
    int lua_timedout;     /* True if we reached the time limit for script
                             execution. */

    // 是否要杀死脚本
    int lua_kill;         /* Kill the script if true. */


    /* Assert & bug reporting */

    char *assert_failed;
    char *assert_file;
    int assert_line;
    int bug_report_start; /* True if bug report header was already logged. */
    int watchdog_period;  /* Software watchdog period in ms. 0 = off */
};


typedef struct {
	double min, max;
	//if minex is 1, it means we want don't want the =, minex is 0, means we want =.
	int minex, maxex;
}zrangespec;

/*Structure to hold an inclusive/exclusive range spec by lexicographic comparsion.*/
typedef struct{
	robj *min, *max;
	int minex, maxex;
}zlexrangespec;

zskiplist *zslCreate(void);
void zslFree(zskiplist *zsl);
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj);
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score);
int zslDelete(zskiplist *zsl, double score, robj *obj);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
double zzlGetScore(unsigned char *sptr);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned int zsetLength(robj *zobj);
void zsetConvert(robj *zobj, int encoding);
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o);

/* Structure to hold list iteration abstraction.
 *
 * 列表迭代器对象
 */
typedef struct {

    // 列表对象
    robj *subject;

    // 对象所使用的编码
    unsigned char encoding;

    // 迭代的方向
    unsigned char direction; /* Iteration direction */

    // ziplist 索引，迭代 ziplist 编码的列表时使用
    unsigned char *zi;

    // 链表节点的指针，迭代双端链表编码的列表时使用
    listNode *ln;

} listTypeIterator;

/* Structure for an entry while iterating over a list.
 *
 * 迭代列表时使用的记录结构，
 * 用于保存迭代器，以及迭代器返回的列表节点。
 */
typedef struct {

    // 列表迭代器
    listTypeIterator *li;

    // ziplist 节点索引
    unsigned char *zi;  /* Entry in ziplist */

    // 双端链表节点指针
    listNode *ln;       /* Entry in linked list */

} listTypeEntry;

/**Structure to hold set iteration abstraction.
 */ 
typedef struct{

	robj *subject;

	int encoding;
	//The index for iterate intset
	int ii;
	//The iterator for iterate the hashtable
	dictIterator *di;

}setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
/*
 * 哈希对象的迭代器
 */
typedef struct {

    // 被迭代的哈希对象
    robj *subject;

    // 哈希对象的编码
    int encoding;

    // 域指针和值指针
    // 在迭代 ZIPLIST 编码的哈希对象时使用
    unsigned char *fptr, *vptr;

    // 字典迭代器和指向当前迭代字典节点的指针
    // 在迭代 HT 编码的哈希对象时使用
    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern dictType setDictType;
extern dictType zsetDictType;
extern dictType clusterNodesDictType;
extern dictType clusterNodesBlackListDictType;
extern dictType dbDictType;
extern dictType shaScriptObjectDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType replScriptCacheDictType;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/
/* Utils */
long long ustime(void);
long long mstime(void);
void getRandomHexChars(char *p, unsigned int len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
size_t redisPopcount(void *s, long count);
void redisSetProcTitle(char *title);

/* networking.c -- Networking and Client related operations */
redisClient *createClient(int fd);
void closeTimedoutClients(void);
void freeClient(redisClient *c);
void freeClientAsync(redisClient *c);
void resetClient(redisClient *c);
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask);
void addReply(redisClient *c, robj *obj);
void *addDeferredMultiBulkLength(redisClient *c);
void setDeferredMultiBulkLength(redisClient *c, void *node, long length);
void addReplySds(redisClient *c, sds s);
void processInputBuffer(redisClient *c);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
void addReplyBulk(redisClient *c, robj *obj);
void addReplyBulkCString(redisClient *c, char *s);
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len);
void addReplyBulkLongLong(redisClient *c, long long ll);
void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void addReply(redisClient *c, robj *obj);
void addReplySds(redisClient *c, sds s);
void addReplyError(redisClient *c, char *err);
void addReplyStatus(redisClient *c, char *status);
void addReplyDouble(redisClient *c, double d);
void addReplyLongLong(redisClient *c, long long ll);
void addReplyMultiBulkLen(redisClient *c, long length);
void copyClientOutputBuffer(redisClient *dst, redisClient *src);
void *dupClientReplyValue(void *o);
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer);
void formatPeerId(char *peerid, size_t peerid_len, char *ip, int port);
char *getClientPeerId(redisClient *client);
sds catClientInfoString(sds s, redisClient *client);
sds getAllClientsInfoString(void);
void rewriteClientCommandVector(redisClient *c, int argc, ...);
void rewriteClientCommandArgument(redisClient *c, int i, robj *newval);
unsigned long getClientOutputBufferMemoryUsage(redisClient *c);
void freeClientsInAsyncFreeQueue(void);
void asyncCloseClientOnOutputBufferLimitReached(redisClient *c);
int getClientLimitClassByName(char *name);
char *getClientLimitClassName(int class);
void flushSlavesOutputBuffers(void);
void disconnectSlaves(void);
int listenToPort(int port, int *fds, int *count);
void pauseClients(mstime_t duration);
int clientsArePaused(void);
int processEventsWhileBlocked(void);

#ifdef __GNUC__
void addReplyErrorFormat(redisClient *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(redisClient *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormat(redisClient *c, const char *fmt, ...);
void addReplyStatusFormat(redisClient *c, const char *fmt, ...);
#endif

/* Core functions */
int freeMemoryIfNeeded(void);
int processCommand(redisClient *c);
void setupSignalHandlers(void);
struct redisCommand *lookupCommand(sds name);
struct redisCommand *lookupCommandByCString(char *s);
struct redisCommand *lookupCommandOrOriginal(sds name);
void call(redisClient *c, int flags);
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int flags);
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int target);
void forceCommandPropagation(redisClient *c, int flags);
int prepareForShutdown();
#ifdef __GNUC__
void redisLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void redisLog(int level, const char *fmt, ...);
#endif
void redisLogRaw(int level, const char *msg);
void redisLogFromHandler(int level, const char *msg);
void usage();
void updateDictResizePolicy(void);
int htNeedsResize(dict *dict);
void oom(const char *msg);
void populateCommandTable(void);
void resetCommandTableStats(void);
void adjustOpenFilesLimit(void);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(void);
void resetServerStats(void);
unsigned int getLRUClock(void);

/* List data type*/
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subj, robj *value, int where);
robj* listTypePop(robj *subject, int where);
unsigned long listTypeLength(robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry);
void listTypeDelete(listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
void unblockClientWaitingData(redisClient *c);
void handleClientBlockedOnList(void);
void popGenericCommand(redisClient *c, int where);

/*Set data type*/
robj *setTypeCreate(robj *value);
int setTypeAdd(robj *subject, robj *value);
int setTypeRemove(robj *subject, robj *value);
int setTypeIsMember(robj *subject, robj *value);
setTypeIterator *setTypeInitIterator(robj *subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele);
robj *setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele);
unsigned long setTypeSize(robj *subject);
void setTypeConvert(robj *subject, int enc);

/*Hash data type*/
void hashTypeConvert(robj *o, int enc);
void hashTypeTryConversion(robj *subject, robj **argv, int start, int end);
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2);
robj *hashTypeGetObject(robj *o, robj *key);
int hashTypeExists(robj *o, robj *key);
int hashTypeSet(robj *o, robj *key, robj *value);
int hashTypeDelete(robj *o, robj *key);
unsigned long hashTypeLength(robj *o);
hashTypeIterator *hashTypeInitIterator(robj *subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
							     unsigned char **vstr, 
								 unsigned int *vlen,
								 long long *vll);
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst);
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what);
robj *hashTypeLookupWirteOrCreate(redisClient *c, robj *key);

/* MULTI/EXEC/WATCH... */
void unwatchAllKeys(redisClient *c);
void initClientMultiState(redisClient *c);
void freeClientMultiState(redisClient *c);
void queueMultiCommand(redisClient *c);
void touchWatchedKey(redisDb *db, robj *key);
void touchWatchedKeysOnFlush(int dbid);
void discardTransaction(redisClient *c);
void flagTransaction(redisClient *c);

/*db.c --Key space access API */
int removeExpire(redisDb *db, robj *key);
void propagateExpire(redisDb *db, robj *key);
int expireIfNeeded(redisDb *db, robj *key);
long long getExpire(redisDb *db, robj *key);
void setExpire(redisDb *db, robj *key, long long when);
robj *lookupKey(redisDb *db, robj *key);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyWirte(redisDb *db, robj *key);
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(redisClient *c, robj *key, robj *reply);
void dbAdd(redisDb *db, robj *key, robj *val);
void dbOverWrite(redisDb *db, robj *key, robj *val);
void setKey(redisDb *db, robj *key, robj *val);
int dbExists(redisDb *db, robj *key);
robj *dbRandomKey(redisDb *db);
int dbDelete(redisDb *db, robj *key);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);
long long emptyDb(void(callback)(void*));
int selectDb(redisClient *c, int id);
void signalModifiedKey(redisDb *db, robj *key);
void signalFlushedDb(int dbid);
unsigned int getKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count);
unsigned int countKeysInSlot(unsigned int hashslot);
unsigned int delKeysInSlot(unsigned int hashslot);
int verifyClusterConfigWithData(void);
void scanGenericCommand(redisClient *c, robj *o, unsigned long cursor);
int parseScanCursorOrReply(redisClient *c, robj *o, unsigned long *cursor);

/* Redis object implementation */
void decrRefCount(robj *o);
void decrRefCountVoid(void *o);
void incrRefCount(robj *o);
robj *resetRefCount(robj *obj);
void freeStringObject(robj *o);
void freeListObject(robj *o);
void freeSetObject(robj *o);
void freeZsetObject(robj *o);
void freeHashObject(robj *o);
robj *createObject(int type, void *ptr);
robj *createStringObject(char *ptr, size_t len);
robj *createRawStringObject(char *ptr, size_t len);
robj *createEmbeddedStringObject(char *ptr, size_t len);
robj *dupStringObject(robj *o);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *getDecodedObject(robj *o);
size_t stringObjectLen(robj *o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongDouble(long double value);
robj *createListObject(void);
robj *createZiplistObject(void);
robj *createSetObject(void);
robj *createIntsetObject(void);
robj *createHashObject(void);
robj *createZsetObject(void);
robj *createZsetZiplistObject(void);
int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg);
int checkType(redisClient *c, robj *o, int type);
int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg);
int getLongLongFromObject(robj *o, long long *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg);
char *strEncoding(int encoding);
int compareStringObjects(robj *a, robj *b);
int collateStringObjects(robj *a, robj *b);
int equalStringObjects(robj *a, robj *b);
unsigned long long estimateObjectIdleTime(robj *o);
#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)

/* Command prototypes */
/* String related commands*/
void setCommand(redisClient *c);
void setnxCommand(redisClient *c);
void setexCommand(redisClient *c);
void psetexCommand(redisClient *c);
void getCommand(redisClient *c);
void setrangeCommand(redisClient *c);
void getrangeCommand(redisClient *c);
void incrCommand(redisClient *c);
void decrCommand(redisClient *c);
void incrbyCommand(redisClient *c);
void decrbyCommand(redisClient *c);
void incrbyfloatCommand(redisClient *c);
/* list related command  */
void lpushCommand(redisClient *c);
void rpushCommand(redisClient *c);
void lpushxCommand(redisClient *c);
void rpushxCommand(redisClient *c);
void linsetCommand(redisClient *c);
void lpopCommand(redisClient *c);
void rpopCommand(redisClient *c);
void llenCommand(redisClient *c);
void lindexCommand(redisClient *c);
void lrangeCommand(redisClient *c);
void ltrimCommand(redisClient *c);
void lsetCommand(redisClient *c);
void lremCommand(redisClient *c);
/*hash map related command*/
void hsetCommand(redisClient *c);
void hsetnxCommand(redisClient *c);
void hgetCommand(redisClient *c);
void hmsetCommand(redisClient *c);
void hmgetCommand(redisClient *c);
void hdelCommand(redisClient *c);
void hlenCommand(redisClient *c);
void hkeysCommand(redisClient *c);
void hvalsCommand(redisClient *c);
void hgetallCommand(redisClient *c);
void hexistsCommand(redisClient *c);
void hscanCommand(redisClient *c);
void hincrByCommand(redisClient *c);
void hincrByfloatCommand(redisClient *c);
/*Set relate Command*/
void saddCommand(redisClient *c);
void sremCommand(redisClient *c);
void smoveCommand(redisClient *c);
void sismemberCommand(redisClient *c);
void scardCommand(redisClient *c);
void spopCommand(redisClient *c);
void srandmemberCommand(redisClient *c);
void sinterCommand(redisClient *c);
void sinterstoreCommand(redisClient *c);
void sunionCommand(redisClient *c);
void sunionstoreCommand(redisClient *c);
void sdiffCommand(redisClient *c);
void sdiffstoreCommand(redisClient *c);
void sscanCommand(redisClient *c);
/*zset relate Command */
void zaddCommand(redisClient *c);
void zincrbyCommand(redisClient *c);
void zrangeCommand(redisClient *c);
void zrangebyscoreCommand(redisClient *c);
void zrevrangebyscoreCommand(redisClient *c);
void zrangebylexCommand(redisClient *c);
void zrevrangebylexCommand(redisClient *c);
void zcountCommand(redisClient *c);
void zlexcountCommand(redisClient *c);
void zrevrangeCommand(redisClient *c);
void zcardCommand(redisClient *c);
void zremCommand(redisClient *c);
void zscoreCommand(redisClient *c);
void zremrangebyscoreCommand(redisClient *c);
void zremrangebylexCommand(redisClient *c);
void zrankCommand(redisClient *c);
void zrevrankCommand(redisClient *c);
void zremrangebyrankCommand(redisClient *c);
void zunionstoreCommand(redisClient *c);
void zinterstoreCommand(redisClient *c);
void objectCommand(redisClient *c);


/* Debugging stuff */
void _redisAssertWithInfo(redisClient *c, robj *o, char *estr, char *file, int line);
void _redisAssert(char *estr, char *file, int line);
void _redisPanic(char *msg, char *file, int line);
void bugReportStart(void);
void redisLogObjectDebugInfo(robj *o);
void sigsegvHandler(int sig, siginfo_t *info, void *secret);
sds genRedisInfoString(char *section);
void enableWatchdog(int period);
void disableWatchdog(void);
void watchdogScheduleSignal(int period);
void redisLogHexDump(int level, char *descr, void *value, size_t len);

#define redisDebug(fmt, ...) \
    printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define redisDebugMark() \
    printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

#endif