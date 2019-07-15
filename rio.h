#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"

/*
 * RIO API 接口和状态
 */
struct _rio {

    /* Backend functions.
     * Since this functions do not tolerate short writes or reads the return
     * value is simplified to: zero on error, non zero on complete success. */
    // API
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);

    /* The update_cksum method if not NULL is used to compute the checksum of
     * all the data that was read or written so far. The method should be
     * designed so that can be called with the current checksum, and the buf
     * and len fields pointing to the new block of data to add to the checksum
     * computation. */
    // 校验和计算函数，每次有写入/读取新数据时都要计算一次
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum */
    // 当前校验和
    uint64_t cksum;

    /* number of bytes read or written */
    size_t processed_bytes;

    /* maximum single read or write chunk size */
    size_t max_processing_chunk;

    /* Backend-specific vars. */
    union {

        struct {
            // 缓存指针
            sds ptr;
            // 偏移量
            off_t pos;
        } buffer;

        struct {
            // 被打开文件的指针
            FILE *fp;
            // 最近一次 fsync() 以来，写入的字节量
            off_t buffered; /* Bytes written since last fsync. */
            // 写入多少字节之后，才会自动执行一次 fsync()
            off_t autosync; /* fsync after 'autosync' bytes written. */
        } file;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */

/**
 * 写点我自己的理解，对于底层的io操作有些地方其实很坑，
 * 1.比如对于read操作，如果读取的描述符剩下的字节数（比如20个）少于我们要求读取的个数（比如50）
 * 那么第一次当read返回时，会返回剩下的字节数（比如20个），下一次的read操作会遇到EOF，返回0.
 * 那么我们想要有一个函数Wrapper 底层的read操作，只接受我们指定的输入，如果出现剩余字节数少于
 * 我们需求时，返回剩余的字节数，函数结束，不会再返回一个0的情况。
 * 2.底层的io函数往往缺少对于被信号中断时的恢复机制，我们的需要Wrapper low level function
 * 使得他可以从信号处理中恢复。
 */

/* 
 * Write len of bytes from buf into the r
 * 
 * 写入成功返回实际写入的字节数，写入失败返回 -1 。
 */
static inline size_t rioWrite(rio *r, const void *buf, size_t len){
    while(len > 0){
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if(r->update_cksum) r->update_cksum(r, buf, bytes_to_write);
        //这里的写函数 很有意思 可以看作他将要写入的数据看作一个整体一次性写入，
        //写入成功返回1 否则返回 0， 所以这里可以看到 他和以往的函数 
        //int written = write(fd, buf, sizeof(buf))
        //len -= written
        //的模式不同 就在于他这个函数 要是写入成功，写入的字节数肯定是bytes_to_write这么多
        if(r->write(r, buf, bytes_to_write) == 0)
            return 0;
        buf = (char *)buf + bytes_to_write;
        len -= bytes_to_write;
        r->processed_bytes += bytes_to_write;
    }
    return 1;
}

/* Read len bytes from the r, and save the content into buf */
static inline size_t rioRead(rio *r, void *buf, size_t len){
    while(len > 0){
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        if(r->update_cksum) r->update_cksum(r, buf, bytes_to_read);
        if(r->read(r, buf, bytes_to_read) == 0)
            return 0;

        buf = (char *)buf + bytes_to_read;
        len -= bytes_to_read;
        r->processed_bytes += bytes_to_read;
    }
    return 1;
}

/* Returns the current r offset */
static inline off_t rioTell(rio *r){
    return r->tell(r);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);

size_t rioWriteBulkCount(rio *r, char prefix, int count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);
#endif 