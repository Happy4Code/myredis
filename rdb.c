#include "redis.h"
#include "lzf.h"
#include "zipmap.h"
#include "endianconv.h"
#include "rdb.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

/* Write the string of `len` lengths into rdb file.
   If success return len, otherwise return -1
*/
static int rdbWriteRaw(rio *rdb, void *p, size_t len){
    if(rdb && rioWrite(rdb, p, len) == 0) return -1;
    return len;
}

/* Write only 1 byte character `type` into the rdb file. */
int rdbSaveType(rio *rdb, unsigned char type){
    return rdbWriteRaw(rdb, &type, 1);
}

/* Load a "type" in RDB format, that is a one byte unsigned integer.
 * 
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, EXPIRE type, and so forth. 
 * 函数即可以用于载入键的类型（rdb.h/REDIS_RDB_TYPE_*），
 * 也可以用于载入特殊标识号（rdb.h/REDIS_RDB_OPCODE_*）
 */
int rdbLoadType(rio *rdb){
    unsigned char type;

    if(rioRead(rdb, &type, 1) == 0) return -1;

    return type;
}

/* Load the time of second UNIT */
time_t rdbLoadTime(rio *rdb){
    int32_t t32;
    if(rioRead(rdb, &t32, 4) == 0) return -1;
    return(time_t)t32;
}

/* Save the time of millisecond UNIT*/
int rdbSaveMillisecondTime(rio *rdb, long long t){
    int64_t t64 = (int64_t)t;
    return rdbWriteRaw(rdb, &t64, 8);
}

/* Load the time of millisecond UNIT */
long long rdbLoadMillsecondTime(rio *rdb){
    int64_t t64;
    if(rioRead(rdb, &t64, 8) == 0) return -1;
    return (long long)t64;
}

/* Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the REDIS_RDB_* definitions for more information
 * on the types of encoding. 
 *
 * 对 len 进行特殊编码之后写入到 rdb 。
 *
 * 写入成功返回保存编码后的 len 所需的字节数。
 */
int rdbSaveLen(rio *rdb, uint32_t len){
    unsigned char buf[2];
    size_t nwritten;

    //Check if the len can be represent as a 6bit variable.
    if(len < (1<<6)){
        //Save 6bit len
        //This code works just because for length that can be represented
        //by 6 bits long.The most 2 sigificant bit must be 00, so len & 0XFF
        //will leave the 2 most sigificant bit to 00, other 6bits to it corresponding
        //value.The we use | operator to put the most siginifant bit to REDIS_RDB_6BITLEN
        buf[0] = (len & 0xFF)|(REDIS_RDB_6BITLEN<<6);
        if(rdbWriteRaw(rdb, buf, 1) == -1) return -1;
        nwritten = 1;
    }else if(len < (1 << 14)){
        //Check if this varaible
        //0000 00XF
        //We explain the following code, len >> 8 means we want to acquired the 
        //X part value, we use them to & 0xFF get the vaue.The same thoughts as 
        //previous part, we use the | operator to set the 2-MSB to REDIS_RDB_14BITLEN
        buf[0] = ((len>>8) & 0xFF)|(REDIS_RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        if(rdbWriteRaw(rdb, buf, 2) == -1) return -1;
        nwritten = 2; 
    }else{
        //Save 32bit len
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if(rdbWriteRaw(rdb, buf, 1) == -1) return -1;
        //Notice this part, it convert the length from host endian into
        //network endian which is big-endian.
        len = htonl(len);
        if(rdbWriteRaw(rdb, &len, 4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}

/* Load an encoded length. The "isencoded" argument is set to 1 if the length
 * is not actually a length but an "encoding type". See the REDIS_RDB_ENC_*
 * definitions in rdb.h for more information. 
 *
 * 读入一个被编码的长度值。
 *
 * 如果 length 值不是整数，而是一个被编码后值，那么 isencoded 将被设为 1 。
 *
 * 查看 rdb./hREDIS_RDB_ENC_* 定义以获得更多信息。
 */
uint32_t rdbLoadLen(rio *rdb, int *isencoded){
    unsigned char buf[2];
    uint32_t len;
    int type;

    if(isencoded) *isencoded = 0;

    //Read 1 byte try, because the first bytes 2MSB contains the info.
    if(rioRead(rdb, buf, 1) == 0) return REDIS_RDB_LENERR;
    //After this statement the type save the value in the 2MSB
    type = (buf[0]&0xC0) >> 6;

    if(type == REDIS_RDB_ENCVAL){
        //Read a 6 bit encoding type
        if(isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    
    } else if(type == REDIS_RDB_6BITLEN){
        return buf[0]&0x3F;
    
    }else if(type == REDIS_RDB_14BITLEN){
        if(rioRead(rdb, buf+1, 1) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8 | buf[1]);
    //IF this length is saved in a 32 bit length;
    }else{
        if(rioRead(rdb, &len, 4) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}
/* Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. 
 *
 * 尝试使用特殊的整数编码来保存 value ，这要求它的值必须在给定范围之内。
 *
 * 如果可以编码的话，将编码后的值保存在 enc 指针中，
 * 并返回值在编码后所需的长度。
 *
 * 如果不能编码的话，返回 0 。
 */
int rdbEncodeInteger(long long value, unsigned char *enc){
    //Check if it within -128~127
    if(value >= -(1<<7) && value <= (1<<7) - 1){
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    }else if(value >= -(1<<15) && value <= (1<<15) - 1){
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        //Those two statement they do one thing, put the low part into enc[1], high
        //part into the enc[2]
        // XXXX   YYYY
        // |-H-|  |-L-|  
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    }else if(value >= -((long long)1<<31) && value <= ((long long)1<<31) - 1){
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    }else{
        return 0;
    }
}

/* Loads an integer-encoded object with the specified encoding type "enctype".
 *
 * 载入被编码成指定类型的编码整数对象。
 *
 * If the "encode" argument is set the function may return an integer-encoded
 * string object, otherwise it always returns a raw string object. 
 *
 * 如果 encoded 参数被设置了的话，那么可能会返回一个整数编码的字符串对象，
 * 否则，字符串总是未编码的。
 * 
 * 这个函数 是配合 rdbEncodeInteger(long long value, unsigned char *enc) 这个一起使用的
 */
robj *rdbLoadIntegerObject(rio *rdb, int enctype, int encode){
    unsigned char enc[4];
    long long val;

    //If is encoded as REDIS_RDB_ENCVAL, the low 2bit indicate which enctype
    //this belong to.
    /**
     * 下面这段代码有一些小细节需要注意，首先注意到了 long long val = (signed char)enc[0];
     * 其实就是简单的一个操作， 将处于enc 无符号数组里的第一个数字 转换成longlong类型
     * 为什么不写成 (long long)enc[0] 要写成 (signed char)enc[0]这样
     * 原因如下：
     * 由于数据最开始存在于一个无符号的数组中 那么比如 enc[0] = 255，由于最后
     * 用于承载数据的val是一个long long类型，这意味着如果如果 enc中的数据应该被当作
     * 有符号数字看待， 直接val = (long long)enc[0],此时val = 255,但是其实
     * 我们想表示的是-1。所以我们先用他合适大小的有符号容器将其 转化为有符号数，
     * 随后在将其付值给long long 类型的val，他会做signed extension.保证其是-1，
     * 下面几个同理。
     */

    if(enctype == REDIS_RDB_ENC_INT8){
        if(rioRead(rdb, enc, 1) == 0) return NULL;
        val = (signed char)enc[0];
    }else if(enctype == REDIS_RDB_ENC_INT16){
        uint16_t v;
        if(rioRead(rdb, enc, 2) == 0) return NULL;
        v = enc[0]|(enc[1] <<8);
        val = (int16_t)v;
    }else if(enctype == REDIS_RDB_ENC_INT32){
        uint32_t v;
        if(rioRead(rdb, enc, 4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    }else{
        val = 0;
        redisPainc("Unkonwn RDB integer encoding type");
    }

    if(encode)
        return createStringObjectFromLongLong(val);
    else
        return createObject(REDIS_STRING, sdsfromlonglong(val));
}

/* String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space 
 *
 * 那些保存像是 "2391" 、 "-100" 这样的字符串的字符串对象，
 * 可以将它们的值保存到 8 位、16 位或 32 位的带符号整数值中，
 * 从而节省一些内存。
 *
 * 这个函数就是尝试将字符串编码成整数，
 * 如果成功的话，返回保存整数值所需的字节数，这个值必然大于 0 。
 *
 * 如果转换失败，那么返回 0 。
 */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc){
    long long value;
    char *endptr, buf[32];

    //Check if it is possible to encode this value as number
    value = strtoll(s, &endptr, 10);
    if(endptr[0] != '\0') return 0;

    //Convert this back to string
    ll2string(buf, 32, value);

    /* If the number converted back into a string is not identical then it's not
     * possible to encode the string as integer
     * Eg: 
     *    "1 234"  ----> 1
     *    "  1234" ----> 1234
     * 所以将转换的数字 转换回字符串 通过长度检查 确保他们是一致的是必要的 
     */
    if(strlen(buf) != len || memcpy(buf, s, len)) return 0;

    return rdbEncodeInteger(value, enc);
}

/*
 * 尝试对输入字符串 s 进行压缩，
 * 如果压缩成功，那么将压缩后的字符串保存到 rdb 中。
 *
 * 函数在成功时返回保存压缩后的 s 所需的字节数，
 * 压缩失败或者内存不足时返回 0 ，
 * 写入失败时返回 -1 。
 */
int rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len){
    size_t comprlen, outlen;
    unsigned char byte;
    int n, nwritten = 0;
    void *out;

    /*We require at least four bytes compression for this to be worth it */
    if(len <= 4) return 0;
    outlen = len - 4;
    if((out = zmalloc(outlen+1)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if(comprlen == 0){
        zfree(out);
        return 0;
    }

    //The data compression success! Save it to the disk.
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    /* Write as the following format
     *  ----------------- -------------- ------------ ---------------------
     * | REDIS_RDB_ENC_LZF| compress_len | origin_len |  compressed_string |
     *  ----------------- -------------- ------------ ---------------------
     */
    ///Write the REDIS_RDB_ENC_LZF part
    if((n = rdbWriteRaw(rdb, &byte, 1)) == -1) goto writeerr;
    nwritten += n;

    //Write the compress_len part
    if((n = rdbSaveLen(rdb, comprlen)) == -1) goto writeerr;
    nwritten += n;
    
    //Write the origin_len part
    if((n = rdbSaveLen(rdb, len)) == -1) goto writeerr;
    nwritten += n;
    //Write the compressed_string part
    if((n = rdbWriteRaw(rdb, out, comprlen)) == -1) goto writeerr;
    nwritten += n;

    zfree(out);
    return nwritten;

    writeerr:
        zfree(out);
        return -1;
}

/*
 * 从 rdb 中载入被 LZF 压缩的字符串，解压它，并创建相应的字符串对象。
 */
robj *rdbLoadLzfStringObject(rio *rdb){
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    //read the compress_len part
    if((clen = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;
    //Read the uncompress_len part
    if((clen = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;
    //Allocate the room
    if((c = zmalloc(clen)) == NULL) goto err;
    //Init the sds
    if((val = sdsnewlen(NULL, len)) == NULL) goto err;

    //Read the compress part
    if(rioRead(rdb, c, clen) == 0) goto err;

    //Uncompress get the string
    if(lzf_decompress(c, clen, val, len) == 0) goto err;
    
    zfree(c);
    //Return the string object
    return createObject(REDIS_STRING, val);

    err: 
        zfree(c);
        sdsfree(val);
        return NULL;
}

/*
 * 看到这里 针对上面的几个函数做一下总结：
 * rdbSaveLen 和 rdbLoadLen 这一对函数 用于将length按照其大小
 * 存入以 
 * 1.REDIS_RDB_6BITLEN
 * 2.REDIS_RDB_14BITLEN
 * 3.REDIS_RDB_32BITLEN
 * 三种形式存入写入rdb文件中，他不能写入 REDIS_RDB_ENCVAL*类型的数据
 * 而rdbLoadLen 读取以下类型的数据，针对前三种他会返回 相应数据的大小
 * 1.REDIS_RDB_6BITLEN
 * 2.REDIS_RDB_14BITLEN
 * 3.REDIS_RDB_32BITLEN
 * 4.REDIS_RDB_ENCVAL 
 * 对于第四种，他只返回对应的剩余的6位的值，这个值可以用于确定数据具体是
 * REDIS_RDB_ENC_INT8 0      
 * REDIS_RDB_ENC_INT16 1      
 * REDIS_RDB_ENC_INT32 2       
 * REDIS_RDB_ENC_LZF 3   
 * 中的那种类型。
 * 
 * 所以用起来的时候，都是rdbLoadLen 根据返回的isEncoded的值是否为1 确定是否是一个
 * REDIS_RDB_ENCVAL 类型的，如果是则根据 相应类型调用 rdbLoadLzfStringObject（LZF）
 * 或 rdbLoadIntegerObject 进行操作。
 * 
 * 同理在写入的时候 针对REDIS_RDB_ENCVAL这一类的 根据写入数据类型 使用
 * rdbSaveLzfStringObject 或 rdbTryIntegerEncoding 进行写入
 * 
 * 针对LZF内部的长度 我们都是只通过 rdbSaveLen 和 rdbLoadLen 来操作的，这保证了
 * 长度不会出现REDIS_RDB_ENCVAL 类型的。
 */

/* Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form 
 *
 * 以 [len][data] 的形式将字符串对象写入到 rdb 中。
 *
 * 如果对象是字符串表示的整数值，那么程序尝试以特殊的形式来保存它。
 *
 * 函数返回保存字符串所需的空间字节数。
 */
int rdbSaveRawString(rio *rdb, unsigned char *s, size_t len){
    int enclen;
    int n, nwritten = 0;

    /* We make a guess if it is a integer and can be 
     * represent as a less than 32bit number.
     * Think why this part len <= 11, image 
     * the max length of len is 11.
     * -2147483647
     */
    if(len <= 11){
        unsigned char buf[5];
        if((enclen = rdbTryIntegerEncoding((char *)s, len, buf)) > 0){
            //The integer success convert into a RDB format REDIS_RDB_ENCVAL string
            //which saved at s.
            if(rdbWriteRaw(rdb, buf, enclen) == -1) return -1;

            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it 
     *
     * 如果字符串长度大于 20 ，并且服务器开启了 LZF 压缩，
     * 那么在保存字符串到数据库之前，先对字符串进行 LZF 压缩。
     */
    if(server.rdb_compression && len > 20){

        //Try to compress
        n = rdbSaveLzfStringObject(rdb, s, len);
        if(n == -1) return -1;
        if(n > 0) return n;
          
         /* Return the value of 0 means the data can't be compressed, we process it as the old way */
    }

    //When we comes to here, it means the data can't compress and also can't be represent as a integer
    if((n = rdbSaveLen(rdb, len)) == -1) return -1;
    nwritten += n;

    //Write the content
    if(len > 0){
        if(rdbWriteRaw(rdb, s, len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Save a long long value as either an encoded string or a string. 
 *
 * 将输入的 long long 类型的 value 转换成一个特殊编码的字符串，
 * 或者是一个普通的字符串表示的整数，
 * 然后将它写入到 rdb 中。
 *
 * 这里可以注意一下，value是long long类型的，long long类型不少于64位，而我们在表示数字时最多只能
 * 用32位的容器存储，超过32位我们按照字符串来对待。
 * 
 * 函数返回在 rdb 中保存 value 所需的字节数。
 */
int rdbSaveLongLongAsStringObject(rio *rdb, long long value){
    unsigned char buf[32];
    int n, nwritten = 0;

    //We first to try to encode the value as integer format
    int enclen = rdbEncodeInteger(value, buf);
    //If it can be represent as less than 32bit integer
    if(enclen > 0){
        return rdbWriteRaw(rdb, buf, enclen);
    }else{
        //When we comes to here, it means the `value` can not be
        //represent as a 32 bit integer, so we treat then as a string.
        enclen = ll2string((char*)buf, 32, value);
        redisAssert(enclen < 32);
        //Try to decide how to save the length
        if((n = rdbSaveLen(rdb, enclen)) == -1) return -1;
            nwritten += n;
        if((n = rdbWriteRaw(rdb, buf, enclen) == -1)) return -1;
            nwritten += n;
        return nwritten;
    }
}

/* 
 * Save the sds obj into the rdb 
 */
int rdbSaveStringObject(rio *rdb, robj *obj){
    /* 由于sds的encoding有三种形式REDIS_ENCODING_INT、REDIS_ENCODING_RAW、REDIS_ENCODING_EMBER
     * 对于REDIS_ENCODING_RAW和REDIS_ENCODING_EMBER他们的处理方式是一样的，而REDIS_ENCODING_INT不一样
     */
    if(obj->encoding == REDIS_ENCODING_INT)
        return rdbSaveLongLongAsStringObject(rdb, (long)obj->ptr);
    else{
        redisAssertWithInfo(NULL, obj, sdsEncodedObject(obj));
        return rdbSaveRawString(rdb, obj->ptr, sdslen(obj->ptr));
    }
}

/*
 * 从rdb中载入一个字符串对象
 * encode 用来表示当字符串中包含的信息是一个数字时，是否对他进行encode，
 * 说白了是否进行encode 对应底层两个不同方法：
 * encode：createStringObjectFromLongLong
 * 不encode：createObject
 */
robj *rdbGenericLoadStringObject(rio *rdb, int encode){
    int isencoded;
    uint32_t len;
    sds val;

    len = rdbLoadLen(rdb, &isencoded);

    //If it is an special enocde EDIS_RDB_ENC*
    if(isencoded){
        switch(len){
            case REDIS_RDB_ENC_INT8:
            case REDIS_RDB_ENC_INT16:
            case REDIS_RDB_ENC_INT32:
                return rdbLoadIntegerObject(rdb, len, encode);

            case REDIS_RDB_ENC_LZF:
                return rdbLoadLzfStringObject(rdb);
            default:
                redisPanic("Unknown RDB encoding type");
        }
    }
    if(len == REDIS_RDB_LENERR) return NULL;

    //当来到这里时，说明数据既不是压缩数据，也不是可以容纳在32位空间的数字，那么我们就把他们当作普通字符串对待
    val = sdsnewlen(NULL, len);
    if(len && rioRead(rdb, val, len) == 0){
        sdsfree(val);
        return NULL;
    }
    return createObject(REDIS_STRING, val);
}

robj *rdbLoadStringObject(rio *rdb){
    return rdbGenericLoadStringObject(rdb, 0);
}

robj *rdbLoadEncodedStringObject(rio *rdb){
    return rdbGenericLoadStringObject(rdb, 1);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 *
 * 以字符串形式来保存一个双精度浮点数。
 * 字符串的前面是一个 8 位长的无符号整数值，
 * 它指定了浮点数表示的长度。
 *
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 *
 * 其中， 8 位整数中的以下值用作特殊值，来指示一些特殊情况：
 *
 * 253: not a number
 *      输入不是数
 * 254: + inf
 *      输入为正无穷
 * 255: - inf
 *      输入为负无穷
 */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    //if val is not a number
    if(isnan(val)){
        buf[0] = 253;
        len = 1;
    }else if(!isfinite(val)){
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    }else{
/* 这里补充一下这里补充一下DBL_MANT_DIG 这个符号是啥？
 * 当我们要进行 double --> text represetation -->double 
 * 这里补充一下DBL_MANT_DIG 就是用来说明最少DBL_MANT_DIG位内的精度是可以保证的，
 * 即转换完 得到的double 和输入的一样，通常情况下DBL_MANT_DIG是17
 */

#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)        
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb,buf,len);;
}

int rdbLoadDoubleValue(rio *rdb, double *val){
    char buf[256];
    unsigned char len;

    //get the length for the double
    if(rioRead(rdb, &len, 1) == 0) return -1;

    switch(len){
        case 255: *val = R_NegInf; return 0;
        case 254: *val = R_PosInf; return 0;
        case 253: *val = R_Nan; return 0;

        //get the double content
        default:
            if(rioRead(rdb, buf, len) == 0) return -1;
            buf[len] = '\0';
            //%lg means convert to a long double
            sscanf(buf, "%lg", val);
            return 0;
    }
}

int rdbSaveObjectType(rio *rdb, robj *o){
    switch(o->type){
        case REDIS_STRING:
            return rdbSaveType(rdb, REDIS_RDB_TYPE_STRING);
        case REDIS_LIST:
            if(o->encoding == REDIS_ENCODING_ZIPLIST){
                return rdbSaveType(rdb, REDIS_RDB_TYPE_LIST_ZIPLIST);
            }else if(o->encoding == REDIS_ENCODING_LINKEDLIST){
                return rdbSaveType(rdb, REDIS_RDB_TYPE_LIST);
            }else{
                redisPanic("Unknown list encoding");
            }
        case REDIS_SET:
            if(o->encoding == REDIS_ENCODING_INTSET){
                return rdbSaveType(rdb, REDIS_RDB_TYPE_SET_INTSET);
            }else if(o->encoding == REDIS_ENCODING_HT){
                return rdbSaveType(rdb, REDIS_RDB_TYPE_SET);
            }else{
                 redisPanic("Unknown set encoding");
            }
        case REDIS_ZSET:
            if (o->encoding == REDIS_ENCODING_ZIPLIST)
                return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET_ZIPLIST);
            else if (o->encoding == REDIS_ENCODING_SKIPLIST)
                return rdbSaveType(rdb,REDIS_RDB_TYPE_ZSET);
            else
                redisPanic("Unknown sorted set encoding");

        case REDIS_HASH:
            if (o->encoding == REDIS_ENCODING_ZIPLIST)
                return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH_ZIPLIST);
            else if (o->encoding == REDIS_ENCODING_HT)
                return rdbSaveType(rdb,REDIS_RDB_TYPE_HASH);
            else
                redisPanic("Unknown hash encoding");

        default:
            redisPanic("Unknown object type");
        }

    return -1; /* avoid warning */
}

/*
 * Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the type is not specifically
 * a vaild Object type.
 */
int rdbLoadObjectType(rio *rdb){
    int type;
    if((type = rdbLoadType(rdb)) == -1) return -1;
    if(!rdbIsObjectType(type)) return -1;
    return type;
}

 /* Put the given object into the rdb.
 *  Returns the number of bytes needed to save this object, if failed return 0;
 */
int rdbSaveObject(rio *rdb, robj *o){
    int n, nwritten = 0;

    //Save the string object
    if(o->type == REDIS_STRING){
        if((n = rdbSaveStringObject(rdb, o)) == -1) return -1;
        nwritten += n;
    }else if(o->type == REDIS_LIST){
        if(o->encoding == REDIS_ENCODING_ZIPLIST){
            size_t len = ziplistBlobLen((unsigned char *)o->ptr);

            //Save the whole ziplist as a string
            if((n = rdbSaveRawString(rdb, o->ptr, len)) == -1) return -1;
            nwritten += n;

        }else if(o->encoding == REDIS_ENCODING_LINKEDLIST){
            list *list = o->ptr;
            listIter li;
            listNode *ln;            

            //The format of rdb to save a list is
            //| len | ele1 | ele2 | , so we need to save the len first.
            if((n = rdbSaveLen(rdb, listLength(list))) == -1) return -1;
                nwritten += n;
            listRewind(list, &li);
            while((ln = listNext(&li))){
                robj *eleobj = listNodeValue(ln);
                //以字符串形式来保存 列表项
                if((n = rdbSaveStringObject(rdb, eleobj)) == -1) return -1;
                nwritten += n;
            }
        }else{
            redisPainc("Unknown list encoding");
        }
    }else if(o->type == REDIS_SET){
        if(o->encoding == REDIS_ENCODING_HT){
            dict *d = o->ptr;
            dictEntry *de;
            dictIterator *di = dictGetIterator(d);

            if((n = rdbSaveLen(rdb, dictSize(d))) == -1) return -1;
                nwritten += n;
            /* 对于这里，强调一点细节之前没注意，当我们对一个处于rehasing状态的
             * dictionary进行遍历的时候，遍历完第一张表之后会遍历第二张表，
             * 这个需要注意。
             */
            while((de = dictNext(di))){
                robj *eleobj = dictGetKey(de);
                //Save the element as the string object
                if((n = rdbSaveStringObject(rdb, eleobj)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);

        }else if(o->encoding == REDIS_ENCODING_INTSET){
            /* 针对这种情况的时候有一个细节需要注意，intsetBlobLen返回的字节数为
             * intset结构的字节数(8字节) + intset的内容所占的字节数
             */
            size_t len = intsetBlobLen(o->ptr);
            /* 还要注意的是，intset持久化的时候intset的信息也会一同持久化
             * intset的信息是指 encoding(uint32_t) 和 length(uint32_t)
             */
            if((n = rdbSaveRawString(rdb, o->ptr,len)) == -1) return -1;
                nwritten += n;
        }else{
            redisPainc("Unknown set encoding");
        }
    }else if(o->type == REDIS_ZSET){
        if(o->encoding == REDIS_ENCODING_ZIPLIST){
            size_t len = ziplistBlobLen(o->ptr);
            if((n = rdbSaveRawString(rdb, o->ptr, len)) == -1) return -1;
            nwritten += n;

        }else if(o->encoding == REDIS_ENCODING_SKIPLIST){
            zset *zs = o->ptr;
            dict *d = zs->dict;
            dictEntry *de;
            dictIterator *di = dictGetIterator(d);

            if((n = rdbSaveLen(rdb, dictSize(d))) == -1) return -1;
            nwritten += n;
            /* 下来的操作我们主要使用遍历dictionary的方式，将整个ZSET加入到rdb中
             * ，这里可能会有疑问，如果通过遍历字典的方式，将整个ZSET加入rdb中，如何保证
             * 在RDB中他的有序，事实上，他并不需要保证其在RDB文件中时是有序的，当我们还原这个
             * 对象的时候我们都是通过 将数据一项一项的插入到有序集合中，在插入的过程中，会保证其有序性
             */
            while((de = dictNext(di))){
                robj *eleobj = dictGetKey(de);
                double *score = dictGetVal(de);

                //Save the object as string
                if((n = rdbSaveStringObject(rdb, eleobj)) == -1) return -1;
                nwritten += n;

                //Convert score into  a string
                if((n = rdbSaveDoubleValue(rdb, *score)) == -1) return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);

        }else{
            redisPainc("Unknown zset encoding");
        }
    }else if(o->type == REDIS_HASH){
        if(o->encoding == REDIS_ENCODING_ZIPLIST){
            size_t len = ziplistBlobLen(o->ptr);
            if((n = rdbSaveRawString(rdb, o->ptr, len)) == -1) return -1;
            nwritten += n;
        }else if(o->encoding == REDIS_ENCODING_HT){
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if((n = rdbSaveLen(rdb, dictSize((dict*)o->ptr))) == -1) return -1;
            nwritten += n;

            while((de = dictNext(di))){
                robj *key = dictGetKey(de);
                robj *val = dictGetVal(de);

                //Save the key and value both as the string
                if((n = rdbSaveStringObject(rdb, key)) == -1) return -1;
                nwritten += n;
                if((n = rdbSaveStringObject(rdb, val)) == -1) return -1;
                nwritten += n;
            } 
            dictReleaseIterator(di);
        }else{
            redisPainc("Unknow hash encoding");
        }
    }else{
        redisPanic("Unknown object type");    
    }
    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. */
// 未使用，可能已经废弃
off_t rdbSavedObjectLen(robj *o) {
    int len = rdbSaveObject(NULL,o);
    redisAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* Save a key value pair, with expire time, type, key ,value.
 * If error occurs return -1
 * On success if te key was actually saved 1 is returned, otherwise 0 is
 * returned(the key was expired).
 */
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val,
                        long long expiretime, long long now){
    /* Save the expire time */
    if(expiretime != -1){
        //对存在过期时间的键进行检查
        if(expiretime < now) return 0;

        if(rdbSaveType(rdb, REDIS_RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if(rdbSaveMillisecondTime(rdb, expiretime) == -1) return -1;
    }
    /*After save the expire time, we go on to save the type, key, value */
    if(rdbSaveObjectType(rdb, val) == -1) return -1;
    if(rdbSaveStringObject(rdb, key) == -1) return -1;
    if(rdbSaveObject(rdb, val) == -1) return -1;
    return 1;
}   

/* Save the DB on disk. Return REDIS_ERR on error, REDIS_OK on success 
 *
 * 将数据库保存到磁盘上。
 *
 * 保存成功返回 REDIS_OK ，出错/失败返回 REDIS_ERR 。
 */
int rdbSave(char *filename){
    dictIterator *di = NULL;
    dictEntry *de;
    char tmpfile[256];
    char magic[10];
    int j;
    long long now = mstime();
    FILE *fp;
    rio rdb;
    uint64_t cksum;

    //Create a tmp file, format the file name.
    snprintf(tmpfile, 256, "temp-%d.rdb", (int)getpid());
    fp = fopen(tmpfile, "w");
    if(!fp){
        redisLog(REDIS_WARNING, "Failed opening .rdb for saving: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    //Init this I/O
    rioInitWithFile(&rdb, fp);

    //If there is the checksum function set it.
    if(server.rdb_checksum)
        rdb.update_cksum = rioGenericUpdateChecksum;
    
    //Write the `Redis` and redis version
    //%04 means that padding the number with zeros if it less than 1000
    snprintf(magic, sizeof(magic), "REDIS%04d", REDIS_RDB_VERSION);
    if(rdbWriteRaw(&rdb, magic, 9) == -1) goto werr;

    //Loop through the whole database
    for(j = 0; j < server.dbnum; j++){
        //Choose the database, recall that default we have 16 database.
        redisDb *db = server.db + j;

        //Point to the database key-value space
        dict *d = db->dict;

        //We skip the empty database
        if(dictSize(d) == 0) continue;

        //Create a dictionary iterator for current key-value space dictionary
        //Notice this is a safe version iterator
        di = dictGetSafeIterator(d); 
        if(!di){
            fclose(fp);
            return REDIS_ERR;
        }

        /**
         * Write the SELECT DB opcode
         */
        if(rdbSaveType(&rdb, REDIS_RDB_OPCODE_SELECTDB) == -1) goto werr;
        if(rdbSaveLen(&rdb, j) == -1) goto werr;

        //Iterate the whole key-value space to store each key-vale into the rdb file
        while((de = dictNext(di))){
            /*  在键空间里面添加 key-value时，key是sds的ptr
             *  sds copy = sdsdup(key->ptr);
             *  int retval = dictAdd(db->dict, copy, val);
             */

            sds keystr = dictGetKey(de);
            robj key, *o = dictGetVal(de);
            long long expire;

            // 根据 keystr ，在栈中创建一个 key 对象
            // 这个地方很有意思，由于我们的键空间的key是一个sds，但是我们编写
            // 的方法都是基于robj 的， 那么需要将sds包裹在一个对象里面，但是
            // 如果我们malloc申请堆内存，那还涉及到要释放，释放，而且这个地方每次
            // 都需要申请，那么频繁调用系统调用的开销就不可忽视了，这个地方我们使用
            // 在当前栈空间新建一个robj 的key对象
            initStaticStringObject(key,keystr);

            //Get the expire time
            expire = getExpire(db, &key);

            // 保存键值对数据
            if (rdbSaveKeyValuePair(&rdb,&key,o,expire,now) == -1) goto werr;
        }
        dictReleaseIterator(di);
    }
    
    di = NULL; /* So that we don't release it again on error. */
    /**Write the EOF code */
    if(rdbSaveType(&rdb, REDIS_RDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. 
     *
     * CRC64 校验和。
     *
     * 如果校验和功能已关闭，那么 rdb.cksum 将为 0 ，
     * 在这种情况下， RDB 载入时会跳过校验和检查。
     */
    cksum = rdb.cksum;
    memrev64ifbe(&cksum);
    rioWrite(&rdb,&cksum,8);

    /* Make sure data will not remaing on the OS's ouput buffer*/ 
    if(fflush(fp) == EOF) goto werr;
    if(fsync(fileno(fp)) == -1) goto werr;
    if(fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. 
     *
     * 使用 RENAME ，原子性地对临时文件进行改名，覆盖原来的 RDB 文件。
     */
    if (rename(tmpfile,filename) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return REDIS_ERR;
    }

    // 写入完成，打印日志
    redisLog(REDIS_NOTICE,"DB saved on disk");

    // 清零数据库脏状态
    server.dirty = 0;

    // 记录最后一次完成 SAVE 的时间
    server.lastsave = time(NULL);

    // 记录最后一次执行 SAVE 的状态
    server.lastbgsave_status = REDIS_OK;

    return REDIS_OK;

    werr:
    //close this filedescriptor
    fclose(fp);
    unlink(tmpfile);

    redisLog(REDIS_WARNING,"Write error saving DB on disk: %s", strerror(errno));

    if (di) dictReleaseIterator(di);

    return REDIS_ERR;
}

int rdbSaveBackground(char *filename){
    pid_t childpid;
    long long start;    

    //If current the BGSAVE is executing, then return s REDIS_ERR
    if(server.rdb_child_pid != -1) return REDIS_ERR;

    //Record before the BGSAVE how many times the database have been modified
    server.dirty_before_bgsave = server.dirty;

    //Record the recent moment try to  execute BGSAVE
    server.lastbgsave_try = time(NULL);
    
    //Before we execute the fork, record the time in order to compute the fork() time-consume
    start = ustime();

    /* 关于Linux中父子进程的，通过fork衍生出的子进程，是需要在子进程结束时
     * 由父进程进行回收的，否则就会成为僵尸进程，常见的手段有父进程调用wait||
     * waitpid等待子进程结束，进行回收，或者通过绑定信号处理函数，改写SIGCHILD
     * 信号的行为 这里是通过在server_cron 里面判断
     * 是否在执行BGSAVE，来调用waitpid(-1, status, WNOHANG),如果没有子
     * 进程结束立刻返回，该waitpid不会阻塞。
     */
    if((childpid = fork()) == 0){
        //If we current in the child process
        int retval;

        /* Close the network connection filedescriptor */
        /* 这个细节要好好注意，具体情况可以参照UNIX网络编程里面讲到的
         * 当fork()发生的时候，子进程会在克隆的过程中，会将父进程的描述符
         * 复制一份，如果子进程里面不关闭描述符，那么将会造成，父进程调用
         * close的时候，描述符引用次数减1，但是由于子进程也引用了父进程描述
         * 的描述符，那么此时该描述符的引用次数（至少为1），如果这里父进程是想
         * 关闭描述符，那么将造成描述符无法关闭的情况（直到子进程退出或者调用close
         * 使得描述符引用次数为0）。
         */
        closeListeningSockets(0);

        /* Set the title for the process */
        redisSetProcTitle("redis-rdb-bgsave");

        //Execute the save operation
        retval = rdbSave(filename);

        //Print when we use the `copy-on-write` use how much memory
        if(retval == REDIS_OK){
            size_t private_dirty = zmalloc_get_private_dirty();

            if(private_dirty){
                    redisLog(REDIS_NOTICE,
                    "RDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
        }

        //Sending signal to the parent process
        exitFromChild((retval == REDIS_OK) ? 0 : 1); 
    }else{
        //If we current in the father process
        //Caculate the fork() time
        server.stat_fork_time = ustime()-start;

        //If something wrong during the fork()
        if(childpid == -1){
            server.lastbgsave_status = REDIS_ERR;
            redisLog(REDIS_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return REDIS_ERR;
        }

        //print the BGSAVE log
        redisLog(REDIS_NOTICE, "Background saving started by pid %d", childpid)

        //Record the time BGSAVE start
        server.rdb_save_time_start = time(NULL);

        //Save the child process ID that execute the BGSAVE
        server.rdb_child_pid = childpid;

        //关闭自动Reshash
        /* 这个地方全是细节，我们看到他关闭了Rehash的标志
         * 说明在RDB过程中不允许进行Rehash，那为什么呢？
         * 原因在于 BGSAVE 的RDB备份通过fork的模式来实现的，
         * 子进程和父进程在操作系统层面最初是共享内存的，直到
         * 其中任何一个对共享区域进行了修改，（一般情况下）我们
         * 要保证这个修改只对修改的线程本身可见，（当然特殊情况也可
         * 已是的这种修改对所有共享的进程都可见），在一般情况下会
         * 触发Copy-on-Write，会为修改的部分重新放置在一块新的真实内存
         * 并修改 改动这个部分的进程相关的 虚拟内存分页，使其指向这片区域。
         * 那么问题在于 要变动的数据越多 copyonwrite要复制的东西也越多
         * 这就失去了copyonwrite的意义，这里的策略是说关闭Rehash操作，
         * 这个和Redis rehashing的实现方式有关，他使用两张表来完成rehashing
         * 那么就使得rehashing阶段如果进行rdb，copy-on-write的开销会很大。
         * 所以在开始进行RDB的时候就禁用rehashing，避免在这个过程中过大的开销
         * 对于已经在进行rehash的那些字典，让他们继续。
         */
        updateDictResizePolicy();

        return REDIS_OK;
    }
    return REDIS_OK;
}

/* 移除BGSAVE 所产生的临时文件
 * 该部分只在BGSAVE 被中断的时候使用
 */
void rdbRemoveTempFile(pid_t childpid){
    char tmpfile[256];

    snprintf(tmpfile, 256, "temp-%d.rdb", (int)childpid);
    unlink(tmpfile);
}   

/**
 * 在这里我们再思考一下 Redis在操作intset和ziplist中存在多个字节表示一个变量的
 * 场景(int, long, bytes)对于机器的字节序进行了处理，他统一按照小段字节序进行
 * 表示，这意味着redis如果工作在一个大段字节序的机器上时，他会把这种情况下的
 * 数据转换成小端的表示方式，也就是说所有多字节的变量在redis里面的都是按照小端
 * 字节序来存储的，换言之，因为对于但自己的变量比如char类型，大端小端一样，换句话说
 * 整个redis采用小端字节序来存储数据。
 */

/* Load a Redis object of the specific type from the specific file.
 * 
 * On success a newly allocated object is returned, otherwise NULL.
 */
robj *rdbLoadObject(int rdbType, rio *rdb){
    robj *o, *ele, *dec;
    size_t len;
    unsigned int i;

    //Load the string object
    if(rdbType == REDIS_RDB_TYPE_STRING){
        if((o == rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        /* 这个地方需要使用tryObjectEncoding 的原因在于：
         * 首先我们分析一下rdbLoadEncodedStringObject
         * 这个函数，他会里面调用 rdbGenericLoadStringObject
         * 这个函数，但是！这函数只对数字字符串的情况进行了encode，
         * 对于普通的字符串，不管是解压缩得到的字符串还是普通
         * 字符串都不会进行encode操作，所以这里的tryObjectEncoding
         * 主要是针对非数字字符串。由于在tryObjectEncoding里面存在
         * if(!sdsEncodedObject(o))  这个判断所以不用担心，
         * 我们已经encode过的数字对象再次被encode。
         */
        o = tryObjectEncoding(o);
    /* 之前看代码的时候有一个细节一直都被忽略了，rdbType的类型为
     * REDIS_RDB_TYPE_LIST时，他的当前处理的就是一个rdb文件中双向链表
     * 形态的list，只是说我们会在还原的过程中会去创建两种类型的list在，
     * 不符合ziplist的要求时切换为双线链表，所以这里的代码其实只是处理了
     * rdb文件中encoding为Linkedlist类型的list对象。关于rdb文件中
     * encoding类型为ziplist的文件，在最后面处理的。
     */
    }else if(rdbType == REDIS_RDB_TYPE_LIST){
        //First read the length of the list
        if((len = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;

        /* Use a real list when there are too many entries
         * 这里就是check 一下是否需要对List 底层的encoding
         * 做转换。
         */
        if(len > server.list_max_ziplist_entries){
            o = createListObject;
        }else{
            o = createZiplistObject();
        }

        /* Load every single element of the list */
        while(len--){
            //Load the string from the list
            if((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;

            //Check if the string object is too large which is exceeds the
            //max_ziplist_value
            if(o->encoding == REDIS_ENCODING_ZIPLIST && 
               sdsEncodedObject(ele) &&
               sdslen(ele->ptr) > server.list_max_ziplist_value)
                listTypeConvert(o, REDIS_ENCODING_LINKEDLIST);
            
            //To input the element into the list object according to encoding
            if(o->encoding == REDIS_ENCODING_ZIPLIST){
                dec = getDecodedObject(ele);
                o->ptr = ziplistPush(o->ptr, dec->ptr, sdslen(dec->ptr), REDIS_TAIL);

                decrRefCount(dec);
                decrRefCount(ele);
            }else{
                //Push the new item into the end of the linkedlist
                ele = tryObjectEncoding(ele);
                listAddNodeTail(o->ptr, ele);
            }
        }
    /* 和list一样，最开始看这段代码的时候以为这段代码处理了rdb文件中encoding为
     * HT和intset两种情况的，实际上这里只处理rdb文件中encoding为HT的对象，将其
     * 还原，这里和list一样，也是先根据情况创建不同encoding的set，不行再切换。
     * 回忆一下我们save encoding为HT的set时，我们其实只存储了他的key部分，所以
     * 在下面的代码中我们才可以看到 他一个接一个的取出字符串，这些字符串对应了ht的
     * key值，也对应了set的元素。
     * 
     * 我是通过阅读一个set 的RDB文件的出的结果
     * 
     */
    }else if(rdbType == REDIS_RDB_TYPE_SET){
        //First read the length of the list
        if((len = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;

        //According to the len choose the corresponding object
        if(len > server.set_max_intset_entries){
            o = createSetObject();
            /* 这个地方只能说太细了，
             * 通过判断当前长度是否大于
             * DICT_HT_INITIAL_SIZE
             * 如果大于，在创建的时候就将
             * 字典进行扩容，避免后续操作
             * 过早rehashing带来的性能开销
             */
            if(len > DICT_HT_INITIAL_SIZE){
                dictExpand(o->ptr, len);
            }
        }else{
            o = createIntsetObject();
        }

        while(len--){
            long long llval;

            //Load the element
            if((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            ele = tryObjectEncoding(ele);

            //Add the element into the intset, and in certain time we convert
            //it into the HT
            if(o->encoding == REDIS_ENCODING_INTSET){
                //Check the whole set if there is a element is not a integer
                if(isObjectRepresentableAsLongLong(ele, &llval) == REDIS_OK){
                    o->ptr = intsetAdd(o->ptr, llval, NULL);
                }else{
                    //We need to conver the intset into the dictionary
                    setTypeConvert(o, REDIS_ENCODING_HT);
                    dictExpand(o->ptr, len);
                }
            }

            /* This function will be called when the set just converted
             * to a regular hash table encoded set.
             */
            if(o->encoding == REDIS_ENCODING_HT){
                //Here we do not release the ele, becase it is referenced in the
                //dictionary.
                dictAdd(o->ptr, ele, NULL);
            }else{
                //If this element we put into the intset, then the ele is no longer
                //be reference by anyone, we need to manually release it.
                decrRefCount(ele);
            }
        }
    }else if(rdbType == REDIS_RDB_TYPE_ZSET){
        
        size_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        //Load the number of elements in the sorted-set
        if((zsetlen = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;

        //Create a zset object
        o = createZsetObject();
        zs = o->ptr;

        /* Load every element for the rdb file and put them into the zset object */
        while(zsetlen--){
            robj *ele;
            double score;
            zskiplistNode *zn;

            if((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            /* 总结一下对这里都要使用rdbLoadEncodedStringObject而不使用
             * rdbLoadStringObject 函数读取字符串的原因在于，在对字符串ZSET的
             * element value 存储的过程中使用的方法是saveStringObject，这个
             * 方法会对字符串进行encode和压缩，所以这里rdbLoadEncodedStringObject
             * 来读取字符串是很适合时的，一方面对于encode的字符串这里会进行还原。
             */
            ele = tryObjectEncoding(ele);

            //Get the score
            if(rdbLoadDoubleValue(rdb, &score) == -1) return NULL;

            //We recode the max element length.
            if(sdsEncodedObject(ele) && sdslen(ele->ptr) > maxelelen) 
                maxelelen = sdslen(ele->ptr);
            
            //Then we insert it into the skiplist
            zn = zslInsert(zs->zsl, score, ele);
            dictAdd(zs->dict, ele, &zn->score);

            //Because the element is referenced by both zskilist and dictionary
            incrRefCount(ele);
        }

        if(zsetLength(o) <= server.zset_max_ziplist_entries && 
           maxelelen < server.zset_max_ziplist_value)
                zsetConvert(o, REDIS_ENCODING_ZIPLIST);
    }else if(rdbType == REDIS_RDB_TYPE_HASH){
        //Load the hash Object
        if((len = rdbLoadLen(rdb, NULL)) == REDIS_RDB_LENERR) return NULL;
        
        o = createHashObject();

        if(len > server.hash_max_ziplist_entries){
            //If it exceed the limit of the ziplist, we use the 
            //dictionray implementation.
            dict *d = dictCreate(&hashDictType, NULL);
            o = createObject(REDIS_HASH, d);
            o->encoding = REDIS_ENCODING_HT;
        } else{
            o = createHashObject();
        }

        robj *key, *value;
        int retVal;
        while(len--){
            if(o->encoding == REDIS_ENCODING_ZIPLIST){
                /* Load the raw strings */
                /* 这个地方说一下一些细节问题，为什么它调用的是rdbLoadStringObject
                 * 而不是rdbLoadEncodedStringObject这个方法，好好思考这两个方法
                 * 的差异主要体现在他们处理数字类型的字符串上，当使用rdbLoadEncodedStringObject
                 * 他碰到数字类型的字符串时，他会根据数字值调用createStringObjectFromLongLong
                 * 来创建字符串，如果创建的字符串可以用long类型表示，那么他会用o->ptr这个指针
                 * 容纳这个字符串的值。而rdbLoadStringObject他则是调用 createObject
                 * 原封不动的将数字转化为字符串给我们。
                 */
                if((key = rdbLoadStringObject(rdb)) == NULL) return NULL;
                redisAssert(sdsEncodedObject(key));
                if((value = rdbLoadStringObject(rdb)) == NULL) return NULL;
                redisAssert(sdsEncodedObject(value));

                /* Put the key and value into the ziplist */
                o->ptr = ziplistPush(o->ptr, key->ptr, sdslen(key->ptr), ZIPLIST_TAIL);
                o->ptr = ziplistPush(o->ptr, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);

                /* 如果元素过大，超过限制，我们进行转换 */
                if(sdslen(key->ptr) > server.hash_max_ziplist_value ||
                   sdslen(value->ptr) > server.hash_max_ziplist_value){
                    decrRefCount(key);
                    decrRefCount(value);
                    hashTypeConvert(o, REDIS_ENCODING_HT);
                    break;
                }
                decrRefCount(key);
                decrRefCount(value);
            }else if(o->encoding == REDIS_ENCODING_HT){
                if((key = rdbLoadStringObject(rdb)) == NULL) return NULL;
                redisAssert(sdsEncodedObject(key));
                if((value = rdbLoadStringObject(rdb)) == NULL) return NULL;
                redisAssert(sdsEncodedObject(value));

                key = tryObjectEncoding(key);
                value = tryObjectEncoding(value);

                //Add the pair to the hash table
                retVal = dictAdd(o, key, value);
                redisAssert(retVal == REDIS_OK);
            }else{
                redisPainc("Unknown hastable encoding");
            }
        }
    }else if(rdbType == REDIS_RDB_TYPE_HASH_ZIPLIST ||
             rdbType == REDIS_RDB_TYPE_LIST_ZIPLIST ||
             rdbType == REDIS_RDB_TYPE_SET_INTSET ||
             rdbType == REDIS_RDB_TYPE_ZSET_ZIPLIST ||
             rdbType == REDIS_RDB_TYPE_HASH_ZIPLIST){
        
        robj *aux = rdbLoadStringObject(rdb);
        if(aux == NULL) return NULL;

        o = createStringObject(REDIS_STRING, NULL);
        o->ptr = zmalloc(sdslen(aux->ptr));
        memcpy(o->ptr, aux->ptr, sdslen(aux->ptr));
        decrRefCount(aux);

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. 
         *
         * 根据读取的类型，将值恢复成原来的编码对象。
         *
         * 在创建编码对象的过程中，程序会检查对象的元素长度，
         * 如果长度超过指定值的话，就会将内存编码对象转换成普通数据结构对象。
         */
        switch (rdbType){

        case REDIS_RDB_TYPE_HASH_ZIPMAP:
        /* Convert to ziplist encoded hash. This must be deprecated
        * when loading dumps created by Redis 2.4 gets deprecated. */
            {
                // 创建 ZIPLIST
                unsigned char *zl = ziplistNew();
                unsigned char *zi = zipmapRewind(o->ptr);
                unsigned char *fstr, *vstr;
                unsigned int flen, vlen;
                unsigned int maxlen = 0;

                // 从 2.6 开始， HASH 不再使用 ZIPMAP 来进行编码
                // 所以遇到 ZIPMAP 编码的值时，要将它转换为 ZIPLIST

                // 从字符串中取出 ZIPMAP 的域和值，然后推入到 ZIPLIST 中
                while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                    if (flen > maxlen) maxlen = flen;
                    if (vlen > maxlen) maxlen = vlen;
                    zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                    zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                }
                zfree(o->ptr);

                // 设置类型、编码和值指针
                o->ptr = zl;
                o->type = REDIS_HASH;
                o->encoding = REDIS_ENCODING_ZIPLIST;

                // 是否需要从 ZIPLIST 编码转换为 HT 编码
                if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                    maxlen > server.hash_max_ziplist_value)
                {
                    hashTypeConvert(o, REDIS_ENCODING_HT);
                }
            }
            break;
        
        case REDIS_RDB_TYPE_LIST_ZIPLIST:
            
            o->type = REDIS_LIST;
            o->encoding = REDIS_ENCODING_ZIPLIST;

            //check if we need to convert
            if(ziplistLen(o->ptr) > server.list_max_ziplist_entries)
                listTypeConvert(o, REDIS_ENCODING_LINKEDLIST);
            break;
        
        case REDIS_RDB_TYPE_SET_INTSET:
            
            o->type = REDIS_SET;
            o->encoding = REDIS_ENCODING_INTSET;
            //Check if we need to convert the encoding
            if(intsetLen(o->ptr) > server.set_max_intset_entries)
                setTypeConvert(o, REDIS_ENCODING_HT);
            break;
        
        case REDIS_RDB_TYPE_ZSET_ZIPLIST:
            o->type = REDIS_ZSET;
            o->encoding = REDIS_ENCODING_ZIPLIST;

            // 检查是否需要转换编码
            if (zsetLength(o) > server.zset_max_ziplist_entries)
                zsetConvert(o,REDIS_ENCODING_SKIPLIST);
            break;
        
        case REDIS_RDB_TYPE_HASH_ZIPLIST:
            
            o->type = REDIS_HASH;
            o->encoding = REDIS_ENCODING_ZIPLIST;

            // 检查是否需要转换编码
            if (hashTypeLength(o) > server.hash_max_ziplist_entries)
                hashTypeConvert(o, REDIS_ENCODING_HT);
            break;
        
        default:
            redisPanic("Unknown encoding");
            break;
        }

    }else{
        redisPanic("Unknown redis object type");
    }
    return o;
}

/* Mark that we loading in the global state and setup the fields
 * needed to provide loading stats.
 */
void startLoading(FILE *fp){
    struct stat sb;
    //Set the loading mark
    server.loading = 1;

    server.loading_start_time = time(NULL);

    //The size of the file
    if(fstat(fileno(fp), &sb) == -1){
        server.loading_total_bytes = 1; //Avoid to divide by zero.
    }else{
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading process info */
void loadingProcess(off_t pos){
    server.loading_loaded_bytes = pos;
    if(server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished */
void stopLoading(void){
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
// 记录载入进度信息，以便让客户端进行查询
// 这也会在计算 RDB 校验和时用到。
void rdbLoadProcessCallback(rio *r, const void *buf, size_t len){
    if(server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);

    if(server.loading_process_events_interval_bytes &&
       (r->processed_bytes + len)/server.loading_process_events_interval_bytes > 
        r->processed_bytes/server.loading_process_events_interval_bytes){
         /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();    
    }
}

int rdbLoad(char *filename){
    uint32_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    long long expiretime, now = mstime();
    FILE *fp;
    rio rdb;

    //Open the rdb file
    if((fp = fopen(filename, "r")) == NULL) return REDIS_ERR;

    //Init the input stream
    rioInitWithFile(&rdb, fp);
    rdb.update_cksum = rdbLoadProcessCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    if(rioRead(&rdb, buf, 9) == 0) goto eoferr;
    buf[9] = '\0';

    //Check the redis HEAD
    if(memcpy(buf, "REDIS", 5) != 0){
        fclose(fp);
        redisLog(REDIS_WARNING, "Wrong signature trying to load DB from file");
        errno = EINVAL;
        return REDIS_ERR;
    }

    rdbver = atoi(buf+5);
    if(rdbver < 1 || rdbver > REDIS_RDB_VERSION){
        fclose(fp);
        redisLog(REDIS_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return REDIS_ERR;
    }

    //Start to loading
    startLoading(fp);

    while(1){
        robj *key, *val;
        expiretime = -1;

        /* 这个地方有点歧义，其实他这里的操作就是读取1个字节
         * 并放置在type变量里面，因为如果这个key-value是带有
         * 过期时间的，那么这一个字节会是过期时间的OPCODE，
         * 否则应该是这个keyvalue的类型
         */
        if((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        //Read the expire time value
        if(type == REDIS_RDB_OPCODE_EXPIRETIME){
            //Caculate the time by seconds
            if((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
        
            //这种带有过期时间的，在过期的时间后紧跟的是keyvalue的类型
            if((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. 
             *
             * 将格式转换为毫秒*/
            expiretime *= 1000;
        }else if(type == REDIS_RDB_OPCODE_EXPIRETIME_MS){
            /* Milliseconds precision expire times introduced with RDB
             * version 3. */
            if ((expiretime = rdbLoadMillsecondTime(&rdb)) == -1) goto eoferr;

            /* We read the time so we need to read the object type again */
            if((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        }

        //If we reach the EOF that means we reach the end of rdb
        if(type == REDIS_RDB_OPCODE_EOF) break;

        /* Handle SELECT DB opcode as a special case */
        if(type == REDIS_RDB_OPCODE_SELECTDB){
            //Read the database number
            if((dbid = rdbLoadLen(&rdb, NULL)) == REDIS_RDB_LENERR) goto eoferr;

            //check the correct of the database number
            if(dbid >= (unsigned)server.dbnum){
                redisLog(REDIS_WARNING,"FATAL: Data file was created with a Redis server configured to handle more than %d databases. Exiting\n", server.dbnum);
                exit(1);   
            }

            //change the db as the dbid
            db = server.db + dbid;

            continue;
        }

        /* Read Key */
        if((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;

        /* Read value */
        if((val = rdbLoadObject(type, &rdb)) == NULL) goto eoferr;

        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. 
         *
         * 如果服务器为主节点的话，
         * 那么在键已经过期的时候，不再将它们关联到数据库中去
         */
        if(server.masterhost == NULL && expiretime == -1 && expiretime < now){
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }

        /* Add the new object in the hash table */
        dbAdd(db, key, val);

        /* Set the expire time */
        if(expiretime != -1) setExpire(db, key, expiretime);

        /* 
         * 回忆一下 dict的key他会拷贝当前key的key->ptr
         * 所以当前key对象可以安全删除了
         */
        decrRefCount(key);
    }

    /* Verify the checksum if RDB verion is >= 5 */
    if(rdbver >=5 && server.rdb_checksum){
        uint64_t cksum, expected = rdb.cksum;

        //Read the file checksum
        if(rioRead(&rdb, &cksum, 8) == 0) goto eoferr;
        memrev64ifbe(&cksum);

        //Check the ckecksum
        if(cksum == 0){
            redisLog(REDIS_WARNING,"RDB file was saved with checksum disabled: no check performed.");    
        }else if(cksum != expected){
            redisLog(REDIS_WARNING,"Wrong RDB checksum. Aborting now.");
            exit(1);
        }
    }

    fclose(fp);

    stopLoading();

    return REDIS_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    redisLog(REDIS_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    exit(1);
    return REDIS_ERR; /* Just to avoid warning */
}

/* A background saving child (BGSAVE) terminated its work. Handle this. 
 *
 * 处理 BGSAVE 完成时发送的信号
 */
void backgroundSaveDoneHandler(int exitcode, int bysignal){
    //If the BGSAVE success
    if(!bysignal && exitcode == 0){
        redisLog(REDIS_NOTICE,
        "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = REDIS_OK;
    }else if(!bysignal && exitcode != 0){
        redisLog(REDIS_WARNING, "Background saving error");
        server.lastbgsave_status = REDIS_ERR;
    }else{
        //If the BGSAVE is teriminal by signal
        redisLog(REDIS_WARNING,
        "Background saving terminated by signal %d", bysignal);
        //Remove the tmp rdbFile,when the BGSAVE child process is
        //interrupted by the signal we need do some clean up job.
        rdbRemoveTempFile(server.rdb_child_pid);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        /* 这里是说如果中断子进程的信号是SIGUSR1，那么在父进程中不会将上一次
         * rdb的结果置为REDIS_ERR，对曰SIGUSR1他是一个自定义信号类型，
         * 可以由用户决定在什么场合触发，系统不会发送这个类型的信号。
         */
        if(bysignal != SIGUSR1)
            server.lastbgsave_status = REDIS_ERR;
    }

    //Update the server status
    server.rdb_child_pid = -1;
    server.rdb_save_time_last = time(NULL) - serve.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    // 处理正在等待 BGSAVE 完成的那些 slave
    updateSlavesWaitingBgsave(exitcode == 0 ? REDIS_OK : REDIS_ERR);
}

void saveCommand(redisClient *c){
    //BGSAVE is being executed, if we execute it again, it triggers a error
    if(server.rdb_child_pid != -1){
        addReplyError(c,"Background save already in progress");
        return;
    }

    //Execute
    if(rdbSave(server.rdb_filename) == REDIS_OK){
        addReply(c, shared.ok);
    }else{
        addReply(c, shared.err);
    }
}

void bgsaveCommand(redisClient *c) {

    // 不能重复执行 BGSAVE
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");

    // 不能在 BGREWRITEAOF 正在运行时执行
    } else if (server.aof_child_pid != -1) {
        addReplyError(c,"Can't BGSAVE while AOF log rewriting is in progress");

    // 执行 BGSAVE
    } else if (rdbSaveBackground(server.rdb_filename) == REDIS_OK) {
        addReplyStatus(c,"Background saving started");

    } else {
        addReply(c,shared.err);
    }
}

