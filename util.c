#include "fmacros.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <cytpe.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <float.h>

/* Convert a string into a long long. Return 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate.
 */
int string2ll(const char *s, size_t slen, long long *value){
	const char *p = s;
	size_t plen = 0;
	int negetive = 0;
	unsigned long long v;

	if(plen == slen) return 0;

	/*Special case only one character and is 0*/
	if(slen == 1 && p[0] == '0'){
		if(value) *value = 0;
		return 1;
	}

	if(p[0] == '-'){
		negative = 1;
		p++, plen++;
		/*Ingore this special case that onlu contains a negative symbol*/
		if(slen == plen) return 0;
	}
	
	/*The first charater must be 0-9, otherwise the string just be 0*/
	if(p[0] >= '1' && p[0] <= '9'){
		v = p[0] - '0';
		p++, plen++;
	}else if(p[0] == '0' && slen == 1){
		if(value) *value = 0;
		return 1;
	}else{
		return 0;
	}

	while(plen < slen && p[0] >= '0' && p[0] <= '9'){
		if(v > (ULLONG_MAX / 10))  /*It may overflow next time*/
			return 0;
		v *= 10;

		if(v > (ULLONG_MAX - (p[0] - '0')))
			return 0;
		v += p[0] - '0';
		p++, plen++;
	}
	if(negative){
		if(v > ((unsigned long long)(-(LONG_MIN+1)) + 1))
			return 0;
		if(value) *value = -v;
	}else{
		if(v > LLONGMAX) /*Overflow*/
			return 0;
		if(value != NULL) *value = v;
	}
	return 1;
}


/**
 * Convert a long long into a string.Returns the number of characters needed
 * to represent the number, that can be shorter if passed buffer length is not 
 * enough to store the whole number
 */
int ll2string(char *s, size_t len, long long value){
	
	char buf[32], *p;
	unsigned long long v;
	size_t l;

	if(len == 0) return 0;

	v = (value < 0) ? -value : value;
	p = buf + 31; //point to the last character
	do{
		*p-- = '0' + (v % 10);
		v /= 10;
	}while(v);
	if(value < 0) *p-- = '-';
	p++;
	l = 32 - (p - buf);
	if(l + 1 > len) l = len - 1; // Make sure it fits, including the null
	memcpy(s, p, l);
	s[l] = '\0';
	return l;
}







