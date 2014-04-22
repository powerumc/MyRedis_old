
#ifndef __REDIS_MYREDIS_H
#define __REDIS_MYREDIS_H

#include "../deps/mysql/include/mysql.h"
#include "redis.h"
#include "sds.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>


#define MYREDIS_MYSQL_PORT 3306

typedef struct __myredis_conn {
	robj *host;
	robj *user;
	robj *passwd;
	robj *db;
	robj *port;
} myredis_conn;


MYSQL *myredis_connect(redisClient *c);
MYSQL_RES *myredis_query(redisClient *c, MYSQL *mysql);








#endif
