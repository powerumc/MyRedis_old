
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
void myredis_disconnect(MYSQL *mysql);
robj *myredis_query_scalar(redisClient *c, MYSQL *mysql);
void getneCommand(redisClient *c);
int getGenericCommand_no_exire(redisClient *c);
void notifyKeyspaceExpiringEvent(int type, char *event, robj *key, robj *val, int dbid);
int pubsubPublishMessageKeyValue(robj *channel, robj *key, robj *val);


#endif
