#include "myredis.h"

char* itoa(int val, int base){

	static char buf[32] = {0};
	int i = 30;

	for(; val && i ; --i, val /= base)
		buf[i] = "0123456789abcdef"[val % base];
	return &buf[i+1];
}

robj *myredis_lookupKeyRead(redisClient *c, robj *key, const char *pfix) {

	char k[strlen((char *)key->ptr) + 1];
	strcpy(k, (char *)key->ptr);
	char *ptr = strcat(k, pfix);

	robj *key_ = createStringObject(ptr, strlen(ptr));
	robj *res = lookupKeyRead(c->db, key_);

	return res;
}

myredis_conn myredis_connect_get(redisClient *c) {

	myredis_conn myredis;
	myredis.host   = myredis_lookupKeyRead(c, c->argv[1], ".host");
	myredis.user   = myredis_lookupKeyRead(c, c->argv[1], ".user");
	myredis.passwd = myredis_lookupKeyRead(c, c->argv[1], ".passwd");
	myredis.db 	   = myredis_lookupKeyRead(c, c->argv[1], ".db");
	myredis.port   = myredis_lookupKeyRead(c, c->argv[1], ".port");

	return myredis;
}

MYSQL *myredis_connect(redisClient *c) {

	myredis_conn conn = myredis_connect_get(c);

	if (!conn.host)   { addReplyError(c, "mysql: host key could not found."); return NULL; }
	if (!conn.user)   { addReplyError(c, "mysql: user key could not found."); return NULL; }
	if (!conn.passwd) { addReplyError(c, "mysql: passwd key could not found."); return NULL; }
	if (!conn.port) {
		char *n = itoa(MYREDIS_MYSQL_PORT, 10);
		conn.port = createStringObject(n, strlen(n));
	}
	if (!conn.db) {
		conn.db = createStringObject("", 0);
	}

	redisLog(REDIS_DEBUG, "mysql: init.");
	MYSQL *mysql = mysql_init(NULL);
	if (!mysql) {
		addReplyError(c, "mysql init failed.");
		return NULL;
	}
	redisLog(REDIS_DEBUG, "mysql: init success.");


	redisLog(REDIS_DEBUG, "mysql: connecting.");
	MYSQL *res = mysql_real_connect(mysql, conn.host->ptr,
			                               conn.user->ptr,
	                                       conn.passwd->ptr,
	                                       conn.db->ptr,
	                                       atoi(conn.port->ptr),
	                                       (char *)NULL,
	                                       0);
	if (!res) {
		addReplyError(c, "mysql connect failed.");
		return NULL;
	}
	redisLog(REDIS_DEBUG, "mysql: connected.");

	return mysql;
}

void myredis_disconnect(MYSQL *mysql) {
	if (mysql) {
		mysql_close(mysql);
	}
}

MYSQL_RES *myredis_query(redisClient *c, MYSQL *mysql) {
	robj *q = lookupKeyRead(c->db, c->argv[2]);
	if (!q) {
		addReplyError(c, "mysql: query key could not found.");
		return NULL;
	}

	int s = mysql_query(mysql, (const char *)q->ptr);
	if (s) {
		addReplyError(c, "mysql: query failed");
		return NULL;
	}

	MYSQL_RES *res = mysql_store_result(mysql);
	if (!res) return NULL;

	int r_len = mysql_num_rows(res);
	int c_len = mysql_num_fields(res);

	addReplyMultiBulkLen(c, r_len*c_len);

	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res))) {
		for(int i=0; i<c_len; i++) {
			addReplyBulkCString(c, row[i]);
		}
	}

	return res;
}
