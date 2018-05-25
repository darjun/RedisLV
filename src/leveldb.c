#include "redis.h"
#include "bio.h"

// 由于leveldb只保存了key-value对，为了区分dbid、type等，key头部增加了三个元信息（dbid、type和key长度），真实的key内容拼接在元信息之后。
// type值有：
// h: hash类型。key结构：|dbid|h|keylen|key=field|，value就是field对应值。keylen是key的长度。
// s: set类型。key结构：|dbid|s|keylen|key=field|，value为NULL。keylen是key的长度。
// z: zset。key结构：|dbid|z|keylen|key=field|，value为分数以".17g"格式化后的字符串。keylen是key的长度。
// c: string。key结构：|dbid|c|keylen|key|，value就是key对应字符串，keylen是key的长度。
// f: freezed，冻结的key。key结构：|dbid|f|keylen|key|，value是冻结的key类型（上面四种类型），长度为1。keylen是key的长度。
// 注意：redislv实现不支持list类型

// dbid索引
#define LEVELDB_KEY_FLAG_DATABASE_ID 0
// type索引
#define LEVELDB_KEY_FLAG_TYPE 1 
// 长度索引
#define LEVELDB_KEY_FLAG_SET_KEY_LEN 2
// 元信息数量
#define LEVELDB_KEY_FLAG_SET_KEY 3

// 错误处理
// err是通过leveldb接口参数返回，或者在操作结束后调用leveldb_get_last_error获得，需要调用leveldb_free释放
void procLeveldbError(char* err, const char* fmt) {
  if (err != NULL) {
    redisLog(REDIS_WARNING, "%s: %s", fmt, err);
    // 释放err
    leveldb_free(err);
    exit(1);
  }
}

// 初始化leveldb
void initleveldb(struct leveldb* ldb, char *path) {
  // 创建leveldb选项
  ldb->options = leveldb_options_create();
  // 设置选项
  // 1.文件不存在时创建
  // 2.设置压缩
  // 3.设置写缓冲大小64M
  // 4.设置最大打开文件数为500
  leveldb_options_set_create_if_missing(ldb->options, 1);
  leveldb_options_set_compression(ldb->options, 1);
  leveldb_options_set_write_buffer_size(ldb->options, 64 * 1024 * 1024);
  leveldb_options_set_max_open_files(ldb->options, 500);

  char *err = NULL;
  // 打开leveldb
  ldb->db = leveldb_open(ldb->options, path, &err);

  // 处理错误
  procLeveldbError(err, "open leveldb error");

  // 创建leveldb读选项
  ldb->roptions = leveldb_readoptions_create();
  // 迭代时数据不缓存在内存中
  leveldb_readoptions_set_fill_cache(ldb->roptions, 0);

  // 创建leveldb写选项
  ldb->woptions = leveldb_writeoptions_create();
  // 关闭文件同步选项。开启时只有写入文件后才算操作成功，非常影响效率
  leveldb_writeoptions_set_sync(ldb->woptions, 0);
}

// 向freezed中添加一个冻结的key，值为类型
int addFreezedKey(int dbid, sds key, char keytype) {
    dictEntry *entry = dictAddRaw(server.db[dbid].freezed,key);

    if (!entry) return REDIS_ERR;
    dictSetSignedIntegerVal(entry, keytype);
    return REDIS_OK;
}

// 加载冻结的key
int loadFreezedKey(struct leveldb* ldb) {
    int success = REDIS_OK;
    int dbid = 0;
    unsigned long len = 0;
    char *data = NULL;
    char *value = NULL;
    size_t dataLen = 0;
    size_t valueLen = 0;
    sds strkey;
    int retval;
    // 创建leveldb的迭代器
    leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
    char tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN];

    tmp[LEVELDB_KEY_FLAG_TYPE] = 'f';
    // 加载每个db中的冻结key
    for(dbid = 0; dbid < server.dbnum; dbid++) {
        tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
        
        // 使用leveldb提供的迭代器遍历，leveldb中key都是排好序的。
        // 而且每个key头部都是以|dbid|type|开始，所以遍历到dbid或类型不一致的key就可以break了。
        for(leveldb_iter_seek(iterator, tmp, LEVELDB_KEY_FLAG_SET_KEY_LEN); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
            // 取出key
            data = (char*) leveldb_iter_key(iterator, &dataLen);
            // 如果dbid或type不一致，遍历结束
            if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid || data[LEVELDB_KEY_FLAG_TYPE] != 'f') break;

            // 取出value，value是冻结的key的类型，长度为1
            value = (char*) leveldb_iter_value(iterator, &valueLen);
            if(valueLen != 1) continue;
            
            // 真实的key在元信息后面，取出真实key，构造sds，添加到freezed中
            len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
            strkey = sdsnewlen(data+LEVELDB_KEY_FLAG_SET_KEY,len);
            retval = addFreezedKey(dbid, strkey, value[0]);
            redisAssertWithInfo(NULL,NULL,retval == REDIS_OK);
        }
    }
    
    // 错误处理
    char *err = NULL;
    leveldb_iter_get_error(iterator, &err);
    if(err != NULL) {
        redisLog(REDIS_WARNING, "load freezedkey iterator err: %s", err);
        leveldb_free(err);
        success = REDIS_ERR;
        err = NULL;
    }

    // 遍历结束，删除迭代器
    leveldb_iter_destroy(iterator);
    return success;
}

// 判断一个db中的key是否被冻结
int isKeyFreezed(int dbid, robj *key) {
    dictEntry *de = dictFind(server.db[dbid].freezed,key->ptr);
	return de ? 1 : 0;
}

// 创建冻结key的元信息，将真实的key放在元信息之后
sds createleveldbFreezedKeyHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  // 元信息 dbid | type | keylen
  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'f';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  // 创建包含元信息的sds
  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  // 真实的key拼接到元信息后面
  key = sdscatsds(key, name);
  return key;
}

// 冻结一个key
int freezeKey(redisDb *db, struct leveldb *ldb, robj *key, char keytype) {
    sds strkey;
    sds leveldbkey;
    int retval;
    char *err = NULL;

    // 冻结的key一定要存在，需要将这个key从db中删除
    if (!dbDelete(db, key)) {
	    return REDIS_ERR;
    }

    // 创建包含冻结key的头部
    leveldbkey = createleveldbFreezedKeyHead(db->id, key->ptr);
    // 将key和与之对应的type存入leveldb
    leveldb_put(ldb->db, ldb->woptions, leveldbkey, sdslen(leveldbkey), &keytype, 1, &err);
    if (err != NULL) {
        redisLog(REDIS_WARNING, "freezekey err: %s", err);
        // 释放err
        leveldb_free(err);
        // 释放头部key
        sdsfree(leveldbkey);
        err = NULL;
        return REDIS_ERR;
    }
    // leveldb操作计数
    server.leveldb_op_num++;
    // 释放头部key
    sdsfree(leveldbkey);

    // 复制冻结key存入db的freezed
    strkey = sdsdup(key->ptr);
    retval = addFreezedKey(db->id, strkey, keytype);
    redisAssertWithInfo(NULL,key,retval == REDIS_OK);
    if(retval != REDIS_OK)
    {
        // 存入freezed失败，将复制得到的key释放
        sdsfree(strkey);
        return REDIS_ERR;
    }
    
    return REDIS_OK;
}

// redis-server启动时从leveldb中加载数据，创建一个fakeClient模拟客户端向redis中发送命令设置数据
int callCommandForleveldb(struct redisClient *fakeClient, char *data, size_t dataLen, leveldb_iterator_t *iterator) {
    char *value = NULL;
    size_t valueLen = 0;
    int argc;
    unsigned long len;
    robj **argv;
    struct redisCommand *cmd;
    int tmptype;

    tmptype = data[LEVELDB_KEY_FLAG_TYPE];
    // 根据命令类型构造参数
    if(tmptype == 'h'){
        // h表示hash
        argc = 4;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("hset",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        // hash表结构： key : { field1: value1, field2 : value2, ...}
        // hash设置命令格式: hset key field value
        // 在leveldb中的hash结构： |dbid|h|keylen|key=field| -> value
        // 注意多了一个=，长度计算上的变化
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
        // 取出field对应的value
        value = (char*) leveldb_iter_value(iterator, &valueLen);
        argv[3] = createStringObject(value, valueLen);
    }else if(tmptype == 's'){
        // s表示set
        argc = 3;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("sadd",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        // set结构： key : { field1, field2, ... }
        // set添加命令格式 sadd key field
        // leveldb中的set结构： |dbid|s|keylen|key=field| -> NULL
        // 注意多了一个=，长度计算的变化
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[2] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
    }else if(tmptype == 'z'){
        // z表示zset
        argc = 4;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("zadd",4);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        // zset结构： key : { field1: score1, field2: score2, ... } 以score顺序排列的
        // zset设置命令格式： zadd key score field
        // zset在leveldb中的结构： |dbid|z|keylen|key=field| -> score
        // 注意多了一个=，长度计算的变化
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        argv[3] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY+len+1,dataLen-LEVELDB_KEY_FLAG_SET_KEY-len-1);
        value = (char*) leveldb_iter_value(iterator, &valueLen);
        argv[2] = createStringObject(value, valueLen);
    }else if(tmptype == 'c'){
        // c表示普通字符串
        argc = 3;
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;
        argv[0] = createStringObject("set",3);
        len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
        // string结构： key : value
        // string设置命令格式： set key value
        // string在leveldb中的结构： |dbid|c|keylen|key| -> value
        argv[1] = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
        value = (char*) leveldb_iter_value(iterator, &valueLen);
        argv[2] = createStringObject(value, valueLen);
    }else{
        redisLog(REDIS_WARNING,"callCommandForLeveldb no found type: %d %d", fakeClient->db->id, tmptype);
        // 类型错误释放释放client结构中的命令参数
        freeFakeClientArgv(fakeClient);
        return REDIS_ERR;
    }

    // 查找命令，执行
    cmd = lookupCommand(argv[0]->ptr);
    if (!cmd) {
        redisLog(REDIS_WARNING,"Unknown command '%s' from leveldb", (char*)argv[0]->ptr);
        exit(1);
    }
    cmd->proc(fakeClient);

    /* The fake client should not have a reply */
    redisAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    // 释放释放client结构中的命令参数
    freeFakeClientArgv(fakeClient);
    
    return REDIS_OK;
}

// 解冻key
int meltKey(int dbid, struct leveldb *ldb, robj *key, char keytype) {
    int success = 1;
    char *data = NULL;
    size_t dataLen = 0;
    size_t keylen = sdslen(key->ptr);
    sds leveldbkey;
    sds sdskey;
    char *err = NULL;
    struct redisClient *fakeClient = ldb->fakeClient;

    // 选择指定的db
    if(selectDb(fakeClient,dbid) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "meltKey select db error: %d", dbid);
        return REDIS_ERR;
    }

    // 创建冻结key的头部
    leveldbkey = createleveldbFreezedKeyHead(dbid, key->ptr);
    // 删除leveldb中的冻结key
    leveldb_delete(ldb->db, ldb->woptions, leveldbkey, sdslen(leveldbkey), &err);
    if (err != NULL) {
        redisLog(REDIS_WARNING, "meltKey leveldb err: %s", err);
        // 释放err
        leveldb_free(err); 
        // 释放头部
        sdsfree(leveldbkey);
        err = NULL;
        return REDIS_ERR;
    }
    // 释放头部
    sdsfree(leveldbkey);
    // leveldb操作计数
    server.leveldb_op_num++;
    
    // 删除db中的freezed对应key
    dictDelete(server.db[dbid].freezed,key->ptr);

    char tmp[LEVELDB_KEY_FLAG_SET_KEY];
    tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
    tmp[LEVELDB_KEY_FLAG_TYPE] = keytype;
    tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = keylen;
    sdskey = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);
    sdskey = sdscatsds(sdskey, key->ptr);

    // 解冻之后将leveldb文件中的该key重新加载进redis
    leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
    for(leveldb_iter_seek(iterator, sdskey, sdslen(sdskey)); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
        data = (char*) leveldb_iter_key(iterator, &dataLen);
        // 指定下一个key不是解冻的key
        // 1.key长度不同
        // 2.dbid不一致
        // 3.key不同
        if(data[LEVELDB_KEY_FLAG_SET_KEY_LEN] != (char)keylen) break;
        if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
        if(memcmp(key->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, keylen) != 0) break;
        
        // 使用callCommandForleveldb将数据载入redis
        if (callCommandForleveldb(fakeClient, data, dataLen, iterator) == REDIS_OK) {
            server.dirty++;
        } else {
            success = 0;
            break;
        }
    }
    
    // 释放解冻key头部
    sdsfree(sdskey);
    
    // 错误处理
    leveldb_iter_get_error(iterator, &err);
    procLeveldbError(err, "meltKey iterator err");

    // 销毁迭代器
    leveldb_iter_destroy(iterator);
    if(success == 1) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

// 加载leveldb数据到redis中
int loadleveldb(char *path) {
  redisLog(REDIS_NOTICE, "load leveldb path: %s", path);

  // 创建一个仿造的客户端，模拟向redis发送指令设置数据
  struct redisClient *fakeClient = createFakeClient();
  int old_leveldb_state = server.leveldb_state;
  long loops = 0;

  // 加载前先关闭leveldb标志，防止加载过程中使用leveldb命令，导致文件损坏
  server.leveldb_state = REDIS_LEVELDB_OFF;
  // 初始化leveldb状态
  initleveldb(&server.ldb, path);
  server.ldb.fakeClient = fakeClient;

  // 先加载冻结key，这样数据中可能存在的冻结key就不会被加载进来了
  if(loadFreezedKey(&server.ldb) == REDIS_ERR) {
    server.leveldb_state = old_leveldb_state;
    return REDIS_ERR;
  }
  
  // 设置开始加载标志，记录时间等状态
  startLoading(NULL);

  int success = 1;
  int dbid = 0;
  char *data = NULL;
  size_t dataLen = 0;
  int tmpdbid;
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  // 执行一次完全遍历
  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    unsigned long len;
    robj *tmpkey;

    // 每处理100w条数据，执行一次网络事件处理，防止网络长时间未响应
    if (!(loops++ % 1000000)) {
      processEventsWhileBlocked();
      redisLog(REDIS_NOTICE, "load leveldb: %lu", loops);
    }
    // 处理每条记录
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    tmpdbid = data[LEVELDB_KEY_FLAG_DATABASE_ID];
    if(tmpdbid != dbid) {
      // 选择对应的db
      if(selectDb(fakeClient,tmpdbid) == REDIS_OK ){
        dbid = tmpdbid;
      }else{
        redisLog(REDIS_WARNING, "load leveldb select db error: %d", tmpdbid);
        success = 0;
        break;
      }
    }

    len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    // 根据实际key（去掉元信息）创建redis object
    tmpkey = createStringObject(data+LEVELDB_KEY_FLAG_SET_KEY,len);
    // 冻结key不加载
    if(isKeyFreezed(dbid, tmpkey) == 1) {
      decrRefCount(tmpkey);
      continue;
    }
    // 这个redis对象只是为了查找冻结key，用完减少引用计数
    decrRefCount(tmpkey);
    
    // 调用callCommandForleveldb模拟客户端向redis发送命令来设置数据
    if (callCommandForleveldb(fakeClient, data, dataLen, iterator) == REDIS_ERR) {
      success = 0;
      break;
    }
  }
  redisLog(REDIS_NOTICE, "load leveldb sum: %lu", loops);

  // 结束加载
  stopLoading();
  // 恢复leveldb状态
  server.leveldb_state = old_leveldb_state;

  char *err = NULL;
  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "load leveldb iterator err: %s", err);
    // 释放err
    leveldb_free(err);
    err = NULL;
    success = 0;
  }

  // 销毁迭代器
  leveldb_iter_destroy(iterator);
  if(success == 1) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

// 释放leveldb相关资源
void closeleveldb(struct leveldb *ldb) {
  // 释放读写选项，选项，关闭文件，释放仿造客户端
  leveldb_writeoptions_destroy(ldb->woptions);
  leveldb_readoptions_destroy(ldb->roptions);
  leveldb_options_destroy(ldb->options);
  leveldb_close(ldb->db);
  freeFakeClient(ldb->fakeClient);
}

// 创建普通字符串的头部 |dbid|c|keylen|key|
sds createleveldbStringHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  // 设置dbid，type和keylen
  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'c';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  // 根据元信息创建sds
  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  // 将name拼接到元信息后，返回
  key = sdscatsds(key, name);
  return key;
}

// 设置普通字符串
void leveldbSet(int dbid, struct leveldb *ldb, robj** argv) {
  leveldbSetDirect(dbid, ldb, argv[1], argv[2]);
}

// 设置普通字符串
void leveldbSetDirect(int dbid, struct leveldb *ldb, robj *argv1, robj *argv2) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为有可能是整形编码
  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  // 创建普通字符串头部，|dbid|c|keylen|key|
  sds key = createleveldbStringHead(dbid, r1->ptr);
  char *err = NULL;

  // 存入leveldb
  leveldb_put(ldb->db, ldb->woptions, key, sdslen(key), r2->ptr, sdslen(r2->ptr), &err);
  // 错误处理
  procLeveldbError(err, "set direct leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完引用计数减少
  decrRefCount(r1);
  decrRefCount(r2);
}

// 删除某个字符串key
void leveldbDelString(int dbid, struct leveldb *ldb, robj* argv) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为有可能是整形编码
  robj *r1 = getDecodedObject(argv);
  // 创建普通字符串头部，增加元信息
  sds sdskey = createleveldbStringHead(dbid, r1->ptr);
  char *err = NULL;

  // 从leveldb中删除
  leveldb_delete(ldb->db, ldb->woptions, sdskey, sdslen(sdskey), &err);
  // 错误处理
  procLeveldbError(err, "leveldbDel leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;
  
  // 释放头部
  sdsfree(sdskey);
  // 用完引用计数减少
  decrRefCount(r1);
}

// 创建hash的头部 |dbid|h|keylen|key=|
sds createleveldbHashHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  // 设置dbid，type和keylen
  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'h';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  // 根据元信息创建sds
  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  // 将key拼接到元信息后面，hash多添加一个=
  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

// 设置hash的field
void leveldbHset(int dbid, struct leveldb *ldb, robj** argv) {
  leveldbHsetDirect(dbid, ldb, argv[1], argv[2], argv[3]);
}

// 设置hash的field,value
void leveldbHsetDirect(int dbid, struct leveldb *ldb, robj *argv1, robj *argv2, robj *argv3) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  robj *r3 = getDecodedObject(argv3);
  // 创建hash头部，|dbid|h|keylen|key=|
  sds key = createleveldbHashHead(dbid, r1->ptr);
  char *err = NULL;

  // 拼接field
  key = sdscatsds(key, r2->ptr);
  // 存入leveldb
  leveldb_put(ldb->db, ldb->woptions, key, sdslen(key), r3->ptr, sdslen(r3->ptr), &err);
  // 错误处理
  procLeveldbError(err, "hset direct leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完解引用
  decrRefCount(r1);
  decrRefCount(r2);
  decrRefCount(r3);
}

// 批量设置hash的field,value
void leveldbHmset(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建hash的头部，|dbid|h|keylen|key=|
  sds key = createleveldbHashHead(dbid, r1->ptr);
  // 创建批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 处理每对field,value
  for (i = 2; i < argc; i += 2) {
    // 返回解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    // 拼接field组成最终的key
    key = sdscatsds(key, rs[j]->ptr);
    // 放入批量操作
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j+1]->ptr, sdslen(rs[j+1]->ptr));
    // key还原，下次循环使用
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  // 执行批量写操作
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 错误处理
  procLeveldbError(err, "hmset leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 参数用完减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 批量删除hash的field
void leveldbHdel(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建hash的头部，|dbid|h|keylen|key=|
  sds key = createleveldbHashHead(dbid, r1->ptr);
  // 创建批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 处理每个field
  for (i = 2; i < argc; i++) {
    // 返回解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    // 拼接field得到最终的key
    key = sdscatsds(key, rs[j]->ptr);
    // 放入批量操作
    leveldb_writebatch_delete(wb, key, sdslen(key));
    // 还原key，下次循环使用
    sdsrange(key, 0, klen - 1);
    j++;
  }
  // 执行批量删除操作
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 处理错误
  procLeveldbError(err, "hdel leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 用完参数减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 清空hash
void leveldbHclear(int dbid, struct leveldb *ldb, robj *argv) {
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是征信编码
  robj *r1 = getDecodedObject(argv);
  // 创建hash的头部，|dbid|h|keylen|key=|
  sds key = createleveldbHashHead(dbid, r1->ptr);
  // 创建迭代器
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  // 遍历hash的所有field
  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    // 直到下个key:value不属于当前hash
    // 1.key长度不等
    // 2.dbid不一致
    // 3.key内容不等（实际是hash的key，不包括field部分）
    // 按照这个顺序比较是因为，keylen和dbid都在元信息中，比较容易获取
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    // 从leveldb中直接删除
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    // 错误处理
    procLeveldbError(err, "hclear leveldb error");
  }
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  procLeveldbError(err, "hclear leveldb iterator error");
  // 销毁迭代器
  leveldb_iter_destroy(iterator);
}

// 创建set的头部，|dbid|s|keylen|key=|
sds createleveldbSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  // 设置dbid，type和keylen
  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 's';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  // 根据元信息创建sds
  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  // 将key拼接到元信息后面，set后面多添加一个=
  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

// 批量向set中添加元素
void leveldbSadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 为开启leveldb直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建set的头部，|dbid|s|keylen|key=|
  sds key = createleveldbSetHead(dbid, r1->ptr);
  // 创建批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 处理每个元素
  for (i = 2; i < argc; i++) {
    // 返回解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    // 拼接field得到最终key
    key = sdscatsds(key, rs[j]->ptr);
    // 添加元素
    leveldb_writebatch_put(wb, key, sdslen(key), NULL, 0);
    // 还原key，下次循环使用
    sdsrange(key, 0, klen - 1);
    j++;
  }
  // 执行批量写操作
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 错误处理
  procLeveldbError(err, "sadd leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 用完参数减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 批量删除set的元素
void leveldbSrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建set的头部，|dbid|s|keylen|key=|
  sds key = createleveldbSetHead(dbid, r1->ptr);
  // 创建批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 处理每个元素
  for (i = 2; i < argc; i++ ) {
    // 返回解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    // 拼接field得到最终的key
    key = sdscatsds(key, rs[j]->ptr);
    // 放入批量操作
    leveldb_writebatch_delete(wb, key, sdslen(key));
    // 还原key，下次循环使用
    sdsrange(key, 0, klen - 1);
    j++;
  }
  // 执行批量操作
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 错误处理
  procLeveldbError(err, "srem leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 用完参数减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 清空set
void leveldbSclear(int dbid, struct leveldb *ldb, robj* argv) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv);
  // 创建set头部，|dbid|s|keylen|key=|
  sds key = createleveldbSetHead(dbid, r1->ptr);
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  // 遍历该set的所有元素
  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    // 直到下个元素不属于当前set
    // 1.key长度不等
    // 2.dbid不一致
    // 3.key不等
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    // 删除
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    // 错误处理
    procLeveldbError(err, "sclear leveldb error");
  }
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  procLeveldbError(err, "sclear leveldb iterator error");
  // 销毁迭代器
  leveldb_iter_destroy(iterator);
}

// 创建zset的头部，|dbid|z|keylen|key=|
sds createleveldbSortedSetHead(int dbid, sds name) {
  char tmp[LEVELDB_KEY_FLAG_SET_KEY];

  // 设置dbid，type和keylen
  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  tmp[LEVELDB_KEY_FLAG_TYPE] = 'z';
  tmp[LEVELDB_KEY_FLAG_SET_KEY_LEN] = sdslen(name);

  // 根据元信息创建sds
  sds key = sdsnewlen(tmp, LEVELDB_KEY_FLAG_SET_KEY);

  // 将key拼接到元信息后面，zset多添加一个=
  key = sdscatsds(key, name);
  key = sdscat(key, "=");
  return key;
}

// 批量插入zset元素
void leveldbZadd(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 返回解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建zset头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  // 创建leveldb批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 依次处理每条记录
  for (i = 2; i < argc; i += 2) {
    // 获取解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    rs[j+1] = getDecodedObject(argv[i+1]);
    // 拼接field得到最好的key
    key = sdscatsds(key, rs[j+1]->ptr);
    // 放入批量操作
    leveldb_writebatch_put(wb, key, sdslen(key), rs[j]->ptr, sdslen(rs[j]->ptr));
    // key还原，下次循环使用
    sdsrange(key, 0, klen - 1);
    j += 2;
  }
  // 执行批量写
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 错误处理
  procLeveldbError(err, "zadd leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 用完参数减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 添加zset一个field,score
void leveldbZaddDirect(int dbid, struct leveldb *ldb, robj* argv1, robj* argv2, double score) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv1);
  robj *r2 = getDecodedObject(argv2);
  // 创建zset头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  // 拼接field，得到最终的key
  key = sdscatsds(key, r2->ptr);
  size_t klen = sdslen(key);
  char *err = NULL;
  char buf[128];
  // score用".17g"格式化
  int len = snprintf(buf,sizeof(buf),"%.17g",score);

  // 写入元素 key=field -> score
  leveldb_put(ldb->db, ldb->woptions, key, klen, buf, len, &err);
  // 错误处理
  procLeveldbError(err, "zadd direct leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  decrRefCount(r2);
}

// 批量删除zset的field
void leveldbZrem(int dbid, struct leveldb *ldb, robj** argv, int argc) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv[1]);
  // 创建zset的头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  // 创建批量写数据结构
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  robj **rs = zmalloc(sizeof(robj*)*(argc - 2));
  size_t klen = sdslen(key);
  int i, j = 0;
  char *err = NULL;

  // 每次处理一条记录
  for (i = 2; i < argc; i++ ) {
    // 获取解码后的redis object，因为可能是整形编码
    rs[j] = getDecodedObject(argv[i]);
    // 拼接field得到最终的key
    key = sdscatsds(key, rs[j]->ptr);
    // 放入批量操作
    leveldb_writebatch_delete(wb, key, sdslen(key));
    // key还原，下次循环使用
    sdsrange(key, 0, klen - 1);
    j++;
  }
  // 执行批量删除
  leveldb_write(ldb->db, ldb->woptions, wb, &err);
  // 错误处理
  procLeveldbError(err, "zrem leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 销毁批量写数据结构
  leveldb_writebatch_destroy(wb);
  // 用完参数减少引用计数
  for(j = 0; j < argc - 2; j++) {
    decrRefCount(rs[j]);
  }
  // 释放参数数组
  zfree(rs);
}

// 以long long类型为field的zset，删除某个field
void leveldbZremByLongLong(int dbid, struct leveldb *ldb, robj *arg, long long vlong) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(arg);
  // 创建zset的头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;
  char buf[64];
  int len;

  // long long转为string，拼接到key后面得到最终的key
  len = ll2string(buf,64,vlong);
  key = sdscatlen(key, buf, len);
  //redisLog(REDIS_NOTICE, "leveldbZremByLongLong %s", key);
  // 删除
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  // 错误处理
  procLeveldbError(err, "zrem by long long leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
}

// 通过c buffer指定删除的field
void leveldbZremByCBuffer(int dbid, struct leveldb *ldb, robj *arg, unsigned char *vstr, unsigned int vlen) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(arg);
  // 创建zset的头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  // 拼接得到最终的key
  key = sdscatlen(key, vstr, vlen);
  //redisLog(REDIS_NOTICE, "leveldbZremByCBuffer %s", key);
  // 删除
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  // 错误处理
  procLeveldbError(err, "zrem by c buffer leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
}

// 通过redis object指定删除的field
void leveldbZremByObject(int dbid, struct leveldb *ldb, robj *arg, robj *field) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(arg);
  robj *r2 = getDecodedObject(field);
  // 创建zset的头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *err = NULL;

  // 拼接key得到最终的key
  key = sdscatsds(key,  r2->ptr);
  //redisLog(REDIS_NOTICE, "leveldbZremByObject %s", key);
  // 删除
  leveldb_delete(ldb->db, ldb->woptions, key, sdslen(key), &err);
  // 错误处理
  procLeveldbError(err, "zrem by object leveldb error");
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  decrRefCount(r2);
}

// 清空zset
void leveldbZclear(int dbid, struct leveldb *ldb, robj* argv) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }

  // 获取解码后的redis object，因为可能是整形编码
  robj *r1 = getDecodedObject(argv);
  // 创建zset的头部，|dbid|z|keylen|key=|
  sds key = createleveldbSortedSetHead(dbid, r1->ptr);
  char *data = NULL;
  size_t dataLen = 0;
  size_t keyLen = sdslen(r1->ptr);
  int cmp;
  size_t klen = sdslen(key);
  char *err = NULL;

  // 创建迭代器
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);
  // 遍历zset的每条记录
  for(leveldb_iter_seek(iterator, key, klen); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    size_t len = data[LEVELDB_KEY_FLAG_SET_KEY_LEN];
    // 直到下个元素不属于当前zset
    // 1.key长度不等
    // 2.dbid不一致
    // 3.key不等
    if(len != keyLen) break;
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    cmp = memcmp(r1->ptr, data + LEVELDB_KEY_FLAG_SET_KEY, len);
    if(cmp != 0) break;
    // 删除
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    // 错误处理
    procLeveldbError(err, "zclear leveldb error");
  }
  // leveldb操作计数
  server.leveldb_op_num++;

  // 释放头部
  sdsfree(key);
  // 用完减少引用计数
  decrRefCount(r1);
  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  procLeveldbError(err, "zclear leveldb iterator error");
  // 销毁迭代器
  leveldb_iter_destroy(iterator);
}

// 删除当前db所有key
void leveldbFlushdb(int dbid, struct leveldb* ldb) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  char tmp[1];
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  // 创建leveldb迭代器
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  tmp[LEVELDB_KEY_FLAG_DATABASE_ID] = dbid;
  // 遍历当前db的所有key
  // 因为所有键都是以|dbid|开头，leveldb中的key又都是排序的，所以同一db的key存放在一起，顺序迭代即可
  for(leveldb_iter_seek(iterator, tmp, 1); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    // 直到下个key不属于当前db
    if(data[LEVELDB_KEY_FLAG_DATABASE_ID] != dbid) break;
    // 删除
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    // 错误处理
    procLeveldbError(err, "flushdb leveldb error");
  }
  // leveldb操作计数
  server.leveldb_op_num++;

  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  procLeveldbError(err, "flushdb leveldb iterator error");
  // 销毁迭代器
  leveldb_iter_destroy(iterator);
}

// 删除所有db的所有key
void leveldbFlushall(struct leveldb* ldb) {
  // 未开启leveldb，直接返回
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    return;
  }
  char *err = NULL;
  char *data = NULL;
  size_t dataLen = 0;
  // 创建迭代器
  leveldb_iterator_t *iterator = leveldb_create_iterator(ldb->db, ldb->roptions);

  // 遍历所有key
  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    // 删除
    leveldb_delete(ldb->db, ldb->woptions, data, dataLen, &err);
    // 处理错误
    procLeveldbError(err, "flushall leveldb error");
  }
  // level操作计数
  server.leveldb_op_num++;

  // 错误处理
  leveldb_iter_get_error(iterator, &err);
  procLeveldbError(err, "flushall leveldb iterator error");
  // 销毁迭代器
  leveldb_iter_destroy(iterator);
}

// 将redis所有内容备份到leveldb，arg为指定路径
void backupleveldb(void *arg) {
  char *path = (char *)arg;
  time_t backup_start = time(NULL);
  // 创建选项
  leveldb_options_t *options = leveldb_options_create();
  int success = 0;

  // 设置leveldb选项
  // 1.文件不存在时创建
  // 2.文件存在时出错
  // 3.开启压缩
  // 4.设置写缓冲大小为64M
  leveldb_options_set_create_if_missing(options, 1);
  leveldb_options_set_error_if_exists(options, 1);
  leveldb_options_set_compression(options, 1);
  leveldb_options_set_write_buffer_size(options, 64 * 1024 * 1024);

  char *err = NULL;
  // 打开leveldb
  leveldb_t *db = leveldb_open(options, path, &err);

  if (err != NULL) {
    redisLog(REDIS_WARNING, "open leveldb err: %s", err);
    // 释放err
    leveldb_free(err); 
    err = NULL;
    goto cleanup;
  }

  char *data = NULL;
  size_t dataLen = 0;
  char *value = NULL;
  size_t valueLen = 0;
  int i = 0;
  // 创建批量写类型
  leveldb_writebatch_t* wb = leveldb_writebatch_create();
  // 创建写选项
  leveldb_writeoptions_t *woptions = leveldb_writeoptions_create();
  // 生成迭代器
  leveldb_iterator_t *iterator = leveldb_create_iterator(server.ldb.db, server.ldb.roptions);

  // 设置非同步写
  leveldb_writeoptions_set_sync(woptions, 0);
  // 遍历所有的key
  for(leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
    // 取出key和value
    data = (char*) leveldb_iter_key(iterator, &dataLen);
    value = (char*) leveldb_iter_value(iterator, &valueLen);
    // 放入批量操作
    leveldb_writebatch_put(wb, data, dataLen, value, valueLen);
    i++;
    // 每1000个key写一次
    if(i == 1000) {
      i = 0;
      // 执行这个批量写操作
      leveldb_write(db, woptions, wb, &err);
      if (err != NULL) {
        redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
        // 释放err
        leveldb_free(err); 
        err = NULL;
        goto closehandler;
      }
      // 创建一个新的批量写类型供接下来的1000个key写
      leveldb_writebatch_destroy(wb);
      wb = leveldb_writebatch_create();
    }
  }
  // 执行最后一个批量写操作
  leveldb_write(db, woptions, wb, &err);
  if (err != NULL) {
    redisLog(REDIS_WARNING, "backup write leveldb err: %s", err);
    // 释放err
    leveldb_free(err); 
    err = NULL;
    goto closehandler;
  }
  // 释放批量写类型
  leveldb_writebatch_destroy(wb);
  leveldb_iter_get_error(iterator, &err);
  if(err != NULL) {
    redisLog(REDIS_WARNING, "backup leveldb iterator err: %s", err);
    // 释放err
    leveldb_free(err);
    err = NULL;
    goto closehandler;
  }
  success = 1;
closehandler:
  // 释放迭代器，写选项，leveldb结构
  leveldb_iter_destroy(iterator);
  leveldb_writeoptions_destroy(woptions);
  leveldb_close(db);
cleanup:
  // 释放选项结构
  leveldb_options_destroy(options);
  if( success == 1) {
    // 写成功后写入日志文件
    time_t backup_end = time(NULL);
    char info[1024];
    char tmpfile[512];
    char backupfile[512];
    snprintf(tmpfile,512,"%s/temp.log", path);
    snprintf(backupfile,512,"%s/BACKUP.log", path);
    FILE *fp = fopen(tmpfile,"w");

    if (!fp) {
      redisLog(REDIS_WARNING, "Failed opening .log for saving: %s",
          strerror(errno));
    }else{
      int infolen = snprintf(info, sizeof(info), "BACKUP\n\tSTART:\t%jd\n\tEND:\t\t%jd\n\tCOST:\t\t%jd\nSUCCESS", backup_start, backup_end, backup_end-backup_start);

      // 先写入临时文件，成功后重命名为目标文件，并删除临时文件
      fwrite(info, infolen, 1, fp);
      fclose(fp);
      if (rename(tmpfile,backupfile) == -1) {
        redisLog(REDIS_WARNING,"Error moving temp backup file on the final destination: %s", strerror(errno));
      }
      unlink(tmpfile);
      redisLog(REDIS_NOTICE, "backup leveldb path: %s", path);
    }
  }
  zfree(path);
}

// 执行备份命令
void backupCommand(redisClient *c) {
  // 未开启leveldb，返回错误
  if(server.leveldb_state == REDIS_LEVELDB_OFF) {
    addReplyError(c,"leveldb off");
    return;
  }

  size_t len = sdslen(c->argv[1]->ptr);
  char *path = zmalloc(len + 1);
  memcpy(path, c->argv[1]->ptr, len);
  path[len] = '\0';
  // 备份耗时较长，需要在后台执行。创建后台任务执行备份操作。
  bioCreateBackgroundJob(REDIS_BIO_LEVELDB_BACKUP,(void*)path,NULL,NULL);
  // 返回客户端备份已启动
  addReplyStatus(c,"backup leveldb started");
}

// 冻结命令
void freezeCommand(redisClient *c) {
    // 未开启leveldb，返回错误
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    int deleted = 0;
    int j;
    robj *o;

    // 依次处理每个key
    for (j = 1; j < c->argc; j++) {
        // 查找key
        o = lookupKeyRead(c->db,c->argv[j]);
        if (o != NULL) {
            // 该key存在
            if(o->type == REDIS_SET) {
                // 类型为set
                if(freezeKey(c->db, &server.ldb, c->argv[j], 's') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze set key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else if(o->type == REDIS_ZSET) {
                // 类型为zset
                if(freezeKey(c->db, &server.ldb, c->argv[j], 'z') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze zset key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else if(o->type == REDIS_HASH) {
                // 类型为hash
                if(freezeKey(c->db, &server.ldb, c->argv[j], 'h') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze hash key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else if(o->type == REDIS_STRING) {
                // 类型为字符串
                if(freezeKey(c->db, &server.ldb, c->argv[j], 'c') == REDIS_ERR) {
                    redisLog(REDIS_WARNING, "freezeCommand freeze string key:%s failed", (char*)c->argv[j]->ptr);
                    continue;
                }
            } else {
                redisLog(REDIS_WARNING, "freezeCommand keytype not set or zset or hash or string");
                continue;
            }
            // freezeKey会删除key，所以需要触发Keyspace事件
            signalModifiedKey(c->db,c->argv[j]);
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC,"del",c->argv[j],c->db->id);
            server.dirty++;
            deleted++;
        }
    }
    
    // 返回删除的key数量
    addReplyLongLong(c,deleted);
}

// 获取冻结的key类型
char getFreezedKeyType(int dbid, robj *key) {
    // 在freezed中查找entry
    dictEntry *de = dictFind(server.db[dbid].freezed,key->ptr);

    if (de) {
        return (char)(dictGetSignedIntegerVal(de));
    }
    return 0;
}

// 解冻key
void meltCommand(redisClient *c) {
    // 未开启leveldb，返回错误
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    int success = 0;
    int j;
    
    // 依次处理每个key
    for (j = 1; j < c->argc; j++) {
        if(isKeyFreezed(c->db->id, c->argv[j]) == 1) {
            // 解冻这个key
            if(meltKey(c->db->id, &server.ldb, c->argv[j], getFreezedKeyType(c->db->id, c->argv[j])) == REDIS_OK) {
                success++;
            } else {
                redisLog(REDIS_WARNING, "meltCommand melt key:%s failed", (char*)c->argv[j]->ptr);
            }
        }
    }
    
    // 返回解冻的key数量
    addReplyLongLong(c,success);
}

// 返回匹配参数模式的所有冻结的key
void freezedCommand(redisClient *c) {
    // 未开启leveldb，返回错误
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        addReplyError(c,"leveldb off");
        return;
    }
    
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    // reply长度最后才能知道
    void *replylen = addDeferredMultiBulkLength(c);

    // 获取安全迭代器
    di = dictGetSafeIterator(c->db->freezed);
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    // 遍历freezed
    while((de = dictNext(di)) != NULL) {
        // 获取key，type
        sds key = dictGetKey(de);
        char keytype = (char)(dictGetSignedIntegerVal(de));;
        robj *keyobj;
        robj *keyval;

        // 模式匹配
        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            // 创建key->type
            keyobj = createStringObject(key,sdslen(key));
            keyval = createStringObject(&keytype,1);
            // 添加批量回复
            addReplyBulk(c,keyobj);
            addReplyBulk(c,keyval);
            numkeys++;
            numkeys++;
            decrRefCount(keyobj);
            decrRefCount(keyval);
        }
    }
    // 释放迭代器
    dictReleaseIterator(di);
    // 设置最后reply长度
    setDeferredMultiBulkLength(c,replylen,numkeys);
}

// 删除hash
void leveldbDelHash(int dbid, struct leveldb *ldb, robj* objkey, robj *objval) {
    // 未开启leveldb，直接返回
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        return;
    }
    hashTypeIterator *hi;
    // 创建hash的头部，|dbid|h|keylen|key=|
    sds key = createleveldbHashHead(dbid, objkey->ptr);
    // 创建批量写数据结构
    leveldb_writebatch_t* wb = leveldb_writebatch_create();
    size_t klen = sdslen(key);
    char *err = NULL;

    // 遍历hash
    hi = hashTypeInitIterator(objval);
    while (hashTypeNext(hi) != REDIS_ERR) {
        // 根据不同的编码进行不同的处理
        if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
            // 当前是压缩编码
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;

            // 获取当前field
            hashTypeCurrentFromZiplist(hi, REDIS_HASH_KEY, &vstr, &vlen, &vll);
            if (vstr) {
                // field为字符串，拼接到头部
                key = sdscatlen(key, vstr, vlen);
                // 删除
                leveldb_writebatch_delete(wb, key, sdslen(key));
                // 还原key，下次循环使用
                sdsrange(key, 0, klen - 1);
            } else {
                // feild为long long，转为字符串，拼接到头部
                sds sdsll = sdsfromlonglong(vll);
                key = sdscatsds(key, sdsll);
                // 删除
                leveldb_writebatch_delete(wb, key, sdslen(key));
                // 释放
                sdsfree(sdsll);
                // 还原key，下次循环使用
                sdsrange(key, 0, klen - 1);
            }
        } else if (hi->encoding == REDIS_ENCODING_HT) {
            // 当前是hash table编码
            robj *value;
            robj *decval;

            // 获取当前key
            hashTypeCurrentFromHashTable(hi, REDIS_HASH_KEY, &value);
            // 获取解码后的value，拼接到头部
            decval = getDecodedObject(value);
            key = sdscat(key, decval->ptr);
            // 删除
            leveldb_writebatch_delete(wb, key, sdslen(key));
            // 还原key，下次循环使用
            sdsrange(key, 0, klen - 1);
            // 用完decval，减少引用计数
            decrRefCount(decval);
        } else {
            redisPanic("leveldbDelHash unknown hash encoding");
        }
    }

    // 执行批量操作
    leveldb_write(ldb->db, ldb->woptions, wb, &err);
    // 错误处理
    procLeveldbError(err, "leveldbDelHash leveldb error");
    // leveldb操作计数
    server.leveldb_op_num++;

    // 释放迭代器
    hashTypeReleaseIterator(hi);
    // 释放批量写数据结构
    leveldb_writebatch_destroy(wb);
    // 释放头部
    sdsfree(key);
}

// 删除set
void leveldbDelSet(int dbid, struct leveldb *ldb, robj* objkey, robj *objval) {
    // 未开启leveldb，直接返回
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        return;
    }
    setTypeIterator *si;
    robj *eleobj = NULL;
    int64_t intobj;
    int encoding;
    // 创建set头部，|dbid|s|keylen|key=|
    sds key = createleveldbSetHead(dbid, objkey->ptr);
    // 创建批量写数据结构
    leveldb_writebatch_t* wb = leveldb_writebatch_create();
    size_t klen = sdslen(key);
    char *err = NULL;
    
    // 遍历set
    si = setTypeInitIterator(objval);
    while((encoding = setTypeNext(si, &eleobj, &intobj)) != -1) {
        // 根据编码执行不同操作
        if (encoding == REDIS_ENCODING_HT) {
            // hast table
            // 获取解码后的redis object，拼接到key后面
            robj *decval = getDecodedObject(eleobj);
            key = sdscat(key, decval->ptr);
            // 放入批量操作
            leveldb_writebatch_delete(wb, key, sdslen(key));
            // 还原key，下次循环使用
            sdsrange(key, 0, klen - 1);
            // 用完decval减少引用计数
            decrRefCount(decval);
        } else {
            // long long编码，转为string拼接到key后面
            sds sdsll = sdsfromlonglong((long long)intobj);
            key = sdscatsds(key, sdsll);
            // 放入批量操作
            leveldb_writebatch_delete(wb, key, sdslen(key));
            // 释放
            sdsfree(sdsll);
            // 还原key，下次循环使用
            sdsrange(key, 0, klen - 1);
        }
    }
    
    // 执行批量操作
    leveldb_write(ldb->db, ldb->woptions, wb, &err);
    // 处理错误
    procLeveldbError(err, "leveldbDelSet leveldb error");
    // leveldb操作计数
    server.leveldb_op_num++;
    
    // 释放迭代器
    setTypeReleaseIterator(si);
    // 销毁批量写类型
    leveldb_writebatch_destroy(wb);
    // 释放头部
    sdsfree(key);
}

// 删除zset
void leveldbDelZset(int dbid, struct leveldb *ldb, robj* objkey, robj *objval) {
    // 未开启leveldb，直接返回
    if(server.leveldb_state == REDIS_LEVELDB_OFF) {
        return;
    }
    int rangelen = zsetLength(objval);
    // 创建zset的头部，|dbid|z|keylen|key=|
    sds key = createleveldbSortedSetHead(dbid, objkey->ptr);
    // 创建批量写数据结构
    leveldb_writebatch_t* wb = leveldb_writebatch_create();
    size_t klen = sdslen(key);
    char *err = NULL;

    // 根据不同编码进行不同处理
    if (objval->encoding == REDIS_ENCODING_ZIPLIST) {
        // 压缩编码
        unsigned char *zl = objval->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        eptr = ziplistIndex(zl,0);

        redisAssertWithInfo(NULL,objval,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        while (rangelen--) {
            redisAssertWithInfo(NULL,objval,eptr != NULL && sptr != NULL);
            redisAssertWithInfo(NULL,objval,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL) {
                // field是long long类型，转为string，拼接到key后面
                sds sdsll = sdsfromlonglong(vlong);
                key = sdscatsds(key, sdsll);
                // 放入批量操作
                leveldb_writebatch_delete(wb, key, sdslen(key));
                sdsfree(sdsll);
                // 还原key，下次循环使用
                sdsrange(key, 0, klen - 1);
            } else {
                // field是字符串类型，拼接到key后面
                key = sdscatlen(key, vstr, vlen);
                // 放入批量操作
                leveldb_writebatch_delete(wb, key, sdslen(key));
                // 还原key，下次循环使用
                sdsrange(key, 0, klen - 1);
            }
            
            // 下一元素
            zzlNext(zl,&eptr,&sptr);
        }
    } else if (objval->encoding == REDIS_ENCODING_SKIPLIST) {
        // skiplist编码
        zset *zs = objval->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;
        robj *decval;

        // 跟踪第0层的forward指针可以遍历所有元素
        ln = zsl->header->level[0].forward;

        while(rangelen--) {
            redisAssertWithInfo(NULL,objval,ln != NULL);
            ele = ln->obj;
            
            // 获取解码后的redis object
            decval = getDecodedObject(ele);
            // 拼接到key后面
            key = sdscat(key, decval->ptr);
            // 放入批量操作
            leveldb_writebatch_delete(wb, key, sdslen(key));
            // 还原key，下次循环使用
            sdsrange(key, 0, klen - 1);
            // decval用完减少引用计数
            decrRefCount(decval);
            // 下一个元素
            ln = ln->level[0].forward;
        }
    } else {
        redisPanic("leveldbDelZset unknown sorted set encoding");
    }
    
    // 执行批量操作
    leveldb_write(ldb->db, ldb->woptions, wb, &err);
    // 处理错误
    procLeveldbError(err, "leveldbDelZset leveldb error");
    // leveldb操作计数
    server.leveldb_op_num++;

    // 销毁批量写数据结构
    leveldb_writebatch_destroy(wb);
    // 释放key
    sdsfree(key);
}
