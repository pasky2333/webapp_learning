#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import asyncio, logging

import aiomysql

# 执行sql时打印文档
def log(sql, args=()):
    logging.info('SQL: %s', %sql)

# 创建连接池,全局连接池，每个http请求都可以从连接池中直接获取数据库连接
# 好处是不必频繁打开和关闭数据连接
# 连接池由全局变量__pool存储，缺省情况下将编码设置为utf8
async def create_pool(loop, **kw):
    logging.info('create database connction pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop=loop
    )
# 创建select函数执行select语句，传入sql语句和sql参数
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            logging.info('rows return: %s', %len(rs))
            return rs

# 创建execute函数执行insert，delete， update语句，传入sql语句和sql参数
async def execute(sql, args, autocommit=True):
    log(sql, args)
    async with __pool as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected
