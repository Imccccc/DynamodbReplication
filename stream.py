#!/usr/bin/env python
# -*- coding: utf8 -*-
import sys
import traceback
import time
import boto3
import redis

def get_point_before_copy(arn):
    rsp = client.describe_stream(StreamArn=arn, Limit=100)
    if 'LastEvaluatedShardId' in rsp['StreamDescription']:
        print 'LastEvaluatedShardId = ', rsp['StreamDescription']['LastEvaluatedShardId']
    shards = rsp['StreamDescription']['Shards']
    for shard in shards:
        SNrange = shard['SequenceNumberRange']
        if 'EndingSequenceNumber' in SNrange:
            pass
        shardId = shard['ShardId']

    rsp = client.get_shard_iterator(StreamArn=arn,
        ShardId=shardId,
        ShardIteratorType='TRIM_HORIZON')  # 只要最近的stream record

    # 获得 records 在里面找sequence number
    rsp = client.get_records(ShardIterator=rsp['ShardIterator'], Limit=128)
    sequenceNumber = ''
    for record in rsp['Records']:
        if 'SequenceNumber' in record:
            sequenceNumber = record['SequenceNumber']
    #print shardId, '\n', sequenceNumber
    return shardId, sequenceNumber


def isInSNrange(SNrange, sequenceN):
    if sequenceN is '':
        return False
    n = int(sequenceN)
    start = int(SNrange['StartingSequenceNumber'])
    if n == 0:
        return False

    if 'EndingSequenceNumber' not in SNrange:
        if n >= start:
            return True
    else:
        end = int(SNrange['EndingSequenceNumber'])
        if (sequenceN >= start and
                sequenceN <= end):
            return True
    return False


def get_stream_arn(table_name):
    rsp = client.list_streams(TableName=table_name)
    assert_aws_succ(rsp)
    assert 'Streams' in rsp
    assert len(rsp['Streams']) == 1
    if 'LastEvaluatedStreamArn' in rsp:  # 这个要不要记录？
        raise  #这种情况不应该发生

    return rsp['Streams'][0]['StreamArn']


def get_shardIterator(shardid, streamArn, SequenceNumber=''):
    if SequenceNumber:
        rsp = client.get_shard_iterator(StreamArn=streamArn,
                                        ShardId=shardid,
                                        ShardIteratorType='AT_SEQUENCE_NUMBER',
                                        SequenceNumber=SequenceNumber)
    else:  #should get start the oldest record
        rsp = client.get_shard_iterator(StreamArn=streamArn,
                                        ShardId=shardid,
                                        ShardIteratorType='TRIM_HORIZON')
    return rsp['ShardIterator']


def get_records(ShardIterator):
    rsp = client.get_records(ShardIterator=ShardIterator,
                             Limit=1000)
    assert 'Records' in rsp
    if 'NextShardIterator' in rsp:
        return rsp['Records'], rsp['NextShardIterator']
    else:  # the shard has been closed
        return rsp['Records'], None


def handle_records(records, table, shardId, mod_cb):
    global count
    if table is None:
        sys.exit(0)
        logger.error('In handle_records, Dest Talbe is None!')
    if len(records) != 0:
        for record in records:
            assert 'dynamodb' in record
            now = int(time.time())
            # 这个应该放入redis
            SequenceNumber = record['dynamodb']['SequenceNumber']
            r.hmset(hkey, {'lasttime': str(now), 'ShardId': shardId,
                    'sequenceNumber': SequenceNumber})
            if record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY':
                data = record['dynamodb']['NewImage']
                if mod_cb:
                    mod_cb(data)
                try:
                    dbclient.put_item(TableName=table, Item=data)
                    count += 1
                    logger.info("%s %s %s", count, record['eventName'],
                                record['dynamodb']['Keys'])
                    if record['eventName'] == 'INSERT':
                        attr_inc('sync_INSERT_success')
                    else:
                        attr_inc('sync_MODIFY_success')

                except ValidationException, e:
                    logger.error(e)
                    logger.error(traceback.format_exc())
                    if record['eventName'] == 'INSERT':
                        attr_inc('sync_INSERT_failed')
                    else:
                        attr_inc('sync_MODIFY_failed')
                    raise e
            # TODO: handle event = REMOVE
            if record['eventName'] == 'REMOVE':
                key = record['dynamodb']['Keys']
                try:
                    dbclient.delete_item(TableName=table, Key=key)
                    count += 1
                    logger.info("%s %s %s", count, record['eventName'],
                                record['dynamodb']['Keys'])
                    attr_inc('sync_REMOVE_success')
                except ValidationException, e:
                    logger.error(e)
                    logger.error(traceback.format_exc())
                    attr_inc('sync_REMOVE_failed')
                    raise e


def get_shards(streamArn, shardId, nextpage):
    if not nextpage:
        rsp = client.describe_stream(StreamArn=streamArn, Limit=100)
    else:
        rsp = client.describe_stream(StreamArn=streamArn, Limit=100,
                                     ExclusiveStartShardId=shardId)
    shards = []
    found = False
    if nextpage:
        return rsp['StreamDescription']['Shards']

    for s in rsp['StreamDescription']['Shards']:
        if s['ShardId'] == shardId:
            found = True
        if found:
            shards.append(s)

    return shards