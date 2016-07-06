#!/usr/bin/env python
# -*- coding: utf8 -*-

import boto3
import os
import logging
import json
import time
import sys


dynamodb = boto3.resource('dynamodb')
#table = dynamodb.Table('name')
usage = '''           Usage: python dynamodb_copy.py
                                        --src [source table]
                                        --dst [destination table]
                                        --f   force init destination table'''
backup_info = {}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def db_copy(src, dst, backup_file):
    global backup_info
    backup_info['db_copy'] = True
    with open(backup_file, 'w') as f:
        json.dump(backup_info, f)
    count = 0
    start = int(time.time())
    ExclusiveStartKey = None
    while count < src.item_count:
        try:
            if ExclusiveStartKey:
                rs = src.scan(ExclusiveStartKey=ExclusiveStartKey)
            else:
                rs = src.scan()
            for e in rs['Items']:
                dst.put_item(Item=e)
                count += 1
                if count % 1000 == 0:
                    logger.debug("Copy %d item" % count)
            if 'ExclusiveStartKey' in rs:
                ExclusiveStartKey=rs['ExclusiveStartKey']
            else:
                break
        except Exception, e:
            logger.debug(e)
            logger.debug("db_copy error")
            raise e
    end = int(time.time())
    logger.debug("Copying task completed. Time taken: " + str(end - start))
    del backup_info['db_copy']
    with open(backup_file, 'w') as f:
        json.dump(backup_info, f)


def handle_argument():
    ret = {}
    ret['force_init'] = False
    args = sys.argv
    if len(sys.argv) < 5:
        print usage
        sys.exit()
    if '--src' not in args or '--dst' not in args:
        print 'source table and destination table are required'
        sys.exit()

    if '-f' in args:
        ret['force_init'] = True

    # finding the index of index
    src_index = args.index('--src') + 1
    dst_index = args.index('--dst') + 1
    if sys.argv[src_index] == sys.argv[dst_index]:
        print "Source and Destination cannot ba same table"
        sys.exit(0)

    ret['src'] = args[src_index]
    ret['dst'] = args[dst_index]
    return ret


def table_check(src, dst):
    try:
        src = dynamodb.Table(src)
        dst = dynamodb.Table(dst)
        srcStreamArn = src.latest_stream_arn  #check dst exist or not
        dstStreamArn = dst.latest_stream_arn  #check src and get its struct
    except Exception, e:
        logging.debug("Error geting stream arns from src and dst")
        logging.debug('%s', e)
        sys.exit(0)

    if src.creation_date_time > dst.creation_date_time:
        logger.debug("Destination Talbe created before Source Table")
        sys.exit()

    if src.key_schema != dst.key_schema:
        logger.debug("Key schema does not equal!")
        sys.exit()

    if dst.item_count != 0:
        logger.debug("dst is not empty")

    return src, dst


def main():
    global backup_info 
    args = handle_argument()
    src, dst = table_check(args['src'], args['dst'])
    backup_file = '%sTO%s.data' % (args['src'], args['dst'])
    now = int(time.time())
    if args['force_init']:
        os.remove(backup_file)
        logger.debug('force_init mode, remove file:%s', backup_file)

    if os.path.isfile(backup_file):
        logger.debug('file \'%s\' exist, we should resume the last action', backup_file)
        with open(backup_file, 'r') as f:
            backup_info = json.load(f)
    else:
        backup_info['init'] = True

    if 'init' in backup_info: 
        del backup_info['init']
        logger.debug('first time or force_init mode')
        # get the last stream itera
        db_copy(src, dst, backup_file)
        # start stream
    elif 'db_copy' in backup_info:
        logger.debug('Resume copy process')
    else:  # 处理stream断掉的情况
        pass


if __name__ == '__main__':
    main()