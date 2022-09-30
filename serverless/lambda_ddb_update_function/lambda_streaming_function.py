'''
Run this Lambda with a long timeout to send movies updates to DynamoDB

Environment variables
 DDB_TABLE_NAME - The dynamo db table that contains the movie data
'''

from __future__ import print_function
import random
import time
import os

import boto3
import constants

ddb_client = boto3.client('dynamodb')

def handler(event, context):
    while context.get_remaining_time_in_millis() > 2000:
        item_id = random_id()
        print('Updating {} in DDB'.format(item_id))
        item = get_id_as_dict(item_id)
        if not item or not item.get('rank', None):
            continue
        # add some clicks and purchases
        inverted_rank = 5001 - item['rank']
        num = random.randint(0, inverted_rank // 250) + 1
        add_int_value_to_item(item_id, 'clicks', num)
        if float(num) / 10 > 1:
            add_int_value_to_item(item_id, 'purchases', num / 4)
        msg = 'Updated {} with clicks - {}, and purchases - {}'
        print(msg.format(item_id, num, num / 4))
        time.sleep(0.100)

def random_id():
    return random.choice(constants.ALL_IDS)

def item_to_dict(item):
    resp = {}
    if type(item) is str:
        return item
    for key, struct in item.items():
        if type(struct) is str:
            if key == 'I':
                return int(struct)
            else:
                return struct
        else:
            for k, v in struct.items():
                if k == 'S':
                    value = str(v.encode('utf-8'))
                elif k == 'N':
                    if '.' in v:
                        value = float(v)
                    else:
                        value = int(v)
                elif k == 'SS':
                    value = [li for li in v]
                else:
                    key = k
                    value = item_to_dict(v)
                resp[key] = value
    return resp

def get_id_as_dict(movie_id):
    item = ddb_client.get_item(
        TableName=os.environ['DDB_TABLE_NAME'],
        Key={'id': {'S': movie_id}}
    )
    item = item.get('Item', None)
    if item:
        return item_to_dict(item)
    return None

def add_int_value_to_item(item_id, attr_name, val):
    ddb_client.update_item(
        TableName=os.environ['DDB_TABLE_NAME'],
        Key={'id': {'S': item_id}},
        UpdateExpression='SET #{} = #{} + :incr'.format(attr_name, attr_name),
        ExpressionAttributeNames={'#{}'.format(attr_name): attr_name},
        ExpressionAttributeValues={':incr' : {'N' : str(val)}},
    )
