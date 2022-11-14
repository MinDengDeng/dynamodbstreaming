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

ddb_client = boto3.client('dynamodb')

def handler(event, context):
    print('Event: {}'.format(event))
    print('context: {}'.format(context))
    item_id = event["movieid"]
    purchases_count = event["purchases"]
    #item_id = 'tt0379786'
    print('Updating {} in DDB'.format(item_id))
    item1 = get_id_as_dict(item_id)
    print('item 1: {}'.format(item1))
    add_int_value_to_item(item_id, 'purchases', purchases_count)
    item2 = get_id_as_dict(item_id)
    print('item 2: {}'.format(item2))
    msg = 'Updated {} purchases from {} - {}'
    print(msg.format(item_id, item1['purchases'], item2['purchases']))

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
    print('SET #{} = #{} + :incr'.format(attr_name, attr_name))
    ExpressionAttributeNames={'#{}'.format(attr_name): attr_name}
    print('ExpressionAttributeNames: {}'.format(ExpressionAttributeNames))
    ExpressionAttributeValues={':incr' : {'N' : str(val)}}
    print('ExpressionAttributeValues: {}'.format(ExpressionAttributeValues))
    
    ddb_client.update_item(
        TableName=os.environ['DDB_TABLE_NAME'],
        Key={'id': {'S': item_id}},
        UpdateExpression='SET #{} = #{} + :incr'.format(attr_name, attr_name),
        ExpressionAttributeNames={'#{}'.format(attr_name): attr_name},
        ExpressionAttributeValues={':incr' : {'N' : str(val)}},
    )

