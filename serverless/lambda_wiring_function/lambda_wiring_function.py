'''
 This code wires together the pieces of the lab. It sends a mapping to Amazon ES
 and then loads data into the Dynamo table.

 REQUIRED environment variables

 REGION - The region where everything is deployed
 S3_BUCKET - The bucket that contains the source movie data
 S3_MOVIE_KEY - The object key for the movie data in S3
 DDB_TABLE_NAME - Table to send the Imdb data
 AES_ENDPOINT - The endpoint of the Amazon Elasticsearch Service domain
 MOVIES_INDEX_NAME - The name of the index for the movie data
 STACK_PREFIX - The user's parameter for stack prefix
 USER_POOL_ID - The ID of the Cognito user pool. Allows create/delete domain
'''

from __future__ import print_function
from signed_request import send_signed

import json
import os
import random
import uuid

import boto3
import constants
import requests
import cfnresponse

FIELD_TYPES = {
    'id': 'S',
    'title': 'S',
    'year': 'N',
    'rating': 'N',
    'running_time_secs': 'N',
    'genres': 'SS',
    'release_date': 'S',
    'directors' : 'SS',
    'image_url': 'S',
    'plot': 'S',
    'rank': 'N',
    'actors': 'SS',
    'price': 'N',
    'clicks': 'N',
    'purchases': 'N',
    'location': 'S',
}

def handler(event, context):
    '''Lambda main entry point'''
    print('Wiring function invoked. Sending mapping to AES')
    print('Event: {}'.format(event))
    responseData = {}
    physicalResourceId = {}
    if event['RequestType'] == 'Create':
        try:
            send_mapping()
            send_data_to_ddb()
            #create_cognito_domain()
            create_cognito_user()
        except Exception as e:
            cfnresponse.send(event, context, cfnresponse.FAILED,responseData, physicalResourceId)
            return False
    if event['RequestType'] == 'Delete':
        try:
            delete_cognito_domain()
        except Exception as e:
            cfnresponse.send(event, context, cfnresponse.FAILED,responseData, physicalResourceId)
            return False 
    cfnresponse.send(event, context, cfnresponse.SUCCESS,responseData, physicalResourceId)
    return True

def send_mapping():
    '''Send the Movie mapping to the domain'''
    print('Entered send_mapping')
    index_name = os.environ['MOVIES_INDEX_NAME']
    url = 'https://{}/{}'.format(os.environ['AES_ENDPOINT'], index_name)
    print('Deleting any existing index at {}'.format(url))
    send_signed('delete', url, region=os.environ['REGION'])
    print('Sending to URL {}'.format(url))
    body = ' '.join(constants.MAPPING.split())
    print('Mapping body\n{}'.format(body))
    result = send_signed('put', url, region=os.environ['REGION'], body=body)
    print('Sent the mapping {}. Result: {}'.format(index_name, result))
    if result.status < 200 or result.status >= 300:
        raise RuntimeError('Bad status code for sending the mapping, aborting')

def send_data_to_ddb():
    '''Load the movie data into the DDB Table. This will also send to the
    Dynamo stream, which will trigger the stream Lambda to load it into the
    Amazon Elasticsearch Service domain'''
    print('Entered send_data_to_ddb')
    bucket = os.environ['S3_BUCKET']
    movie_key = os.environ['S3_MOVIE_KEY']
    print('Sending data to DDB, source bucket {}, key {}'.format(bucket, movie_key))
    try:
        s3 = boto3.resource('s3')
        print('Loading object from S3')
        file_contents = s3.Object(bucket, movie_key).get()["Body"].read()
        imdb_data = json.loads(file_contents)
        print('S3 Key loaded')
        count = 0
        for rec in imdb_data:
            if not valid_record(rec):
                if not rec.get('fields', None):
                    continue
                rec = make_record_valid_with_random_data(rec)
            fields = inject_price_clicks_and_purchases(rec['fields'])
            #fields = inject_location(rec['fields'])
            put_item(rec['id'],
                     rec['fields']['year'],
                     **inject_types(fields))
            count += 1
            if (count % 100) == 0:
                print('Processed {} records'.format(count))
    except Exception as e:
        print("Wiring function failed, execption message {}".format(e.message))
        raise

#def create_cognito_domain():
#    print('Creating Cognito domain')
#    cognito_idp = boto3.client('cognito-idp')
#    domain_name = '{}-{}'.format(os.environ['STACK_PREFIX'], str(uuid.uuid1())[:8])
#    print('Creating domain: {}'.format(domain_name))
#    try:
#        response = cognito_idp.create_user_pool_domain(
#            Domain=domain_name,
#            UserPoolId=os.environ['USER_POOL_ID']
#        )
#        print('Created Cognito domain')
#        return domain_name
#    except Exception as e:
#        print('Exception creating Cognito domain.\nMessage: {}'.format(e.message))
#        return None

def create_cognito_user():
    print('Creating Cognito user')
    cognito_idp = boto3.client('cognito-idp')
    try:
        response = cognito_idp.admin_create_user(
            UserPoolId=os.environ['USER_POOL_ID'],
            Username='kibana',
            TemporaryPassword='Abcd1234!'
        )
        print('Created user.')
    except Exception as e:
        print('Exception creating Cognito user.\nMessage: {}'.format(e.message))

def delete_cognito_domain():
    try:
        print('Deleting cognito domain')
        cognito_idp = boto3.client('cognito-idp')
        user_pool_info = cognito_idp.describe_user_pool(
            UserPoolId=os.environ['USER_POOL_ID']
        )
        print('User pool info:\n{}'.format(user_pool_info))
        user_pool_info = user_pool_info.get('UserPool', None)
        if not user_pool_info:
            print('Trying to delete the Cognito domain, but can\'t describe it!')
            print('Ignoring.')
            return
        domain_name = user_pool_info.get('Domain')
        if len(domain_name) < 1:
            print('Trying to delete the Cognito domain, but the Domain name is empty!')
            print('Ignoring and continuing.')
            return
        print('Deleting Cognito domain: {}'.format(domain_name))
        cognito_idp.delete_user_pool_domain(
            Domain=domain_name,
            UserPoolId=os.environ['USER_POOL_ID']
        )
    except Exception as e:
        print("Exception deleting cognito domain.\nMessage: {}".format(e))

def send_response(event, context, status_code):
    '''Return the required result to the pre-signed S3 URL so that CloudFormation
       knows that the function completed and it can continue.'''
    print('Entered send_response')
    response_body = {'Status': status_code,
                     'Reason': 'See CloudWatch Log Stream: ' + context.log_stream_name,
                     'PhysicalResourceId': context.log_stream_name,
                     'StackId': event['StackId'],
                     'RequestId': event['RequestId'],
                     'LogicalResourceId': event['LogicalResourceId'],
                     'Data': {'Data': 'No data'}}
    json_response_body = json.dumps(response_body)
    print('Response: {}'.format(json_response_body))
    headers = {
        'content-type' : '',
        'content-length' : str(len(json_response_body))
    }
    try:
        req = requests.put(event['ResponseURL'],
                           data=json_response_body,
                           headers=headers)
        if req.status_code != 200:
            print('Received non 200 response while sending to CFN\nText: {}'.format(req.text))
            raise Exception('Recieved non 200 response while sending response to CFN.')
        print('Successfully sent response to CFN\n{}'.format(req.text))
        return
    except requests.exceptions.RequestException as e:
        print(e)
        raise

def valid_record(rec):
    '''Returns true if all of the desired fields are present in the movie record'''
    if not rec.get('id', None):
        return False
    elif not rec.get('fields', None):
        return False
    elif not rec['fields'].get('year', None):
        return False
    elif not rec['fields'].get('rating', None):
        return False
    elif not rec['fields'].get('rank', None):
        return False
    return True

def make_record_valid_with_random_data(rec):
    '''Add some additional fields to the record if they can be faked'''
    if not rec.get('id', None):
        new_id = None
        while not new_id or new_id in constants.ALL_IDS:
            new_id = 'tt{02d}'.format(random.randint(1000000, 10000000))
        rec['id'] = new_id
    if not rec['fields'].get('year', None):
        rec['fields']['year'] = random.randint(1910, 2019)
    if not rec['fields'].get('rating', None):
        rec['fields']['rating'] = float(random.randint(1, 100)) / 10.0
    if not rec['fields'].get('rank', None):
        rec['fields']['rank'] = random.randint(1, 5001)
    return rec

def price_from_rating(rating):
    ''' 9.99, 19.99, or 29.99'''
    return float(int((rating * 4) / 10) * 10) - 0.01

def clicks_from_rank(rank):
    '''Rank runs from 1 to 5000, with 1 as best, 5000 as worst. Invert the sense
       and provide clicks as a random number proportional to the rank and with a
       window of 10% around that.'''
    inverted_rank = 5001 - rank # TODO! Magic number alert
    base_clicks = random.random() * inverted_rank * 1000
    window = random.random() * inverted_rank / 10
    fuzz = random.uniform(base_clicks - window, base_clicks + window)
    return int(max(0.0, fuzz)) # can't have negative clicks

def purchases_from_rating_and_rank(rating, rank):
    '''Rank runs from 1 to 5000, with 1 as best, 5000 as worst. Invert the sense.'''
    inverted_rank = 5001 - rank
    purchase_window = 1 + random.random() * rating # 1% +- rating% will buy
    return int(purchase_window * inverted_rank)

def inject_price_clicks_and_purchases(rec):
    '''To make this fun and to have updates, add some fields for price, click count,
       and purchase count to the fields of rec.'''
    rec['price'] = price_from_rating(rec['rating'])
    rec['clicks'] = clicks_from_rank(rec['rank'])
    rec['purchases'] = purchases_from_rating_and_rank(rec['rating'], rec['rank'])
    return rec

def inject_location(rec):
    '''Pick a random location so users can play with GEO data too'''
    rec['location'] = random.choice(constants.ALL_LOCATIONS)
    return rec

def put_item(item_id, item_year, **kwargs):
    '''Send a single item to the Dynamo table'''
    table_name = os.environ['DDB_TABLE_NAME']
    items_dict = {
        'id': {'S': item_id},
        'year': {'N': item_year}
    }
    #    items_dict = {
#        'id': {'S': item_id},
#        'year': {'N': '2099'},
#        'plot': {'S': 'Katniss Everdeen and Peeta Mellark become targets of the Capitol after their victory in the 74th Hunger Games sparks a rebellion in the Districts of Panem.'}, 
#        'genres': {'SS': ['Action', 'Adventure', 'Sci-Fi', 'Thriller']}, 
#        'title': {'S': 'The Hunger Games: Catching Fire'}, 
#        #'release_date': {'S': '2013-11-11T00:00:00Z'}, 
#        'rank': {'N': '4'}, 
#        'running_time_secs': {'N': '8760'}, 
#        'directors': {'SS': ['Francis Lawrence']}, 
#        'image_url': {'S': 'http://ia.media-imdb.com/images/M/MV5BMTAyMjQ3OTAxMzNeQTJeQWpwZ15BbWU4MDU0NzA1MzAx._V1_SX400_.jpg'}, 
#        #'year': {'N': '2013'}, 
#        'actors': {'SS': ['Jennifer Lawrence', 'Josh Hutcherson', 'Liam Hemsworth']}, 
#        'rating': {'N': '7.6'}, 
#        'price': {'N': '29.99'}, 
#        'clicks': {'N': '4950953'}, 
#        'purchases': {'N': '15628'}, 
#        #'location': {'S': '31.46, -106.29'}
#    }

    print(kwargs)

    items_dict.update(**kwargs)
    boto3.client('dynamodb').put_item(TableName=table_name, Item=items_dict)

def inject_types(fields):
    '''TODO: this should recurse. Shortcut since movies are not nested.'''
    ret = dict()
    for k, v in fields.items():
        # Sadly, json.loads reads numbers as numbers, but boto wants them as strings
        if FIELD_TYPES[k] == 'N':
            v = str(v)
        ret[k] = {FIELD_TYPES[k]: v}
    return ret
