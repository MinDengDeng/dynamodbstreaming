from __future__ import print_function

from datetime import datetime
import json
import os
from pytz import timezone

import constants
from signed_request import send_signed

def handler(event, context):
    movie_buffer = EsBuffer(os.environ['MOVIES_INDEX_NAME'], 'movie', use_timestamp=False)
    update_buffer = EsBuffer('logs', 'log', use_timestamp=True)
    # print(event['Records'])
    for record in event['Records']:
        #print(record['eventID'])
        if record['eventName'] == 'INSERT':
            movie_buffer.add_record(record['eventName'].lower(),
                                    item_to_dict(record['dynamodb']['NewImage']))
        elif record['eventName'] == 'DELETE':
            # TODO not handling deletes
            print('Ignoring DELETE')
        elif record['eventName'] == 'MODIFY':
            new_image = item_to_dict(record['dynamodb']['NewImage'])
            old_image = item_to_dict(record['dynamodb']['OldImage'])
            print("Old image: {}".format(old_image))
            print("New Image: {}".format(new_image))
            movie_buffer.add_record(record['eventName'].lower(), new_image)
            update_buffer.add_record('insert',
                                     create_monitoring_record(new_image, old_image),
                                     has_id=False)
    if len(movie_buffer) > 0:
        movie_buffer.flush()
    if len(update_buffer) > 0:
        update_buffer.flush()
    return True

class EsBuffer(object):
    operation_control_codes = {'insert' : 'index', 'delete': 'delete', 'modify': 'update'}
    def __init__(self, es_index_root, es_index_type, use_timestamp=True):
        self.buffer = list()
        self.es_index_root = es_index_root
        self.es_index_type = es_index_type
        self.use_timestamp = use_timestamp
    def __len__(self):
        return len(self.buffer) // 2
    def _get_index_name(self):
        if not self.use_timestamp:
            return self.es_index_root
        now_utc = datetime.now(timezone('UTC'))
        here_now = now_utc.astimezone(timezone(constants.TIME_ZONE))
        return "{}-{}".format(self.es_index_root, here_now.strftime("%Y.%m.%d"))
    def _get_operation_code(self, operation):
        if not operation.lower() in self.operation_control_codes:
            raise RuntimeError('Bad operation "{}" for ES Buffer'.format(operation))
        return self.operation_control_codes[operation.lower()]
    def add_record(self, operation, record, has_id=True):
        '''Buffer a log line and an indexing command for that line'''
        components = { "_index": "{}".format(self._get_index_name()),
                       "_type": "{}".format(self.es_index_type),
                     }
        if has_id:
            components['_id'] = record['id']

        control_line = '{{"{}" : {} }}'
        control_line = control_line.format(self._get_operation_code(operation),
                                           json.dumps(components))
        self.buffer.append(control_line)
        doc_line = '{}'.format(json.dumps(record))
        self.buffer.append(doc_line)
    def flush(self):
        print(self.buffer)
        endpoint = os.environ['AES_ENDPOINT']
        if not endpoint:
            raise RuntimeError('AES manager no endpoint')
        url = "https://%s/_bulk/" % endpoint
        result = send_signed("post", url, region=os.environ['REGION'],
                             body='\n'.join(self.buffer) + '\n')
        msg = 'Flushed buffer for {}/{}, {} records. Status: {}'
        msg = msg.format(self.es_index_root, self.es_index_type,
                         len(self), result.status)
        print(msg)
        if result.status == -1:
            print(result)
        return result

def create_monitoring_record(new_image, old_image):
    # Now add a record to the monitoring index.
    now_utc = datetime.now(timezone('UTC'))
    here_now = now_utc.astimezone(timezone(constants.TIME_ZONE))
    return {
        '@timestamp': here_now.replace(microsecond=0).isoformat(),
        'movie_id': new_image['id'],
        'clicks': int(new_image['clicks']) - int(old_image['clicks']),
        'purchases': int(new_image['purchases']) - int(old_image['purchases'])
    }

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

