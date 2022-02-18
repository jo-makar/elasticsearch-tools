#!/usr/bin/env python3
# Download an (entire) Elasticsearch index
# Each file is named as timestamp (incl. usec) of the first record
#
# To continue an interrupted download:
# Add an argument @timestamp argument of the last (incomplete) file minus one usec
# Eg if the last record's @timestamp was 1629936319214 use 1629936319213
# Easily found with: expr $(tail -1 $(ls *.json | tail -1) | jq -M '.sort[0]') - 1

import requests

import argparse
import json
import os
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--source', '-s', type=str, default='127.0.0.1:9200')
    parser.add_argument('--tls', '-t', action='store_true')
    parser.add_argument('--creds', '-c', type=str)
    parser.add_argument('--compress', '-x', action='store_true')
    parser.add_argument('--query', '-q', type=str)
    parser.add_argument('--extra', '-e', type=str)

    # Ignore shard failures, implies a willingness to accept only the available data.
    # Otherwise retry on shard failures and abort if there are too many consecutive failures.
    parser.add_argument('--ignore', '-i', action='store_true')

    parser.add_argument('index', type=str)
    parser.add_argument('after', nargs='?')

    args = parser.parse_args()

    base_url = ('https' if args.tls else 'http') + '://' + args.source
    requests_kwargs = { 'timeout': 100 }
    if args.creds:
        requests_kwargs['auth'] = requests.auth.HTTPBasicAuth(*args.creds.split(':',1))

    # Ref: https://www.elastic.co/guide/en/elasticsearch/reference/7.x/paginate-search-results.html

    request_size = 10000
    file_flush_size = 100000
    file_rotate_size = 1000000

    # TODO Should track each request time to warn if it exceeds keep_alive
    keep_alive = '1m'

    resp = requests.post(f'{base_url}/{args.index}/_pit?keep_alive={keep_alive}', **requests_kwargs)
    resp.raise_for_status()
    pit_id = resp.json()['id']

    body = {
        'size': request_size,
        'pit': {
            'id': pit_id,
            'keep_alive': keep_alive
        },
        'sort': [
            { '@timestamp': 'asc' },
            # Requires Elasticsearch 7.12+
            #{ '_shard_doc': 'desc' }
        ]
    }

    if args.query:
        body['query'] = json.loads(args.query)
    if args.extra:
        body.update(json.loads(args.extra))

    if args.after:
        body['search_after'] = [args.after]

    output_filename = None
    output_fileobj = None
    output_count = 0

    failures = 0 # Consecutive failures

    while True:
        resp = requests.get(f'{base_url}/_search', json=body, **requests_kwargs)
        resp.raise_for_status()
        resp_json = resp.json()

        if args.ignore:
            print('p' if resp_json['_shards']['failed'] > 0 else '.', end='', flush=True)
        else:
            if resp_json['_shards']['failed'] > 0:
                print('f', end='', flush=True)
                failures += 1
                assert failures < 5
                time.sleep(30)
                continue
            else:
                print('.', end='', flush=True)
                failures = 0

        for hit in resp_json['hits']['hits']:
            if output_fileobj is None:
                output_filename = str(hit['sort'][0]) + '.json'
                output_fileobj = open(output_filename, 'w')
                output_count = 0

            output_fileobj.write(json.dumps(hit) + "\n")
            output_count += 1

            if output_count % file_flush_size == 0:
                output_fileobj.flush()
                print('o', end='', flush=True)

            if output_count >= file_rotate_size:
                output_fileobj.close()
                if args.compress:
                    os.system(f'xz {output_filename} &')
                print('x', end='', flush=True)

                output_fileobj = None
                output_filename = None
                output_count = 0

        if args.ignore:
            # Cannot effectively known when to stop based on available records and shard failures.
            # Assume if there are no records at all with shard failures that it's time to stop.
            if resp_json['_shards']['failed'] > 0:
                if len(resp_json['hits']['hits']) == 0:
                    print('0', flush=True)
            else:
                if len(resp_json['hits']['hits']) < request_size:
                    print('', flush=True)
                    break
        else:
            if len(resp_json['hits']['hits']) < request_size:
                print('', flush=True)
                break

        body['pit']['id'] = resp_json['pit_id']
        body['search_after'] = resp_json['hits']['hits'][-1]['sort']
        if 'track_total_hits' not in body:
            body['track_total_hits'] = False
