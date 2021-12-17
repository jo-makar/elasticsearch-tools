#!/usr/bin/env python3
# Get all (including unmapped) fields from an Elasticsearch index
# Ref: https://dev.to/ddreier/finding-un-mapped-fields-in-elasticsearch-4ejl
#
# Avoid performance impact on large indices by using small intervals.
# "Small" here is dependent on the number of documents and timestamp range of the index. 

import requests

import argparse
import datetime
import json
import re

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--source', '-s', type=str, default='127.0.0.1:9200')
    parser.add_argument('--tls', '-t', action='store_true')
    parser.add_argument('--creds', '-c', type=str)
    parser.add_argument('--query', '-q', type=str)
    parser.add_argument('--interval', '-i', type=float, default=1.0) # In seconds

    parser.add_argument('index', type=str)

    args = parser.parse_args()

    base_url = ('https' if args.tls else 'http') + '://' + args.source
    requests_kwargs = { 'timeout': 100 }
    if args.creds:
        requests_kwargs['auth'] = requests.auth.HTTPBasicAuth(*args.creds.split(':',1))

    def timestamp_to_datetime(s: str) -> datetime.datetime:
        assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$', s)
        return datetime.datetime.fromisoformat(s[:-1])

    def datetime_to_timestamp(dt: datetime.datetime) -> str:
        return dt.isoformat(timespec='milliseconds')

    body = {
        'size': 1,
        'sort': [ { '@timestamp': 'asc' } ],
    }
    if args.query:
        body['query'] = json.loads(args.query)

    resp = requests.post(f'{base_url}/{args.index}/_search', json=body, **requests_kwargs)
    resp.raise_for_status()

    beg_timestamp = timestamp_to_datetime(resp.json()['hits']['hits'][0]['_source']['@timestamp'])

    body['sort'][0]['@timestamp'] = 'desc'
    resp = requests.post(f'{base_url}/{args.index}/_search', json=body, **requests_kwargs)
    resp.raise_for_status()

    end_timestamp = timestamp_to_datetime(resp.json()['hits']['hits'][0]['_source']['@timestamp'])

    initial_size = 1000

    all_fields = set()

    cur_timestamp = beg_timestamp
    while cur_timestamp <= end_timestamp:
        body = {
            'query': {
                'range': {
                    '@timestamp': {
                        'gte': datetime_to_timestamp(cur_timestamp),
                         'lt': datetime_to_timestamp(cur_timestamp + 
                                   datetime.timedelta(seconds=args.interval)),
                    }
                }
            },
            'aggs': {
                'fields': {
                    'terms': {
                        'size': initial_size,
                        'script': {
                              'lang': 'painless',

                            # To include only the root-level keys
                            #'source': 'params._source.keySet()'

                            # To include the first two levels of keys
                            #'source': '''
                            #    List rv = new ArrayList();
                            #    for (String k: params._source.keySet()) {
                            #        rv.add(k);
                            #        if (params._source[k] instanceof Map) {
                            #            for (String l: params._source[k].keySet()) {
                            #                rv.add(k + '.' + l);
                            #            }
                            #        }
                            #    }
                            #    rv
                            #'''

                            # To recursively include all levels of keys
                            'source': '''
                                void recurse(def x, String base) {
                                    List rv = new ArrayList();
                                    for (String k: x.keySet()) {
                                        rv.add(base + k);
                                        if (x[k] instanceof Map) {
                                            for (String l: recurse(x[k], k + '.')) {
                                                rv.add(l);
                                            }
                                        }
                                    }
                                    return rv;
                                }

                                recurse(params._source, '');
                            '''
                        }
                    }
                }
            },
            'size': 0,
        }
        if args.query:
            body['query'].update(args.query)

        for i in range(3):
            resp = requests.post(f'{base_url}/{args.index}/_search', json=body, **requests_kwargs)
            resp.raise_for_status()
            resp_json = resp.json()

            field_count = len(resp_json['aggregations']['fields']['buckets'])
            max_field_count = body['aggs']['fields']['terms']['size']
            if field_count < max_field_count:
                print('.', end='', flush=True)
                break

            print('x', end='', flush=True)
            body['aggs']['fields']['terms']['size'] *= 2

        # Reduce the interval or increase the initial field count size if this occurs
        assert field_count < max_field_count

        for field in [row['key'] for row in resp_json['aggregations']['fields']['buckets']]:
            all_fields.add(field)

        cur_timestamp += datetime.timedelta(seconds=args.interval)

    print('', flush=True)
    for field in sorted(all_fields):
        print(json.dumps(field))
