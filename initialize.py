import os
import json
import sys
from typing import List
from decimal import Decimal

import boto3
from boto3.resources.base import ServiceResource
from boto3.dynamodb.types import Binary


def main(table_name: str) -> None:
    table_items = scan_table(table_name, dynamodb_resource)
    put_to_src_data(table_name, table_items)


def scan_table(
    table_name: str,
    dynamodb_resource: ServiceResource
) -> List[dict]:
    table = dynamodb_resource.Table(table_name)
    resp = table.scan()
    table_items = resp['Items']
    while 'LastEvaluatedKey' in resp:
        resp = table.scan(
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        table_items.extend(resp['Items'])
    return table_items


def default(obj) -> object:
    if isinstance(obj, Decimal):
        if int(obj) == obj:
            return int(obj)
        else:
            return float(obj)
    elif isinstance(obj, Binary):
        return obj.value
    elif isinstance(obj, bytes):
        return obj.decode()
    elif isinstance(obj, set):
        return list(obj)
    try:
        return str(obj)
    except Exception:
        return None


def put_to_src_data(
    table_name: str,
    table_items: List[dict]
) -> None:
    f = open(f'src_data/{table_name}.json', 'w')
    json.dump(table_items, f, default=default, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    dynamodb_resource = boto3.resource('dynamodb')
    _, table_name = sys.argv

    main(table_name)
