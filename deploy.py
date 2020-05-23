import json
import sys
from typing import List

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

KEY_SPLITTER = ':'


def main(
    table_name: str,
    dynamodb_resource: ServiceResource,
    dynamodb_client: BaseClient
) -> None:
    partition_key, sort_key = get_table_keys(table_name, dynamodb_client)
    src_items = get_src_items(table_name)
    put_to_table(table_name, src_items, dynamodb_resource)
    table_items = scan_table(table_name, dynamodb_resource)
    diff_keys = get_diff_keys(src_items, table_items, partition_key, sort_key)
    delete_diff_items(
        table_name,
        partition_key,
        sort_key,
        diff_keys,
        dynamodb_resource
    )


def get_table_keys(table_name: str, dynamodb_client: BaseClient) -> (str, str):
    resp = dynamodb_client.describe_table(TableName=table_name)
    sort_key = ''
    for key_schema in resp['Table']['KeySchema']:
        if key_schema['KeyType'] == 'HASH':
            partition_key = key_schema['AttributeName']
        elif key_schema['KeyType'] == 'RANGE':
            sort_key = key_schema['AttributeName']
    return partition_key, sort_key


def get_src_items(
    table_name: str
) -> List[dict]:
    return json.load(
        open(f'./src_data/{table_name}.json')
    )


def put_to_table(
    table_name: str,
    src_items: List[dict],
    dynamodb_resource: ServiceResource
) -> None:
    table = dynamodb_resource.Table(table_name)
    with table.batch_writer() as batch:
        for item in src_items:
            batch.put_item(
                Item=item
            )


def scan_table(
    table_name: str,
    dynamodb_resource: ServiceResource
) -> List[dict]:
    table = dynamodb_resource.Table(table_name)
    resp = table.scan(Limit=10)
    table_items = resp['Items']
    while 'LastEvaluatedKey' in resp:
        resp = table.scan(
            ExclusiveStartKey=resp['LastEvaluatedKey'],
            Limit=10
        )
        table_items += resp['Items']
    return table_items


def get_diff_keys(
    src_items: List[dict],
    table_items: List[dict],
    partition_key: str,
    sort_key: str
) -> List[str]:
    src_keys = []
    for src_item in src_items:
        src_keys.append(
            src_item[partition_key] + KEY_SPLITTER + (src_item[sort_key] if sort_key != '' else '')
        )
    table_keys = []
    for table_item in table_items:
        table_keys.append(
            table_item[partition_key] + KEY_SPLITTER + (table_item[sort_key] if sort_key != '' else '')
        )
    return list(set(table_keys) - set(src_keys))


def delete_diff_items(
    table_name: str,
    partition_key: str,
    sort_key: str,
    diff_keys: List[str],
    dynamodb_resource: ServiceResource
) -> None:
    table = dynamodb_resource.Table(table_name)
    for diff_key in diff_keys:
        key = {partition_key: diff_key.split(KEY_SPLITTER)[0]}
        if sort_key != '':
            sort_key_d = {sort_key: diff_key.split(KEY_SPLITTER)[1]}
            key.update(**sort_key_d)
        table.delete_item(
            Key=key
        )


if __name__ == '__main__':
    dynamodb_resource = boto3.resource('dynamodb')
    dynamodb_client = boto3.client('dynamodb')
    _, table_name = sys.argv

    main(table_name, dynamodb_resource, dynamodb_client)
