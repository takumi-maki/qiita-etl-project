import json
import sys
import time
import datetime
from aiohttp import ClientError
import boto3
import requests
import pandas as pd
from awsglue.utils import getResolvedOptions
from concurrent.futures import ThreadPoolExecutor


job_params = [
    'secret_name',
    'region_name',
    's3_bucket_name',
    'dynamodb_table_name',
    'qiita_get_items_api_url',
]

args = getResolvedOptions(sys.argv, job_params)
print('args', args)

SECRET_NAME = args['secret_name']
REGION_NAME = args['region_name']
S3_BUCKET_NAME = args['s3_bucket_name']
DYNAMODB_TABLE_NAME = args['dynamodb_table_name']
QIITA_GET_ITEMS_API_URL = args['qiita_get_items_api_url']


def get_secret_api_token(secret_name: str, region_name: str) -> str:
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Failed to get to token: {e}")
        raise e

    secret = get_secret_value_response['SecretString']
    secret_json = json.loads(secret)
    print('secret_json', secret_json)
    return secret_json.get("api_token")


def get_articles(page: int, per_page: int, api_token: str, qiita_get_items_api_url: str) -> dict:
    headers = {
        'Authorization': f'Bearer {api_token}',
    }
    params = {
        'page': page,
        'per_page': per_page,
    }
    response = requests.get(
        qiita_get_items_api_url, headers=headers, params=params, stream=True, timeout=100)
    response.raise_for_status()
    return response.json()


def fetch_recent_articles(qiita_get_items_api_url: str, api_token: str, days: int = 7) -> list[dict]:
    all_articles = []
    page = 1
    per_page = 100
    one_week_ago = datetime.datetime.now() - datetime.timedelta(days=days)
    while True:
        articles = get_articles(
            page, per_page, api_token, qiita_get_items_api_url)
        if not articles or articles[-1]['created_at'] < one_week_ago.isoformat():
            break
        all_articles.extend(articles)
        page += 1
    return all_articles


def upload_to_s3(article: dict, bucket_name: str) -> str:
    s3 = boto3.client('s3')
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    key = f'articles/original/{now}/{article["id"]}.json'
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(article))
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise e
    return key


def upload_to_dynamodb(article: dict, s3_key: str, table_name: str):
    dynamodb = boto3.client('dynamodb')
    item = {
        'article_id': {'S': article.get("id", "")},
        's3_key': {'S': s3_key},
        'user_id': {'S': article.get("user", {}).get("id", "")},
        'created_at': {'S': article.get("created_at", "")},
        'title': {'S': article.get("title", "")},
        'url': {'S': article.get("url", "")},
        'tags': {'SS': [tag['name'] for tag in article.get("tags", [])]},
        'likes_count': {'N': str(article.get("likes_count", 0))},
        'reactions_count': {'N': str(article.get("reactions_count", 0))},
        'comments_count': {'N': str(article.get("comments_count", 0))},
        'stocks_count': {'N': str(article.get("stocks_count", 0))},
    }
    try:
        dynamodb.put_item(TableName=table_name, Item=item)
    except Exception as e:
        print(f"Failed to insert item: {e}")
        raise e


def upload_article(article: str, bucket_name: str, table_name: str) -> None:
    s3_key = upload_to_s3(article, bucket_name)
    upload_to_dynamodb(article, s3_key, table_name)


def filter_top_articles(all_articles: list[dict], top_n: int = 20) -> list[dict]:
    top_articles = sorted(all_articles, key=lambda x: x.get(
        "likes_count", 0), reverse=True)[:top_n]
    return top_articles


def articles_to_dataframe(articles: list[dict]) -> pd.DataFrame:
    data = []
    for article in articles:
        tags = '|'.join(tag['name'] for tag in article.get("tags", []))
        data.append({
            'ID': article.get("id", ""),
            'ユーザID': article.get("user", {}).get("id", ""),
            '生成日時': article.get("created_at", ""),
            'タイトル': article.get("title", ""),
            'URL': article.get("url", ""),
            'いいね数': article.get("likes_count", 0),
            'リアクション数': article.get("reactions_count", 0),
            'コメント数': article.get("comments_count", 0),
            'ストック数': article.get("stocks_count", 0),
            'タグ': tags
        })
    df = pd.DataFrame(data)
    return df


def upload_tsv_to_s3(data: str, bucket_name: str) -> None:
    s3 = boto3.client('s3')
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    key = f"articles/processed/{now}/weekly_filtered_top20.tsv"
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=data)
    except Exception as e:
        print(f"Failed to upload to S3: {e}")


def main(secret_name: str, region_name: str, s3_bucket_name: str, dynamodb_table_name: str, qiita_get_items_api_url: str):
    api_token = get_secret_api_token(secret_name, region_name)
    lookback_days = 7
    articles = fetch_recent_articles(
        qiita_get_items_api_url, api_token, lookback_days)

    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(lambda article: upload_article(
            article, s3_bucket_name, dynamodb_table_name), articles)

    top_articles = filter_top_articles(articles)
    df = articles_to_dataframe(top_articles)
    tsv_data = df.to_csv(sep='\t', index=False)
    upload_tsv_to_s3(tsv_data, s3_bucket_name)


if __name__ == '__main__':
    start_time = time.time()
    main(SECRET_NAME, REGION_NAME, S3_BUCKET_NAME,
         DYNAMODB_TABLE_NAME, QIITA_GET_ITEMS_API_URL)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")
