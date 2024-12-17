
import json
import time
from memory_profiler import profile
import datetime
import os

import pandas as pd
import requests
import boto3
from concurrent.futures import ThreadPoolExecutor


API_TOKEN = os.getenv('API_TOKEN')
BASE_URL = 'https://qiita.com/api/v2/items'
HEADERS = {
    'Authorization': f'Bearer {API_TOKEN}',
}


def get_articles(page, per_page):
    params = {
        'page': page,
        'per_page': per_page,
    }
    response = requests.get(BASE_URL,
                            headers=HEADERS,
                            params=params,
                            stream=True,
                            timeout=100
                            )
    response.raise_for_status()
    return response.json()


def upload_to_s3(article):
    session = boto3.Session(profile_name='jsl')
    s3 = session.client('s3')
    bucket_name = 'qiita-etl-project-bucket'
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    key = f'articles/original/{now}/{article["id"]}.json'
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(article))
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        raise e
    return key


def upload_to_dynamodb(article, s3_key):
    session = boto3.Session(profile_name='jsl')
    dynamodb = session.client('dynamodb')
    table_name = 'qiita-etl-project-articles'
    item = {
        'article_id': {'S': article["id"]},
        's3_key': {'S': s3_key},
        'user_name': {'S': article["user"]["name"]},
        'created_at': {'S': article["created_at"]},
        'title': {'S': article["title"]},
        'url': {'S': article["url"]},
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


def upload_article(article):
    s3_key = upload_to_s3(article)
    upload_to_dynamodb(article, s3_key)


def filter_top_articles(all_articles, top_n=20):
    top_articles = sorted(all_articles, key=lambda x: x.get(
        "likes_count", 0), reverse=True)[:top_n]
    return top_articles


def articles_to_dataframe(articles):
    data = []
    for article in articles:
        tags = '|'.join(tag['name'] for tag in article.get("tags", []))
        data.append({
            'ID': article.get("id", ""),  # 存在しない場合は空文字列
            'ユーザ名': article.get("user", {}).get("name", ""),  # ユーザ名がない場合
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


def upload_tsv_to_s3(data, bucket_name):
    session = boto3.Session(profile_name='jsl')
    s3 = session.client('s3')

    now = datetime.datetime.now().strftime("%Y-%m-%d")
    key = f"articles/processed/{now}/weekly_filtered_top20.tsv"
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=data)
    except Exception as e:
        print(f"Failed to upload to S3: {e}")


@profile()
def main():
    all_articles = []
    page = 1
    per_page = 100
    one_week_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)
    while True:
        if page == 2:
            break
        articles = get_articles(page, per_page)
        if not articles:
            break
        if articles[-1]['created_at'] < one_week_ago.isoformat():
            break
        all_articles.extend(articles)
        page += 1
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(upload_article, all_articles)

    top_articles = filter_top_articles(all_articles)
    df = articles_to_dataframe(top_articles)

    tsv_data = df.to_csv(sep='\t', index=False)

    upload_tsv_to_s3(tsv_data, 'qiita-etl-project-bucket')


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()  # 終了時間を記録
    elapsed_time = end_time - start_time  # 経過時間の計算
    print(f"Elapsed time: {elapsed_time} seconds")
