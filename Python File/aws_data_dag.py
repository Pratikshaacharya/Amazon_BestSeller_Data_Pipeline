from airflow.providers.http.hooks.http import HttpHook 
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import json
import requests
import pandas as pd
import awswrangler as wr
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 15),
    'email': ['email address'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}



def check_api_availability(**kwargs):
        
        url = "https://real-time-amazon-data.p.rapidapi.com/best-sellers"
    
        querystring = {"category":"software","type":"BEST_SELLERS","page":"1","country":"US"}
        
        headers = {
        "X-RapidAPI-Key": ["your_rapid_api_key"],
        "X-RapidAPI-Host": ["rapid_api_host"]
        }
        
        try:
                response = requests.get(url, headers=headers, params=querystring)
                
                if response.status_code == 200:
                        return True
                else:
                        print(f"API returned status code:{response.status_code}")
                        return False
        except Exception as e:
                print(f"Error checking API: {str(e)}")
                return False


def clean_price(price_str):
        try:
            return float(price_str.replace('$', '').replace(',', '').strip())
        except:
            return None

def transform_data_cleaned(**kwargs):
        raw_data = kwargs['ti'].xcom_pull(task_ids='extract_aws_data')
        data = json.loads(raw_data)
    
        best_sellers = data['data']['best_sellers']

    
        transformed = [
                {
                    "rank": item["rank"],
                    "asin": item["asin"],
                    "title": item["product_title"],
                    "price": clean_price(item["product_price"]),
                    "rating": item["product_star_rating"],
                    "ratings_count": item["product_num_ratings"],
                    "url": item["product_url"],
                    "image": item["product_photo"],
                }
                for item in best_sellers
        ]
        return json.dumps(transformed)




def store_aws(**kwargs):
        transformed_json = kwargs['ti'].xcom_pull(task_ids='transform_aws_data')
        transformed_data_list = json.loads(transformed_json)
        df_data = pd.DataFrame(transformed_data_list)
        df_data["rank"] = pd.to_numeric(df_data["rank"], errors="coerce").astype("Int64")
        df_data["price"] = pd.to_numeric(df_data["price"], errors="coerce")
        df_data["rating"] = pd.to_numeric(df_data["rating"], errors="coerce")
        df_data["ratings_count"] = pd.to_numeric(df_data["ratings_count"], errors="coerce").astype("Int64")
        df_data["title"] = df_data["title"].astype(str)
        df_data["url"] = df_data["url"].astype(str)
        df_data["image"] = df_data["image"].astype(str)

        aws_credentials = {"key": ["your_key"],
        "secret": ["your_secret_key"],
        "token":["your_token"]
        now = datetime.now()
        dt_string = now.strftime("%d%m%Y%H%M%S")
        dt_string = 'amazon_best_sellers_' + dt_string


        boto3_session = boto3.Session(
                aws_access_key_id=aws_credentials["key"],
                aws_secret_access_key=aws_credentials["secret"],
                aws_session_token=aws_credentials["token"]
        )
        filename = f"s3://amazon-best-sellers-airflow/raw_data/{dt_string}.parquet"

        wr.s3.to_parquet(
        df=df_data,
        path=filename,                   
        index=False,
        boto3_session=boto3_session
        )

def _process_response(response):
        return response.text



with DAG('amazon_best_sellers__dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

        check_api=PythonOperator(
                task_id='check_api',
                python_callable=check_api_availability)

        extract_data = HttpOperator(
        task_id='extract_aws_data',
        http_conn_id='rapidapi_amazon',
        endpoint='best-sellers?category=software&type=BEST_SELLERS&page=1&country=US',
        method='GET',
        headers={
            "X-RapidAPI-Key": ["your_rapid_api_key"],
            "X-RapidAPI-Host": ["rapid_api_host"]
        },
        response_filter=lambda response: response.text,
        log_response=True,
        do_xcom_push=True
        )

        transform = PythonOperator(
        task_id='transform_aws_data',
        python_callable=transform_data_cleaned)
        

        s3_data =  PythonOperator(
        task_id='s3_data',
        python_callable= store_aws)

        check_api >> extract_data >> transform >> s3_data

