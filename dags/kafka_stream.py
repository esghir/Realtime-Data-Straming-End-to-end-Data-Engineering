import json
import requests
from datetime import datetime
import time
import logging
from kafka import KafkaProducer

from airflow import DAG
# L'ISLA7: L'import l'jdid bach t7iyed l'warning
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'amine',
    'start_date': datetime(2025, 8, 19)
}

def get_data():
    """Fetches random user data from an API and handles potential errors."""
    try:
        res = requests.get('https://randomuser.me/api/')
        res.raise_for_status() # Best practice: check for request errors
        res_json = res.json()
        
        if res_json.get('results'):
            return res_json['results'][0]
        else:
            logging.warning(f"API response did not contain 'results': {res_json}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Request to randomuser.me API failed: {e}")
        return None

def format_data(res):
    """Formats the raw API response into a cleaner dictionary."""
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']}, {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    # Configure logging for the script when run directly
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                             max_block_ms=5000,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    curr_time = time.time()

    while time.time() < curr_time + 60:  # Stop after 1 minute
        try:
            res = get_data()
            if res:
                formatted_data = format_data(res)
                producer.send('users_created', value=formatted_data)
                logging.info(f"Sent data for: {formatted_data.get('first_name')} {formatted_data.get('last_name')}")
            else:
                # Wait a bit before retrying if API call failed
                time.sleep(2)
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
    
    producer.flush()
    producer.close()

with DAG('user_automation',
         default_args=default_args,
         # L'ISLA7: Bedelna schedule_interval b schedule
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# L'ISLA7 L'Kbir: Mse7na l'3ayta l "stream_data()" men hna
# if __name__ == "__main__":
#     stream_data()