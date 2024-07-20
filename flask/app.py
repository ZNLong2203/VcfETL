from flask import Flask, request, jsonify
import requests
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

AIRFLOW_API_URL = 'http://airflow:8080/api/v1/dags/my_insert_data_dag/dagRuns'
AIRFLOW_USERNAME = 'admin'
AIRFLOW_PASSWORD = 'admin'

@app.route('/minio-event', methods=['POST'])
def minio_event():
    content = request.json
    app.logger.info(f'Received event: {content}')
    if 'Records' in content:
        for record in content['Records']:
            if record['eventName'] == 's3:ObjectCreated:Put':
                file_name = record['s3']['object']['key']
                response = trigger_airflow_dag(file_name)
                return response
    return '', 200

@app.route('/test', methods=['GET'])
def test():
    return 'Flask is running!'

def trigger_airflow_dag(file_name):
    data = {
        "conf": {
            "vcf_file": file_name
        }
    }
    response = requests.post(AIRFLOW_API_URL, json=data, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
    app.logger.info(f'Triggered Airflow DAG with file: {file_name}, response: {response.text}')
    return response.text, response.status_code

if __name__ == '__main__':
       app.run(host='0.0.0.0', port=5000)
