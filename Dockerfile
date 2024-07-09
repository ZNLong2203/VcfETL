FROM apache/airflow:2.5.1
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY ./dags /opt/airflow/dags
COPY ./src /opt/airflow/src
