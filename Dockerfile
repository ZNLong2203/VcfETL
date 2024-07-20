FROM apache/airflow:2.5.1

# Copy requirements file and install dependencies
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --verbose

# Copy DAGs and src directory to the appropriate location
COPY ./dags /opt/airflow/dags
COPY ./src /opt/airflow/src
