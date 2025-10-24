from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from clickhouse_driver import Client
import psycopg2

def init_clickhouse_database():
    try:
        client = Client(
            host='clickhouse',
            user='airflow',
            password='airflow',
            database='airflow',
            port=9000
        )
        
        client.execute('SELECT 1')
        
        client.execute('''
            CREATE TABLE IF NOT EXISTS user_prosthesis_reports (
                client_id String,
                date Date,
                avg_joint_angle Float32,
                max_joint_angle Float32, 
                min_joint_angle Float32,
                avg_pressure Float32,
                avg_battery Float32,
                most_common_activity String
            ) ENGINE = MergeTree()
            ORDER BY (client_id, date)
        ''')
        
        print("✅ ClickHouse connection and table verified")
        
    except Exception as e:
        print(f"❌ Error connecting to ClickHouse: {e}")
        
        try:
            client = Client(
                host='clickhouse',
                user='airflow',
                password='airflow', 
                port=9000
            )
            
            client.execute('CREATE DATABASE IF NOT EXISTS airflow')
            
            client = Client(
                host='clickhouse',
                user='airflow',
                password='airflow',
                database='airflow',
                port=9000
            )
            
            client.execute('''
                CREATE TABLE IF NOT EXISTS user_prosthesis_reports (
                    client_id String,
                    date Date,
                    avg_joint_angle Float32,
                    max_joint_angle Float32, 
                    min_joint_angle Float32,
                    avg_pressure Float32,
                    avg_battery Float32,
                    most_common_activity String
                ) ENGINE = MergeTree()
                ORDER BY (client_id, date)
            ''')
            
            print("✅ Created database and table successfully")
            
        except Exception as fallback_error:
            print(f"❌ Fallback also failed: {fallback_error}")
            print("⚠️  Continuing anyway - table might already exist")

def extract_crm_data():
    try:
        conn = psycopg2.connect(
            host="crm_db",
            database="crm", 
            user="user",
            password="password",
            port=5432
        )
        
        query = "SELECT client_id, first_name, last_name, email, gender FROM clients"
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"CRM Data: {len(df)} records")
        print(f"CRM Clients: {df['client_id'].tolist()}")
        return df
    except Exception as e:
        print(f"CRM Error: {e}")
        return pd.DataFrame()

def extract_telemetry_data():
    try:
        conn = psycopg2.connect(
            host="telemetry_db",
            database="telemetry",
            user="user", 
            password="password",
            port=5432
        )
        
        query = """
        SELECT client_id, prosthesis_type, joint_angle, pressure_sensor, 
               battery_level, activity_type, timestamp,
               DATE(timestamp) as date
        FROM telemetry
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"Telemetry Data: {len(df)} records")
        if not df.empty:
            print(f"Telemetry date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Telemetry clients: {df['client_id'].unique().tolist()}")
        return df
    except Exception as e:
        print(f"Telemetry Error: {e}")
        return pd.DataFrame()

def transform_data(**kwargs):
    ti = kwargs['ti']
    
    crm_data = ti.xcom_pull(task_ids='extract_crm_data')
    telemetry_data = ti.xcom_pull(task_ids='extract_telemetry_data')
    
    print(f"Transform: CRM records = {len(crm_data)}, Telemetry records = {len(telemetry_data)}")
    
    if crm_data.empty or telemetry_data.empty:
        print("No data to transform - one of the sources is empty")
        return []
    
    merged_df = telemetry_data.merge(crm_data, on='client_id', how='inner')
    print(f"After merge: {len(merged_df)} records")
    
    if merged_df.empty:
        print("No common clients between CRM and Telemetry!")
        return []
    
    daily_agg = merged_df.groupby(['client_id', 'date']).agg({
        'joint_angle': ['mean', 'max', 'min'],
        'pressure_sensor': 'mean',
        'battery_level': 'mean',
        'activity_type': lambda x: x.mode()[0] if not x.mode().empty else 'Unknown'
    }).reset_index()
    
    daily_agg.columns = [
        'client_id', 'date', 'avg_joint_angle', 'max_joint_angle', 'min_joint_angle',
        'avg_pressure', 'avg_battery', 'most_common_activity'
    ]
    
    print(f"Final transformed: {len(daily_agg)} daily records")
    print("Sample transformed data:")
    print(daily_agg.head(3))
    
    result = daily_agg.to_dict('records')
    return result

def load_to_clickhouse(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    print(f"Load: received {len(transformed_data) if transformed_data else 0} records")
    
    if not transformed_data:
        print("No data to load - skipping")
        return
    
    try:
        client = Client(
            host='clickhouse',
            user='airflow',
            password='airflow',
            database='airflow',
            port=9000
        )
        
        client.execute('TRUNCATE TABLE user_prosthesis_reports')
        
        data_to_insert = []
        for record in transformed_data:
            data_to_insert.append((
                str(record['client_id']),
                record['date'],
                float(record['avg_joint_angle']),
                float(record['max_joint_angle']),
                float(record['min_joint_angle']),
                float(record['avg_pressure']),
                float(record['avg_battery']),
                str(record['most_common_activity'])
            ))
        
        if data_to_insert:
            client.execute('INSERT INTO user_prosthesis_reports VALUES', data_to_insert)
            print(f"✅ Successfully loaded {len(data_to_insert)} records to ClickHouse")
            
    except Exception as e:
        print(f"❌ Error loading to ClickHouse: {e}")
        
        try:
            client = Client(
                host='clickhouse',
                user='airflow',
                password='airflow',
                port=9000
            )
            
            client.execute('USE airflow')
            client.execute('TRUNCATE TABLE user_prosthesis_reports')
            
            if data_to_insert:
                client.execute('INSERT INTO user_prosthesis_reports VALUES', data_to_insert)
                print(f"✅ Successfully loaded {len(data_to_insert)} records after fallback")
                
        except Exception as fallback_error:
            print(f"❌ Fallback also failed: {fallback_error}")
            raise

def verify_data_loaded(**kwargs):
    try:
        client = Client(
            host='clickhouse',
            user='airflow',
            password='airflow',
            database='airflow',
            port=9000
        )
        
        count = client.execute('SELECT count(*) FROM user_prosthesis_reports')[0][0]
        print(f"✅ Data loaded successfully! Records in ClickHouse: {count}")
        
        if count > 0:
            sample = client.execute('SELECT * FROM user_prosthesis_reports LIMIT 3')
            print("Sample data:")
            for row in sample:
                print(f"  {row}")
        else:
            print("❌ No data loaded to ClickHouse")
            
    except Exception as e:
        print(f"❌ Error verifying data: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'prosthesis_etl',
    default_args=default_args,
    description='ETL pipeline for prosthesis data',
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=['prosthesis', 'etl']
) as dag:
    
    init_clickhouse_task = PythonOperator(
        task_id='init_clickhouse_database',
        python_callable=init_clickhouse_database
    )
    
    extract_crm_task = PythonOperator(
        task_id='extract_crm_data',
        python_callable=extract_crm_data
    )
    
    extract_telemetry_task = PythonOperator(
        task_id='extract_telemetry_data',
        python_callable=extract_telemetry_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    load_task = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse
    )
    
    verify_task = PythonOperator(
        task_id='verify_data_loaded',
        python_callable=verify_data_loaded
    )
    
    init_clickhouse_task >> extract_crm_task >> extract_telemetry_task >> transform_task >> load_task >> verify_task