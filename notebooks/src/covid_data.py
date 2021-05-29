import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    import psycopg2 
except:
    install('psycopg2-binary')
    import psycopg2
    
try:
    from sqlalchemy import create_engine
except:
    install('sqlalchemy')
    from sqlalchemy import create_engine
    
    
try:
    import pandas as pd 
except:
    install('pandas')
    import pandas as pd 

try:
    import matplotlib 
except:
    install('matplotlib')
    import matplotlib

try:
    import sklearn 
    from sklearn.preprocessing import MinMaxScaler
except:
    install('sklearn')
    import sklearn
    from sklearn.preprocessing import MinMaxScaler


from datetime import date, timedelta
from multiprocessing import Pool, cpu_count
import datetime

engine = create_engine('postgresql://airflow:airflow@postgres/Covid_DB')
def Get_DF_of_day(Day):
    try: 
        URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(URL_Day)
        DF_day['Day']=Day
        return DF_day.reset_index(drop=True)
    except Exception as e:
        print(e)
        print(f"{Day} not available")
        pass

def Get_days():
    List_of_days=[]
    sdate = date(2020, 1, 1)
    edate = date.today()
    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        List_of_days.append(day.strftime("%m-%d-%Y"))
    return List_of_days

def Get_All_countries_data():
    List_of_days = Get_days()
    DF_all=[]
#     with Pool(processes=int(cpu_count()/2)) as P:
#         DF_all = P.map(Get_DF_of_day, List_of_days)
    for item in List_of_days:
        DF_all.append(Get_DF_of_day(item))
    DF_All_Countires=pd.concat(DF_all).reset_index(drop=True)
    DF_All_Countires.to_sql('covid_data_raw', engine, if_exists = 'replace')
    
def clean_data():
    DF_All_Countires = pd.read_sql_query('SELECT * FROM covid_data_raw', engine)
    cond=(DF_All_Countires.Country_Region=='United Kingdom')
    Selec_columns=['Day','Country_Region', 'Last_Update',
          'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
          'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
    DF_UK=DF_All_Countires[cond][Selec_columns].reset_index(drop=True)
    DF_UK['Last_Update']=pd.to_datetime(DF_UK.Last_Update, infer_datetime_format=True)  
    DF_UK['Day']=pd.to_datetime(DF_UK.Day, infer_datetime_format=True)  
    DF_UK['Case_Fatality_Ratio']=DF_UK['Case_Fatality_Ratio'].astype(float)
    DF_UK.to_sql('uk_data', engine, if_exists = 'replace')

def report_data():
    today = date.today().strftime("%m-%d-%Y")
    DF_UK = pd.read_sql_query('SELECT * FROM uk_data', engine)
    Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK = DF_UK[Selec_Columns]
    min_max_scaler = MinMaxScaler()
    DF_UK_scaled = pd.DataFrame(min_max_scaler.fit_transform(DF_UK[Selec_Columns]),columns=Selec_Columns)
    DF_UK_scaled.index=DF_UK.index
    DF_UK_scaled['Day']=DF_UK.Day
    DF_UK_scaled[Selec_Columns].plot(figsize=(20,10))
    plt.savefig('output/uk_scoring_report.png')
    DF_UK_scaled.to_csv('output/uk_scoring_report.csv')
    DF_UK.to_csv('output/uk_scoring_report_NotScaled.csv')
    DF_UK.to_sql(f'uk_scoring_report_{today}', engine, if_exists = 'replace')
    DF_UK_scaled.to_sql(f'uk_scoring_report_scaled_{today}', engine, if_exists = 'replace')
    
default_args = {
    'owner': 'mhd',
    'start_date': dt.datetime(2021, 5, 29),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
    
}

with DAG('covid_data',
        default_args=default_args,
        catchup=False,
        schedule_interval=timedelta(hours=24)) as dag:
    
        Get_All_countries_data = PythonOperator(
            task_id='Get_All_countries_data',
            python_callable=Get_All_countries_data,
            provide_context=False
        )

        clean_data = PythonOperator(
            task_id='clean_data',
            python_callable=clean_data,
            provide_context=False
        )
        
        report_data = PythonOperator(
            task_id='report_data',
            python_callable=report_data,
            provide_context=False
        )


        Get_All_countries_data >>  clean_data >> report_data
