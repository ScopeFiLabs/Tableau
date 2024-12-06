import pandas as pd
import datetime
import pymysql
import os
from sqlalchemy import create_engine, text

# Get the current timestamp
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Define the log file path and create the directory if it doesn't exist
log_dir = '/home/ubuntu/cron_scripts/logs/'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'RandomSpa_boulevard_api.txt')

# Log the timestamp when the script began running
with open(log_file, 'a') as log:
        log.write(f'Beginning to Load Files at: {current_time}\n')



engine=create_engine('')
connection=engine.connect()

current_directory = os.path.dirname(os.path.abspath(__file__))

# Subscriptions (MySQL)
# urn:blvd:ReportExport:41e47911-d2df-492b-b1e6-2c8f70450f01
subscriptions_start_time = datetime.datetime.now()
subscriptions_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/3XaMPl3.csv?signature=3xTraK3yG03sHere.ey',
names=['Subscription_id','Location_Name','Client_Name','Product_Name','Start_On','End_On','Subscription_Interval','Term_Number','Subscription_MRR','Client_Id','Location_Id','Product_Id',
'Subscription_ARR','Subscription_Status', ],skiprows=2,parse_dates=['Start_On','End_On'])
connection.execute(text("TRUNCATE TABLE RandomSpa_boulevard.subscriptions"))
subscriptions_df.to_sql('subscriptions',engine,if_exists='append',index=False,chunksize=1000)
connection.execute(text("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Subscriptions Export',datetime.datetime.now(),
    str(datetime.datetime.now()-subscriptions_start_time))))
connection.execute(text("DELETE FROM RandomSpa_boulevard.active_members WHERE as_of = CAST(NOW() AS date)"))
connection.execute(text("INSERT INTO RandomSpa_boulevard.active_members (SELECT Location_Id,COUNT(*) active_members,CAST(NOW() AS date) as_of FROM RandomSpa_boulevard.subscriptions WHERE Subscription_Status = 'Active' GROUP BY Location_Id)"))
print('Inserted: Subscriptions Export | Time: '+str(datetime.datetime.now()-subscriptions_start_time))

with open(log_file, 'a') as log:
    log.write(f'Subscriptions: Success | Time {{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}\n')
