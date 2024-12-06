import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert

def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)

try:
    engine=create_engine('')
    connection=engine.connect()
    print("DB Connected")

except Exception as e:
    print(e)
    print('Could not connect to database')


# Client Records (MySQL)
# urn:blvd:ReportExport:7538fcd6-1270-4007-b971-ce04abecab62
try:
    #print("Step 1: Start time")
    client_start_time = datetime.datetime.now()
    #print("Start time:", client_start_time)
    
    #print("Step 2: Reading CSV from URL")
    client_records_df = pd.read_csv("https://dashboard.boulevard.io/api/2020-01/report_exports/2af4bf62-4099-4230-a3f8-25251ab6b0ea.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
                            names=['ClientRecord_id','External_Id','First_Name','Last_Name','Email_Address','Mobile_Phone','Home_Phone','Work_Phone','Address_State','Address_Zip','Birthdate','Referral_Source_Name','Created_At','Is_Active','Updated_At'],skiprows=2,parse_dates=['Birthdate','Updated_At'],low_memory=False)
    #print("CSV Read Successfully")

    #print("Step 3: Creating list of records to delete")
    client_to_del = [row for row in client_records_df['ClientRecord_id']]
    #print("Records to delete:", client_to_del)
    
    #print("Step 4: Deleting records from database")
    connection.execute("DELETE FROM RandomSpa_boulevard.client_records WHERE ClientRecord_id IN ({})".format(str(client_to_del).strip('[]')))
    #print("Records deleted successfully")

    #print("Step 5: Dropping duplicates")
    client_records_df = client_records_df.drop_duplicates(subset='ClientRecord_id',keep='first')
    #print("Duplicates dropped")

    #print("Step 6: Inserting records into database")
    client_records_df.to_sql('client_records',engine,if_exists='append',index=False,chunksize=1000)
    #print("Records inserted successfully")

    #print("Step 7: Inserting metadata into loaded_files table")
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Client Records Export',datetime.datetime.now(),str(datetime.datetime.now()-client_start_time)))
    #print("Metadata inserted successfully")

    print('Inserted: Client Records Export | Time: '+str(datetime.datetime.now()-client_start_time))
except Exception as e:
    print('Error occurred:', e)
    print('Could not load Client Record')

# Gift Card Liabilities (MySQL)
# urn:blvd:ReportExport:d215d98e-d259-4afc-ae03-9eb81dc401d7
try:
    gc_start_time = datetime.datetime.now()
    print("Loading DF")
    gc_liabilities_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/5fde1b6ae-1b37-4234-b048-1f849dfc41bd.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                            names=['GiftCardLiability_id','Purchasing_Client','Location_Name','Gift_Card_Code','Gift_Card_Type','Gift_Card_Balance','Created_At','Gift_Card_Count','Location_Id','Median_Gift_Card_Value','Purchasing_Client_Id','Updated_At'],skiprows=2,parse_dates=['Created_At','Updated_At'],low_memory=False)
    #print("Truncating Table")
    connection.execute("TRUNCATE TABLE RandomSpa_boulevard.gift_card_liabilities")
    #print("Inserting Data")
    gc_liabilities_df.to_sql('gift_card_liabilities',engine,if_exists='append',index=False,chunksize=1000)
    #print("Inserting Loaded Files")
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Gift Card Liabilties Export',datetime.datetime.now(),str(datetime.datetime.now()-gc_start_time)))
    print('Inserted: Gift Card Liabilties Export | Time: '+str(datetime.datetime.now()-gc_start_time))
except Exception as e:
    print(e)
    print('Could not load Gift Card Liabilities')

# Voucher Liabilities (MySQL)
# urn:blvd:ReportExport:49c30cce-8abc-4fba-8448-69f6e54b7705
try:
    pd.set_option('display.max_columns', None)
    voucher_start_time = datetime.datetime.now()
    voucher_liabilities_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/5ccsd7b5-b648-41ce-ad92-05be94ddab16.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                             names=['VoucherLiability_id','Voucher_Quantity','Client_Name','Voucher_Value','Product_Category_Name','Product_Id','Product_Brand_Name','Client_Id','Product_Name','Purchase_Location','Service_Category_Name','Service_Id','Service_Name'],skiprows=2,low_memory=False)
    #print(voucher_liabilities_df.head())
    connection.execute("TRUNCATE TABLE RandomSpa_boulevard.voucher_liabilities")
    voucher_liabilities_df.to_sql('voucher_liabilities',engine,if_exists='append',index=False,chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Voucher Liabilties Export',datetime.datetime.now(),str(datetime.datetime.now()-voucher_start_time)))
    print('Inserted: Voucher Liabilties Export | Time: '+str(datetime.datetime.now()-voucher_start_time))
except Exception as e:
    print(e)
    print('Could not load Voucher Liabilities')

# Location Records (MySQL)
# urn:blvd:ReportExport:156520a8-ba05-4f30-9243-e12ffab6b820
try:
    location_start_time = datetime.datetime.now()
    location_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/eff1c427-78a8-46fa-b44a-8255c6325382.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                                    names=['LocationRecord_id','External_Id','Address_State','Address_City','Location_Name','Created_At','Updated_At'],skiprows=2,parse_dates=['Created_At','Updated_At'],low_memory=False)
    location_to_del = [row for row in location_df['LocationRecord_id']]
    connection.execute("DELETE FROM RandomSpa_boulevard.location_records WHERE LocationRecord_id IN ({})".format(str(location_to_del).strip('[]')))
    location_df.to_sql('location_records',engine,if_exists='append',index=False,chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Location Records Export',datetime.datetime.now(),str(datetime.datetime.now()-location_start_time)))
    print('Inserted: Location Records Export | Time: '+str(datetime.datetime.now()-location_start_time))
except:
    print('Could not load Location Records')

# Service Records
# urn:blvd:ReportExport:2c3b665d-f15a-476b-b992-2422eba02589
try:
    service_start_time = datetime.datetime.now()
    service_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/e8b4ce8b-5fb2-4556-82ed-f50ce32ae813.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                                    names=['ServiceRecord_id','External_Id','Service_Name','Category_Id','Category_Name','Is_Active','Default_Price','Created_At','Updated_At'],skiprows=2,parse_dates=['Created_At','Updated_At'],low_memory=False)
    service_to_del = [row for row in service_df['ServiceRecord_id']]
    connection.execute("DELETE FROM RandomSpa_boulevard.service_records WHERE ServiceRecord_id IN ({})".format(str(service_to_del).strip('[]')))
    service_df.to_sql('service_records',engine,if_exists='append',index=False,chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Service Records Export',datetime.datetime.now(),str(datetime.datetime.now()-service_start_time)))
    print('Inserted: Service Records Export | Time: '+str(datetime.datetime.now()-service_start_time))
except:
    print('Could not load Service Records')

# Product Records
# urn:blvd:ReportExport:002fb511-14ff-4b7c-a69b-705b765a8348
try:
    product_start_time = datetime.datetime.now()
    product_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/fa0039dc-8ef9-40c9-a44c-b414e2860cdb.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9-QW10',
                                    names=['ProductRecord_id','Product_Name','Brand_Name','Category_Name','Product_Sku','Product_Barcode','Product_Size','Product_Color','Is_Subscription','External_Id','Created_At','Updated_At'],skiprows=2,parse_dates=['Created_At','Updated_At'],low_memory=False)
    product_to_del = [row for row in product_df['ProductRecord_id']]
    connection.execute("DELETE FROM RandomSpa_boulevard.product_records WHERE ProductRecord_id IN ({})".format(str(product_to_del).strip('[]')))
    product_df.to_sql('product_records',engine,if_exists='append',index=False,chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Product Records Export',datetime.datetime.now(),str(datetime.datetime.now()-product_start_time)))
    print('Inserted: Product Records Export | Time: '+str(datetime.datetime.now()-product_start_time))
except:
    print('Could not load Product Records')

# # Detail Line Item (MySQL) - P1M - P14D 3/3/2024
# # urn:blvd:ReportExport:55e7d926-3348-4c8b-b8d2-0053b411dbf0
try:
    line_item_start_time = datetime.datetime.now()
    line_item_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/118c2d2e-cc6e-4868-ad8d-1e24d835b40e.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                                names=['Sale_id','Sale_Date','Location_Name','Client_Name','Staff_Name','Service_Name','Product_Name','Sale_Type','Gross_Sales','Discount_Amount','Refunds','Net_Sales','Sales_Tax','Account_Balance_Sales','Amendments','Appointment_Count','Appointment_Id','Appointment_Service_Id','Business_Charges','Capped_Business_Charges','Client_Id','Discount_Name','Discretionary_Discounts','Gift_Card_Sales','Hours_Booked','Hours_Scheduled','Location_Id',
                                       'New_Client_Count','Order_Number','Order_Client_Count','Order_Count','Order_Id','Prebooked_Percent','Prebooked_Appointment_Count','Prebooked_Service_Client_Count','Product_Client_Count','Product_Color','Product_Id','Product_Quantity_Sold','Product_Sales_Tax','Product_Sales','Product_Size','Product_Sku','Promotional_Offers','Requested_Appointment_Count','Returning_Client_Count','Sales_per_Order','Self_booked_Sales','Self_booked_Percent',
                                       'Self_booked_Appointment_Count','Service_Client_Count','Service_Count','Service_Id','Service_Sales_Tax','Service_Sales_per_Appointment','Service_Sales','Service_Staff_Count','Staff_Id','Tip_Sales','Utilization_Percent','Operator_Name','New_Memberships_Count'],skiprows=2,parse_dates=['Sale_Date'],dtype={'Utilization_Percent':'object'},low_memory=False)
    na_date_df = line_item_df[line_item_df['Sale_Date'].isna()]
    with_date_df = line_item_df[line_item_df['Sale_Date'].notna()]
    dates_to_del =  str(with_date_df["Sale_Date"].dt.strftime('%Y-%m-%d').unique()).replace(' ',',').strip('[]').replace(',nan','')
    connection.execute("DELETE FROM RandomSpa_boulevard.detail_line_item WHERE Sale_Date IN ({})".format(dates_to_del))
    with_date_df.to_sql('detail_line_item',engine,if_exists='append',index=False,chunksize=1000)
    try:
        sales_to_del = str(na_date_df['Sale_id'].unique()).replace(' ',',').strip('[]')
        connection.execute("DELETE FROM RandomSpa_boulevard.detail_line_item WHERE Sale_id IN ({})".format(sales_to_del))
        na_date_df.to_sql('detail_line_item',engine,if_exists='append',index=False,chunksize=1000)
    except:
        print('No transactions without a Date')
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Detail Line Item Export',datetime.datetime.now(),str(datetime.datetime.now()-line_item_start_time)))
    print('Inserted: Detail Line Item Export | Time: '+str(datetime.datetime.now()-line_item_start_time))
except:
    print('Could not load Detail Line Item - P1M')

# Gift Card Redemptions
# urn:blvd:ReportExport:fe858803-f75a-4ed3-af81-2c48f3e91bb9
try:
    gc_redemptions_start_time = datetime.datetime.now()
    gc_redemptions_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/9ffc25c2-2f35-44bf-b99d-149b226434fd.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
                                names=['GiftCardRedemption_id','Gift_Card_Code','Purchased_Client_Name','Redeeming_Client_Name','Purchased_Location_Name','Redemption_Location_Name','Redeemed_On','Redemption_Value','Purchased_Client_Id','Purchased_Location_Id','Redeemed_Location_Id','Redeeming_Client_Id','Redeemed_Order_Id'],skiprows=2,parse_dates=['Redeemed_On'],low_memory=False)
    dates_to_del = ','.join(map(str, gc_redemptions_df['Redeemed_On'].dt.strftime('%Y-%m-%d').unique()))
    connection.execute("DELETE FROM RandomSpa_boulevard.gift_card_redemptions WHERE Redeemed_On IN ({})".format(dates_to_del))
    gc_redemptions_df.to_sql('gift_card_redemptions', engine, if_exists='append', index=False, chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Gift Card Redemptions Export', datetime.datetime.now(), str(datetime.datetime.now() - gc_redemptions_start_time)))
    print('Inserted: Gift Card Redemptions Export | Time: ' + str(datetime.datetime.now() - gc_redemptions_start_time))


except Exception as e:
    print(e)
    print('Could not load Gift Card Redemptions')

# Remove duplicate rows from Gift Card Redemptions
try:
    with engine.begin() as conn:  # Automatically commits or rolls back
        # Step 1: Create temporary table with unique rows
        conn.execute(text("""
            CREATE TEMPORARY TABLE tmp_gift_card_redemptions AS
            SELECT * FROM gift_card_redemptions
            GROUP BY GiftCardRedemption_id
        """))
        
        # Step 2: Delete all rows from the original table
        conn.execute(text("DELETE FROM gift_card_redemptions;"))
        
        # Step 3: Insert unique rows back into the original table from the temporary table
        conn.execute(text("INSERT INTO gift_card_redemptions SELECT * FROM tmp_gift_card_redemptions;"))
        
        # Step 4: Drop the temporary table
        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_gift_card_redemptions;"))

        print("Duplicate rows removed successfully.")
        
except Exception as e:
    print(e)
    print('Could not remove duplicate rows from Gift Card Redemptions')


# Voucher Redemptions
# urn:blvd:ReportExport:ea2eee12-09c7-48e3-b8bc-5e0b813a0446
try:
    pd.set_option('display.max_columns', None)
    voucher_redemptions_start_time = datetime.datetime.now()
    print('Loading Voucher Redemptions')
    voucher_redemptions_df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/ade125ba-1f20-47dd-aa55-28ffa9f6bf38.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9-do',
                                names=['VoucherRedemption_id','Voucher_Redemptions','Voucher_Redemption_Value','Purchase_Location','Purchasing_Client','Product_Name','Product_Category_Name','Redeemed_Location','Redeemed_On','Redeemed_Order_Id','Service_Category_Name','Service_Name','Product_Id','Service_Id'],skiprows=2,parse_dates=['Redeemed_On'],low_memory=False)
    print('Truncating Table')
    # Filter out rows where Client_name is 'All' and drop the 'Client_name' column
    #voucher_redemptions_df = voucher_redemptions_df[voucher_redemptions_df['Client_name'] != 'All'].drop(columns=['Client_name'])
    
    # Extract unique dates from Redeemed_On column
    dates_to_del = tuple(voucher_redemptions_df['Redeemed_On'].dt.strftime('%Y-%m-%d').unique())

    # Construct the SQL query with placeholders for each date
    delete_query = "DELETE FROM RandomSpa_boulevard.voucher_redemptions WHERE Redeemed_On IN ({})".format(', '.join(['%s'] * len(dates_to_del)))
    
    # Execute the delete query with the list of dates
    connection.execute(delete_query, dates_to_del)
    
    print('Inserting Data')
    voucher_redemptions_df.to_sql('voucher_redemptions', engine, if_exists='append', index=False, chunksize=1000)
    
    print('Inserting Loaded Files')
    log_query = "INSERT INTO RandomSpa_boulevard.loaded_files VALUES (%s, %s, %s)"
    connection.execute(log_query, ('Voucher Redemptions Export', datetime.datetime.now(), datetime.datetime.now() - voucher_redemptions_start_time))
    
    print('Inserted: Voucher Redemptions Export | Time:', datetime.datetime.now() - voucher_redemptions_start_time)

except Exception as e:
    print(e)
    print('Could not load Voucher Redemptions')

# Payments & Refunds
# urn:blvd:ReportExport:9ed68ac5-b44d-45c3-97e7-f4f7ff9bbc01
try:
   start_time = datetime.datetime.now()
   df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/501ht101-94b3-44e6-922b-0640c3b5481a.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.GV0ini',
                   names=['TransactionID','CreatedOn','LocationName','ClientName','OrderNumber','PaymentMethod','Note','TransactionAmount','CardBrand','CardLastFour','CardSwiped','ClientId','CreatedAt','LocationId','MerchantName','OperatorName','OrderId'],skiprows=2,parse_dates=['CreatedOn','CreatedAt'],low_memory=False)
   # Print the oldest date in the "CreatedOn" column
   oldest_date = df["CreatedOn"].min()
   print("Removing rows with the oldest date:", oldest_date)
   df_filtered = df[df["CreatedOn"] != oldest_date]
   records_to_del = str(df_filtered["CreatedOn"].dt.strftime('%Y-%m-%d').unique()).replace(' ',',').strip('[]').replace(',nan','')
   connection.execute("DELETE FROM RandomSpa_boulevard.payments_refunds WHERE CreatedOn IN ({})".format(str(records_to_del)))
   df_filtered.to_sql('payments_refunds',engine,if_exists='append',index=False,chunksize=1000)
   connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('Payments & Refunds Export',datetime.datetime.now(),str(datetime.datetime.now()-start_time)))
   print('Inserted: Payments & Refunds Export | Time: '+str(datetime.datetime.now()-start_time))
except:
   print('Could not load Payments & Refunds')

# New NewFloats
# urn:blvd:ReportExport:2a9a1c77-a34c-4c9e-87ef-a1458ee5f072
try:
    start_time = datetime.datetime.now()
    df = pd.read_csv('https://dashboard.boulevard.io/api/2020-01/report_exports/07ce8977-2626-46a2-976c-b0f3ac4314b6.csv?signature=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9-etA',
                     low_memory=False)
    df = df[df['SaleDate Date']!='All']
    df.drop(columns=['Sale operator_name'],inplace=True)
    df = df[df['New Client Count'].notna()]
    df.set_axis(['SaleDate','LocationName','OperatorName','NewFloats'],axis=1,inplace=True)
    df['SaleDate'] = pd.to_datetime(df['SaleDate'])
    records_to_del = str(df['SaleDate'].dt.strftime('%Y-%m-%d').unique()).replace(' ',',').strip('[]').replace(',nan','')
    connection.execute("DELETE FROM RandomSpa_boulevard.new_floats WHERE SaleDate IN ({})".format(str(records_to_del).strip('[]')))
    df.to_sql('new_floats',engine,if_exists='append',index=False,chunksize=1000)
    connection.execute("INSERT INTO RandomSpa_boulevard.loaded_files VALUES ('{}','{}','{}')".format('New Floats Export',datetime.datetime.now(),str(datetime.datetime.now()-start_time)))
    print('Inserted: New Floats Export | Time: '+str(datetime.datetime.now()-start_time))
except Exception as e:
    print(e)
    print('Could not load New Floats')