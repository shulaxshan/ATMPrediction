import os
import glob
import re
import pandas as pd
import cx_Oracle
import time

# Define the directory path containing the log folders
log_directory_path = 'D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\New_Data'

# Define the path of the master log file that will contain all the log data
#master_log_path = 'D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\data1\\master_log.txt'


#pattern = r"\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-Cash Withdraw\sInitiated\s-------------\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Amount\s:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s----\sImage\sCapture\s\(TRX_RESPONSE_WITHDRAW\)\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Withdraw\sStatus\s:\sOK\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Account\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Action\sCode\s+:\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Response\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trace\sID\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----EOD\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----BATCH\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----TRX\sNO\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Cash\sWithdraw\sInitiated\sCompleted\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Send\sOnline\sData\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----ARC\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trx\sDateTime\s+:\s\d+/\d+/\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Online\sStatus\s:\sOnline_Perfoamed\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-EMV\sTransaction\sCompleted\-+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sStatus\s+:\sSuccess\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sMessage\s:\sApproved"
pattern = r"\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-Cash Withdraw\sInitiated\s-------------\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Amount\s:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s----\sImage\sCapture\s\(TRX_RESPONSE_WITHDRAW\)\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Withdraw\sStatus\s:\sOK\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Account\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Action\sCode\s+:\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Response\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trace\sID\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----EOD\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----BATCH\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----TRX\sNO\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Cash\sWithdraw\sInitiated\sCompleted\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Send\sOnline\sData\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----ARC\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trx\sDateTime\s+:\s\d+\/\d+\/\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Online\sStatus\s:\sOnline_Perfoamed\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-EMV\sTransaction\sCompleted\-+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sStatus\s+:\sSuccess\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sMessage\s:\sApproved\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\s\w*\s\w*\s*:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s\=*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-Dispense\sCommand.E\w*\s-*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-*Amount\s*:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-*Mix\s*:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-*Currency\s*:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-*Present\s*:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-*Parsed\sMix\s:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\sImage\sCapture\s\(CASH_PRESENT\)\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*Deno\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*CU\s+TYP\s*VALUE\s*NUM\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{1,2}\s*RET\s*\d{6}\s*\d{3}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{1,2}\s*REJ\s*\d{6}\s*\d{3}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{2}\s*BILL\s*\d{6}\s*\d{3}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{2}\s*BILL\s*\d{6}\s*\d{3}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{2}\s*BILL\s*\d{6}\s*\d{3}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}]\[INFO]>\s-*\d{2}\s*BILL\s*\d{6}\s*\d{3}"
def create_materlogs():
    try:
        # Loop over each folder in the log directory
        for folder_name in os.listdir(log_directory_path):
            # Construct the full path to the folder
            folder_path = os.path.join(log_directory_path, folder_name)

            # Define the path of the master log file that will contain all the log data
            master_log_path = os.path.join(folder_path, f'{folder_name}_master_log.txt')

            # Loop over each log file in the folder and append to the master log file
            with open(master_log_path, 'a') as master_log_file:
                for log_file_path in os.listdir(folder_path):
                    if log_file_path.endswith('.log'):
                        # Extract the log file name
                        log_file_name = os.path.basename(log_file_path)

                        # Append the folder and log file names to the master log file
                        master_log_file.write(f'---{folder_name}/{log_file_name}---\n')

                        # Extract the pattern from the log file and append to the master log file
                        with open(os.path.join(folder_path, log_file_path), 'r') as log_file:
                            log_contents = log_file.read()
                            matches = re.findall(pattern, log_contents)
                            for match in matches:
                                master_log_file.write(f'{match}\n')
                                print("Succesfully created master logs")
    except Exception as e:
        print(f"Error: {e}")
create_materlogs()



############# Connet Oracle DataBase and Create the table ##############
import cx_Oracle
def connect_to_db():
    try:

        dsn_tns = cx_Oracle.makedsn('localhost','1521','orcl')
        conn = cx_Oracle.connect(user='system',password='asd123',dsn=dsn_tns)
        print ("Successfully connected to the database!")

        c=conn.cursor()
        return conn, c

    except cx_Oracle.Error as error:
        print("Unsuccessfully connect to the database:", error)

connect_to_db()
########################################################################################

############################## Create Table in Oracle DataBase #########################
# def create_oracle_table():
#     conn, c = connect_to_db()
#     try:
#         c.execute('create table system.Akkaraipattu(Amount number(30),Withdraw_status varchar(15), Account varchar(30),Trace_id varchar(20),Trx_datetime Date,Online_status varchar(30),Status varchar(10),Message varchar(20),note_5000 number(10),note_1000 number(10),note_500 number(10),note_100 number(10),upload_date Date)')
#         print("Successfully created table Akkaraipattu")

#         c.execute('create table system.Battarabulla(Amount number(30),Withdraw_status varchar(15), Account varchar(30),Trace_id varchar(20),Trx_datetime Date,Online_status varchar(30),Status varchar(10),Message varchar(20),note_5000 number(10),note_1000 number(10),note_500 number(10),note_100 number(10),upload_date Date)')
#         print("Successfully created table Battarabulla")

#         c.execute('create table system.Wellawatta(Amount number(30),Withdraw_status varchar(15), Account varchar(30),Trace_id varchar(20),Trx_datetime Date,Online_status varchar(30),Status varchar(10),Message varchar(20),note_5000 number(10),note_1000 number(10),note_500 number(10),note_100 number(10),upload_date Date)')
#         print("Successfully created table Wellawatta")

#     except Exception as e:
#         print(f"Unable to create table in data base: {e}")

# create_oracle_table()       



#############################  Read mater data text file ###############################
###### Read Akkaraipattu master file #########
import re
import pandas as pd

def extract_log_data():
    try:
        with open('D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\New_Data\\Battaramulla\\Battaramulla_master_log.txt', 'r') as f:
            text = f.read()
            print("Succsfully read")

            # Extract the required data from the text file using regular expressions
            amounts = [int(m) for m in re.findall(r'Amount\s:\s*(\d+)', text)]
            withdraw_statuses = re.findall(r'Withdraw\s+Status\s*:\s*(\w+)', text)
            accounts = re.findall(r'Account\s*:\s*(\d+)', text)
            trace_ids =  re.findall(r'Trace\s+ID\s*:\s*(\d+)', text)
            trx_datetimes = re.findall(r'Trx\s+DateTime\s\s:\s(\d{1,2}\/\d{1,2}\/\d{4})\s+', text)
            online_statuses = re.findall(r'Online\s+Status\s*:\s*(.+)', text)
            statuses = re.findall(r'Status\s\s:\s*(.+)', text)
            messages = re.findall(r'Message\s*:\s*(.+)', text)
            _005000 = re.findall(r'005000\s\s(\d+)',text)
            _001000 = re.findall(r'001000\s\s(\d+)',text)
            _000500 = re.findall(r'000500\s\s(\d+)',text)
            _000100 = re.findall(r'000100\s\s(\d+)',text)

            # Create a list of dictionaries to store the extracted data for each transaction
            data_list = []
            for i in range(len(online_statuses)):
                data_dict = {
                    'amount': amounts[i],
                    'withdraw_status': withdraw_statuses[i],
                    'account': accounts[i],
                    'trace_id': trace_ids[i],
                    'trx_datetime': trx_datetimes[i],
                    'online_status': online_statuses[i],
                    'status': statuses[i],
                    'message': messages[i],
                    'note_5000':_005000[i],
                    'note_1000':_001000[i],
                    'note_500':_000500[i],
                    'note_100':_000100[i]
                }
                data_list.append(data_dict)

            # Create a pandas dataframe from the list of dictionaries
            dff = pd.DataFrame(data_list)
            dff['trx_datetime'] = pd.to_datetime(dff['trx_datetime'])
            dff['upload_date'] = pd.to_datetime('today').strftime("%d/%m/%Y")
            dff['upload_date']=dff['upload_date'].astype('datetime64[ns]')
            print("Successfully created Data Frame!")
            return dff
    except Exception as e:
        print(f"Unsuccessfully create Data Frame: {e}")
   
##########################################################################################

############################# Insert Data into Oracle Data Base ##########################
def insert_data():
    # connect to the database
    conn, c = connect_to_db()
    # insert data into the table
    try:
        for index, row in extract_log_data().iterrows():
            c.execute('INSERT INTO BATTARABULLA(Amount, Withdraw_status, Account, Trace_id, Trx_datetime, Online_status, Status, Message,note_5000,note_1000,note_500,note_100,upload_date) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)',
                [row['amount'], row['withdraw_status'], row['account'], row['trace_id'], row['trx_datetime'], row['online_status'], row['status'], row['message'], row['note_5000'],
                row['note_1000'],row['note_500'],row['note_100'],row['upload_date']])
                
        
        conn.commit()
        print("Data inserted successfully!")
        
    except cx_Oracle.Error as error:
        print("Failed to insert data into the table:", error)
    # close the database connection
    c.close()
    conn.close()

insert_data()



# def main():
#     dff = extract_log_data()
#     insert_data(dff)
    

# if __name__ == '__main__':
#     print("Starting slow function...")
#     time.sleep(300)
#     create_materlogs()
#     print("Slow function finished. Running rest of the code...")
#     main()

    







