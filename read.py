import os
import glob
import re
import pandas as pd
import cx_Oracle

# Define the directory path containing the log folders
log_directory_path = 'D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\data1'

# Define the path of the master log file that will contain all the log data
master_log_path = 'D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\data1\\master_log.txt'

# Define the regular expression pattern to extract specific 
# pattern = r"-Cash Withdraw Initiated .*\n(?:.+\n)*?.*Fallback Reason  : Approved"
# pattern = r"-Cash Withdraw Initiated .*\n(?:.+\n)*?.*-EMV Transaction Completed"
#pattern = r"-Cash Withdraw Initiated .*\n(?:.+\n)*?.*?(Fallback Reason  : Approved|---Cash Withdraw Initiated Fail)"
#pattern= r"-Cash Withdraw Initiated .*\n(?:.+\n)*?.*?(Fallback Reason  : Approved|\n---Cash Withdraw Initiated Fail)"

pattern = r"\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-Cash Withdraw\sInitiated\s-------------\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Amount\s:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1,2}\]\[INFO]>\s----AUX\sNO\s:\s:xx:[a-zA-Z0-9]{0,22}-\d{2}\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s----\sImage\sCapture\s\(TRX_RESPONSE_WITHDRAW\)\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Withdraw\sStatus\s:\sOK\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Account\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Action\sCode\s+:\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Response\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trace\sID\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----EOD\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----BATCH\sID\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----TRX\sNO\s+:\s\w*\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Cash\sWithdraw\sInitiated\sCompleted\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---Send\sOnline\sData\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----ARC\s+:\s\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Trx\sDateTime\s+:\s\d+/\d+/\d+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-----Online\sStatus\s:\sOnline_Perfoamed\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s-EMV\sTransaction\sCompleted\-+\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sStatus\s+:\sSuccess\n\[\d{8}\s\d{6}\s\d{3}]\[\d{1}]\[INFO]>\s---\sMessage\s:\sApproved"


try:
    # Loop over each folder in the log directory
    for folder_name in os.listdir(log_directory_path):
        # Construct the full path to the folder
        folder_path = os.path.join(log_directory_path, folder_name)

        # Loop over each log file in the folder
        for log_file_path in glob.glob(os.path.join(folder_path, '*.log')):
            # Extract the log file name
            log_file_name = os.path.basename(log_file_path)

            # Extract the folder name
            folder_name = os.path.basename(folder_path)

            # Append the folder and log file names to the master log file
            with open(master_log_path, 'a') as master_log_file:
                master_log_file.write(f'---Branch-{folder_name}/{log_file_name}---\n')

            # Extract specific data from the log file using regular expression pattern matching
            with open(log_file_path, 'r') as log_file:
                log_data = log_file.read()
                matches = re.findall(pattern, log_data)

                # Append the extracted data to the master log file
                with open(master_log_path, 'a') as master_log_file:
                    for match in matches:
                        master_log_file.write(match)
                        master_log_file.write('\n')

except Exception as e:
    print(f"Error: {e}")



############# Connet Oracle DataBase and Create the table ##############
def connect_to_db():
    try:

        dsn_tns = cx_Oracle.makedsn('localhost','1521','orcl')
        conn = cx_Oracle.connect(user='system',password='asd123',dsn=dsn_tns)
        print ("Successfully connected to the database!")

        # create a new table called "my_table"
      #  c=conn.cursor()
       # c.execute('create table system.my_table(Customer_ID varchar(20),Gender varchar(10), Age int)')
        #print("Successfully created table my_table")
       

    except cx_Oracle.Error as error:
        print("Failed to create table:", error)
       # print("Failed to connect to DataBase:", error)

connect_to_db()
#########################################################################

##############  Read mater data text file ###################

# Open the text file and read its contents
with open('D:\\KBSL\\OneDrive - KBSL Information Technologies Limited\\Chulax\\ML\\ATM withdrawal Prediction\\NDB Project\\data1\\master_log.txt', 'r') as f:
    text = f.read()
    print("Succsfully read")


