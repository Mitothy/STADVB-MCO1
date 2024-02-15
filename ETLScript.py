import MySQLdb # TO IMPORT: pip install mysqlclient
import pandas as pd #TO IMPORT: pip install pandas
import sqlalchemy as sa # TO IMPORT: python pip install sqlalchemy

# Change "YOURUSER" to Local User and "YOURPASSWORD" to Local Password
connection = MySQLdb.connect("localhost",  "Tim", "Tif2003#", "seriousmd") 

# Create cursor and use it to execute SQL command
cursor = connection.cursor()

# Loading CSVs
doctorsdf = pd.read_csv('doctors.csv', encoding="ISO-8859-1")
clinicsdf = pd.read_csv('clinics.csv', encoding="ISO-8859-1")
pxdf = pd.read_csv('px.csv', encoding="ISO-8859-1")
appointmentsdf = pd.read_csv('appointments.csv', encoding="ISO-8859-1")

"""
CLEANING
"""
# Cleaning px.csv
pxdf['age'] = pxdf['age'].fillna(0)
pxdf['age'] = pd.to_numeric(pxdf['age'], errors='coerce')
pxdf = pxdf.drop_duplicates(subset='pxid', keep='first')
pxdf.loc[pxdf['age'] > 116, 'age'] = -1 # Oldest recorded age is 116

# Cleaning doctor.csv
doctorsdf['age'] = pd.to_numeric(doctorsdf['age'], errors='coerce').fillna(-1)  
doctorsdf['mainspecialty'].fillna('Unknown', inplace=True)
doctorsdf = doctorsdf.replace('\n','', regex=True)
doctorsdf = doctorsdf.drop_duplicates(subset='doctorid', keep='first')

# Cleaning clinics.csv 
clinicsdf = clinicsdf.drop_duplicates(subset='clinicid', keep='first')

# Trimming appointments.csv into rows that have a matching IDs in other CSVs
appointmentsdf = appointmentsdf[appointmentsdf['pxid'].isin(pxdf['pxid'])]
appointmentsdf = appointmentsdf[appointmentsdf['doctorid'].isin(doctorsdf['doctorid'])]
appointmentsdf = appointmentsdf[appointmentsdf['clinicid'].isin(clinicsdf['clinicid'])]
appointmentsdf['TimeQueued'] = pd.to_datetime(appointmentsdf['TimeQueued'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
appointmentsdf['QueueDate'] = pd.to_datetime(appointmentsdf['QueueDate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
appointmentsdf['StartTime'] = pd.to_datetime(appointmentsdf['StartTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
appointmentsdf['EndTime'] = pd.to_datetime(appointmentsdf['EndTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

print(appointmentsdf)

"""
LOADING TO MYSQL
"""

# Close Connection to mySQL
connection.close()  