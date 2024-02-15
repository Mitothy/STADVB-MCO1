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

# Replace Null Values with 0 in px.csv
pxdf['age'] = pxdf['age'].fillna(0)
pxdf = pxdf.drop_duplicates(subset='pxid', keep='first')

# Remove unnecessary line breaks in doctors.csv
doctorsdf = doctorsdf.replace('\n','', regex=True)
doctorsdf = doctorsdf.drop_duplicates(subset='doctorid', keep='first')

# Cleaning clinics.csv 
clinicsdf['hospitalname'].replace('', 'Unknown', inplace=True)
clinicsdf = clinicsdf.drop_duplicates(subset='clinicid', keep='first')

# Trimming appointments.csv into rows that have a matching IDs in other CSVs
appointmentsdf = appointmentsdf[appointmentsdf['pxid'].isin(pxdf['pxid'])]
appointmentsdf = appointmentsdf[appointmentsdf['doctorid'].isin(doctorsdf['doctorid'])]
appointmentsdf = appointmentsdf[appointmentsdf['clinicid'].isin(clinicsdf['clinicid'])]

# New appointmentsdf # of rows
print(len(appointmentsdf))

"""
LOADING TO MYSQL
"""

# Close Connection to mySQL
connection.close()  