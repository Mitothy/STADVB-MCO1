import MySQLdb # TO IMPORT: pip install mysqlclient
import pandas as pd #TO IMPORT: pip install pandas
import sqlalchemy as sa # TO IMPORT: python pip install sqlalchemy

# Change "YOURUSER" to Local User and "YOURPASSWORD" to Local Password
connection = MySQLdb.connect("localhost",  "YOURUSER", "YOURPASSWORD", "seriousmd") 

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

# Remove unnecessary line breaks in doctors.csv
doctorsdf = doctorsdf.replace('\n','', regex=True)

# Showing it worked
print(doctorsdf.iloc[10590:10600].to_string(index=False))

"""
LOADING TO MYSQL
"""

# Close Connection to mySQL
connection.close()  