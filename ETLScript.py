import MySQLdb # TO IMPORT: python -m  pip install mysqlclient
import pandas as pd #TO IMPORT: pip install pandas

# Change "YOURUSER" to Local User and "YOURPASSWORD" to Local Password
connection = MySQLdb.connect("localhost",  "Tim", "Tif2003#", "seriousmd") 

# Create cursor and use it to execute SQL command
cursor = connection.cursor()

# Loading CSVs
doctorsdf = pd.read_csv('doctors.csv', encoding="ISO-8859-1")
clinicsdf = pd.read_csv('clinics.csv', encoding="ISO-8859-1")
pxdf = pd.read_csv('px.csv', encoding="ISO-8859-1")
appointmentsdf = pd.read_csv('appointments.csv', encoding="ISO-8859-1")

# Close Connection to mySQL
connection.close()