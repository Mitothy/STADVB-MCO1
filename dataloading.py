import MySQLdb # TO IMPORT: python -m  pip install mysqlclient

#Change "YOURUSER" to Local User and "YOURPASSWORD" to Local Password
connection = MySQLdb.connect("localhost",  "YOURUSER", "YOURPASSWORD", "seriousmd") 

# Create cursor and use it to execute SQL command
cursor = connection.cursor()
cursor.execute("SELECT VERSION()")

data = cursor.fetchone()
if data:
    print('Version retrieved: ', data)
else:
    print('Version not retrieved.')

connection.close()