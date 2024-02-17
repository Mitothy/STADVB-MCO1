import pandas as pd #TO IMPORT: pip install pandas
import sqlalchemy as sa # TO IMPORT: pip install sqlalchemy

USERNAME = "root"
PASSWORD = "admin"

def clean_gender(gender):
    gender = gender.upper().strip()  # Convert to uppercase and strip whitespace
    if 'FEM' in gender:
        return 'FEMALE'
    elif 'MAL' in gender:
        return 'MALE'
    else:
        return None  # Return None or a default value if gender is unrecognizable


# Change "YOURUSER" to Local User and "YOURPASSWORD" to Local Password

# Create engine for loading into mySQL
engine = sa.create_engine("mysql+mysqldb://"+USERNAME+":"+PASSWORD+"@localhost/seriousmd")

# Create cursor and use it to execute SQL command

# Loading CSVs
doctorsdf = pd.read_csv('doctors.csv', encoding="ISO-8859-1")
clinicsdf = pd.read_csv('clinics.csv', encoding="ISO-8859-1")
pxdf = pd.read_csv('px.csv', encoding="ISO-8859-1")
appointmentsdf = pd.read_csv('appointments.csv', encoding="ISO-8859-1")

"""
CLEANING
"""
# Cleaning px.csv
pxdf = pxdf.drop_duplicates(subset='pxid', keep='first')
pxdf['gender'] = pxdf['gender'].apply(clean_gender)
pxdf = pxdf.dropna(subset=['age'])
pxdf['age'] = pd.to_numeric(pxdf['age'], errors='coerce')
pxdf = pxdf[(pxdf['age'] >= 0) & (pxdf['age'] <= 116)] # Oldest Recorded Age
pxdf = pxdf[pxdf['pxid'].isin(appointmentsdf['pxid'])]


# Cleaning doctor.csv
doctorsdf['age'] = pd.to_numeric(doctorsdf['age'], errors='coerce').fillna(999) # NEEDS CHANGING I FORGOT WHY I DID THIS
doctorsdf['mainspecialty'].fillna('Unknown', inplace=True)
doctorsdf = doctorsdf.replace('\n','', regex=True)
doctorsdf = doctorsdf.drop_duplicates(subset='doctorid', keep='first')

# Cleaning clinics.csv
clinicsdf = clinicsdf.drop_duplicates(subset='clinicid', keep='first')

# Cleaning appointments.csv
appointmentsdf = appointmentsdf[appointmentsdf['pxid'].isin(pxdf['pxid'])]
appointmentsdf = appointmentsdf[appointmentsdf['doctorid'].isin(doctorsdf['doctorid'])]
appointmentsdf = appointmentsdf[appointmentsdf['clinicid'].isin(clinicsdf['clinicid'])]
appointmentsdf['TimeQueued'] = pd.to_datetime(appointmentsdf['TimeQueued'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
appointmentsdf['QueueDate'] = pd.to_datetime(appointmentsdf['QueueDate'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
appointmentsdf['StartTime'] = pd.to_datetime(appointmentsdf['StartTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
appointmentsdf['EndTime'] = pd.to_datetime(appointmentsdf['EndTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

"""
LOADING TO MYSQL
"""
clinicsdf.to_sql('dim_clinic', con=engine, if_exists='append', index=False)
doctorsdf.to_sql('dim_doc', con=engine, if_exists='append', index=False)
pxdf.to_sql('dim_px', con=engine, if_exists='append', index=False)
appointmentsdf.to_sql('fact_appt', con=engine, if_exists='append', index=False)

# Close Connection to mySQL
engine.dispose()
