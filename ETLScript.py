from sqlalchemy import event
from sqlalchemy.engine import Engine
from datetime import datetime
import time
import logging

logging.basicConfig()
logger = logging.getLogger("myapp.sqltime")
logger.setLevel(logging.DEBUG)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    now = datetime.now().strftime("%H:%M:%S")
    logger.debug(" Start Query at %s: %s", now, statement)

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    logger.debug(" Query Complete!")
    logger.debug(" Total Time: %f", total)


import pandas as pd #TO IMPORT: pip install pandas
import sqlalchemy as sa # TO IMPORT: pip install sqlalchemy
import facts

# MySQL connection credentials
USERNAME = "root"
PASSWORD = "admin"

# User defined functions
def clean_gender(gender):
    gender = gender.upper().strip()  # Convert to uppercase and strip whitespace
    if 'FEM' in gender:
        return 'FEMALE'
    elif 'MAL' in gender:
        return 'MALE'
    else:
        return None  # Return None or a default value if gender is unrecognizable

# Create engine for loading into mySQL
engine = sa.create_engine("mysql+mysqldb://"+USERNAME+":"+PASSWORD+"@localhost/seriousmd")

# Loading CSVs
now = datetime.now()
doctorsdf = pd.read_csv('doctors.csv', encoding="ISO-8859-1",
    dtype={
        'doctorid': 'string',
        'mainspecialty': 'string',
        'age': 'Int32'
    })
print('doctors.csv loaded in', datetime.now() - now)

now = datetime.now()
clinicsdf = pd.read_csv('clinics.csv', encoding="ISO-8859-1",
    dtype={
        'clinicid': 'string',
        'hospitalname': 'string',
        'isHospital': 'Int32',
        'City': 'string',
        'Province': 'string',
        'RegionName': 'string'
    })
print('clinics.csv loaded in', datetime.now() - now)

now = datetime.now()
pxdf = pd.read_csv('px.csv', encoding="ISO-8859-1", skiprows=[995329], # nrows=50, # Skip annoying extra header on line 995330
    dtype={
        'pxid': 'string',
        'age': 'Int32',
        'gender': 'string'
    })
print('px.csv loaded in', datetime.now() - now)

now = datetime.now()
appointmentsdf = pd.read_csv('appointments.csv', encoding="ISO-8859-1", # nrows=50,
    dtype={
        'pxid': 'string',
        'clinicid': 'string',
        'doctorid': 'string',
        'apptid': 'string',
        'status': 'string',
        'TimeQueued': 'string',
        'QueueDate': 'string',
        'StartTime': 'string',
        'EndTime': 'string',
        'type': 'string',
        'Virtual': 'boolean',
    })
print('appointments.csv loaded in', datetime.now() - now)

"""
CLEANING
"""
now = datetime.now()
# doctor.csv
# Check for duplicates
doctorsdf = doctorsdf.drop_duplicates(subset=['doctorid'], keep='first')

# String transformations
doctorsdf = doctorsdf.replace('\n','', regex=True)
doctorsdf['mainspecialty'] = doctorsdf['mainspecialty'].str.title() # Change to titlecase for readability

# Replace invalid specialties with null
doctorsdf['mainspecialty'] = doctorsdf['mainspecialty'].apply(lambda specialty: specialty if not pd.isnull(specialty) and facts.is_valid_specialty(specialty) else None)

# Replace invalid ages with null
# Youngest doctor ever (17) to older recorded age (122)
doctorsdf.loc[~((doctorsdf['age'] >= 17) & (doctorsdf['age'] <= 122)), 'age'] = None
print('doctorsdf cleaned in', datetime.now() - now)


now = datetime.now()
# clinics.csv
# Check for duplicates
clinicsdf = clinicsdf.drop_duplicates(subset='clinicid', keep='first')

# String transformations
clinicsdf = clinicsdf.replace('\n', '', regex=True)
clinicsdf = clinicsdf.replace('\"', '', regex=True)
clinicsdf['City'] = clinicsdf['City'].str.title()
clinicsdf['City'] = clinicsdf['City'].str.replace(' City', '') # Remove unnecessary 'City' after each name

# Standardize city names and replace invalid with null
clinicsdf = clinicsdf.replace({ 'City': facts.city_dict })
clinicsdf['City'] = clinicsdf['City'].apply(lambda city: city if not pd.isnull(city) and facts.is_valid_city(city) else None)

# Standardize province names and replace invalid with null
clinicsdf = clinicsdf.replace({ 'Province': facts.province_dict })
clinicsdf['Province'] = clinicsdf['Province'].apply(lambda province: province if not pd.isnull(province) and facts.is_valid_province(province) else None)
print('clinicsdf cleaned in', datetime.now() - now)


now = datetime.now()
# px.csv
# Remove unreferenced values
pxdf = pxdf[pxdf['pxid'].isin(appointmentsdf['pxid'])]

# Sort for faster insert
pxdf = pxdf.sort_values('pxid')
print('pxdf cleaned in', datetime.now() - now)

# Check for duplicates
pxdf = pxdf.drop_duplicates(subset='pxid', keep='first')

# Standardize gender
pxdf['gender'] = pxdf['gender'].apply(clean_gender)

# Replace invalid ages with null
pxdf.loc[~((pxdf['age'] >= 0) & (pxdf['age'] <= 122)), 'age'] = None # Oldest Recorded Age


now = datetime.now()
# appointments.csv
# Remove unreferenced values
appointmentsdf = appointmentsdf[appointmentsdf['pxid'].isin(pxdf['pxid'])]
appointmentsdf = appointmentsdf[appointmentsdf['doctorid'].isin(doctorsdf['doctorid'])]
appointmentsdf = appointmentsdf[appointmentsdf['clinicid'].isin(clinicsdf['clinicid'])]
print('appointmentsdf cleaned in', datetime.now() - now)

# Drop input without queuedate
appointmentsdf = appointmentsdf.dropna(subset=['QueueDate'])

# Sort for faster insert
appointmentsdf = appointmentsdf.sort_values('apptid')

# Check for duplicates
appointmentsdf = appointmentsdf.drop_duplicates(subset='apptid', keep='first')

# Fill boolean column with false if empty
appointmentsdf['Virtual'] = appointmentsdf['Virtual'].fillna(False)

# Regex replace timestamp to match MySQL (find a '.' and remove that and all digits after)
appointmentsdf['TimeQueued'] = appointmentsdf['TimeQueued'].replace(r'\.\d+', '', regex=True)
appointmentsdf['QueueDate'] = appointmentsdf['QueueDate'].replace(r'\.\d+', '', regex=True)
appointmentsdf['StartTime'] = appointmentsdf['StartTime'].replace(r'\.\d+', '', regex=True)
appointmentsdf['EndTime'] = appointmentsdf['EndTime'].replace(r'\.\d+', '', regex=True)

"""
LOADING TO MYSQL
"""
doctorsdf.to_sql('dim_doc', con=engine, if_exists='append', index=False)
clinicsdf.to_sql('dim_clinic', con=engine, if_exists='append', index=False)
pxdf.to_sql('dim_px', con=engine, if_exists='append', index=False)
appointmentsdf.to_sql('fact_appt', con=engine, if_exists='append', index=False)

# Close Connection to mySQL
engine.dispose()