-- SCHEMA.Drop
DROP SCHEMA IF EXISTS seriousmd;

-- SCHEMA.Create
CREATE SCHEMA IF NOT EXISTS seriousmd;

-- TABLE.Create
-- Doctors
CREATE TABLE IF NOT EXISTS seriousmd.dim_doc (
	doctorid		VARCHAR(32) NOT NULL UNIQUE,
	mainspecialty	TEXT,
	age				INT UNSIGNED,
    PRIMARY KEY (doctorid)
);

-- Clinics
CREATE TABLE IF NOT EXISTS seriousmd.dim_clinic (
	clinicid	 VARCHAR(32) NOT NULL UNIQUE,
	hospitalname TEXT,
	IsHospital	 BOOLEAN,
	City		 TEXT,
	Province	 TEXT,
	RegionName	 TEXT,
    PRIMARY KEY (clinicid)
);

-- Px
CREATE TABLE IF NOT EXISTS seriousmd.dim_px (
	pxid   VARCHAR(32) NOT NULL UNIQUE,
	age	   INT UNSIGNED,
	gender ENUM('MALE',
                 'FEMALE'),
    PRIMARY KEY (pxid)
);

-- Appointments
CREATE TABLE IF NOT EXISTS seriousmd.fact_appt (
	pxid	 VARCHAR(32) NOT NULL,
	clinicid VARCHAR(32) NOT NULL,
	doctorid VARCHAR(32) NOT NULL,
	apptid	 VARCHAR(32) NOT NULL UNIQUE,
	status	 ENUM('Cancel', 'Complete', 'NoShow',
				  'Queued', 'Serving' , 'Skip') NOT NULL,
	TimeQueued DATETIME,
	QueueDate  DATETIME,
	StartTime  DATETIME,
	EndTime    DATETIME,
	type	   ENUM('Consultation', 'Inpatient') NOT NULL,
	`Virtual`  BOOLEAN,
    PRIMARY KEY (apptid),
    FOREIGN KEY (pxid) 	   REFERENCES seriousmd.dim_px(pxid),
    FOREIGN KEY (clinicid) REFERENCES seriousmd.dim_clinic(clinicid),
    FOREIGN KEY (doctorid) REFERENCES seriousmd.dim_doc(doctorid)
);