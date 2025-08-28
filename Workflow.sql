-- CREATING MASTER TABLES

CREATE TABLE master_leads(
	lead_id NVARCHAR(255) NULL,
	name NVARCHAR(100) NULL,
	phone_number VARCHAR(30) NOT NULL,
	email NVARCHAR(250) NULL,
	proptype NVARCHAR(50) NOT NULL,
	lead_source NVARCHAR(50) NULL,
	budget BIGINT NOT NULL,
	date_enquired DATE NOT NULL,
	lead_status NVARCHAR(100) NULL,
	sale_date DATE  NULL,
	location NVARCHAR(100) NULL,
	agent_name NVARCHAR(100) NULL,
	source_file NVARCHAR(250) NULL
)


CREATE TABLE raw_leads (
    lead_id NVARCHAR(255) NULL,      -- from Excel
    name NVARCHAR(200) NULL,
    phone_number NVARCHAR(50) NULL,
    email NVARCHAR(320) NULL,
    proptype NVARCHAR(100) NULL,
    lead_source NVARCHAR(100) NULL,
    price_range NVARCHAR(100) NULL,
    date_enquired DATE NULL,
    status NVARCHAR(100) NULL,
	sale_date DATE NULL,
	location NVARCHAR(200),
	agent_name NVARCHAR(50) NULL,
	source_file NVARCHAR(100) NULL
)

CREATE TABLE leads_rejected(
	lead_id NVARCHAR(255) NULL,      -- from Excel
    name NVARCHAR(200) NULL,
    phone_number NVARCHAR(50) NULL,
    email NVARCHAR(320) NULL,
    proptype NVARCHAR(100) NULL,
    lead_source NVARCHAR(100) NULL,
    budget NVARCHAR(100) NULL,
    date_enquired DATE NULL,
    lead_status NVARCHAR(100) NULL,
	sale_date DATE NULL,
	location NVARCHAR(200) NULL,
	agent_name NVARCHAR(50) NULL,
	source_file NVARCHAR(100) NULL,
)
-----------------------------------------------------------------------------------------

-- IMPORT EXCEL FILES INTO 'raw_leads' TABLE

-- UPDATE SOURCE FILE AND AGENT'S NAME AFTER EACH IMPORT

-----------------------------------------------------------------------------------------

UPDATE raw_leads                             
SET source_file = N'agent1_leads.xlsx'             
WHERE source_file IS NULL 

UPDATE raw_leads 
SET agent_name = N'Vishal'  
WHERE source_file ='agent1_leads.xlsx' AND (agent_name IS NULL OR agent_name='')

-----------------------------------------------------
UPDATE raw_leads 
SET source_file = N'agent2_leads.xlsx' 
WHERE source_file IS NULL

UPDATE raw_leads 
SET agent_name = N'Sakshi'  
WHERE source_file ='agent2_leads.xlsx' AND (agent_name IS NULL OR agent_name='')

-----------------------------------------------------

UPDATE raw_leads 
SET source_file = N'agent3_leads.xlsx' 
WHERE source_file IS NULL 

UPDATE raw_leads 
SET agent_name = N'Gaurav'  
WHERE source_file ='agent3_leads.xlsx' AND (agent_name IS NULL OR agent_name='')


-----------------------------------------------------------------------------------------

-- Normalization View( Clean Phone, Map text, Fix date and price range)

IF OBJECT_ID('dbo.raw_leads_normalized') IS NOT NULL
    DROP VIEW dbo.raw_leads_normalized

GO
CREATE OR ALTER VIEW dbo.raw_leads_normalized 
AS
WITH trimmed AS(
		SELECT
	LTRIM(RTRIM(lead_id)) AS lead_id_raw,
    LTRIM(RTRIM(name)) AS name_raw,
    LTRIM(RTRIM(phone_number))AS phone_raw,
    LOWER(LTRIM(RTRIM(email))) AS email_raw,
    LOWER(LTRIM(RTRIM(proptype))) AS ptype_raw,
    LOWER(LTRIM(RTRIM(status))) AS status_raw,
    LOWER(LTRIM(RTRIM(lead_source))) AS lead_source_raw,
    LTRIM(RTRIM(price_range))   AS pricerange_raw,
    LTRIM(RTRIM(date_enquired)) AS date_created_raw,
    LTRIM(RTRIM(agent_name)) AS agent_name,
	LTRIM(RTRIM(sale_date)) AS sale_date_raw,
	LOWER(LTRIM(RTRIM(location))) AS location_raw,
    source_file
  FROM raw_leads
 ),

  cleaned AS (
	SELECT *,
	CAST(
		REPLACE(REPLACE(REPLACE(REPLACE(LEFT(pricerange_raw, 
			CASE WHEN CHARINDEX('-', pricerange_raw) > 0
				 THEN CHARINDEX('-', pricerange_raw) - 1
				 WHEN CHARINDEX('to', pricerange_raw) > 0
				 THEN CHARINDEX('to', pricerange_raw) - 1
				 ELSE Len(pricerange_raw)
			END
	    ), 
			NCHAR(8377), ''),
			'L', '00000'),
			',', ''), 
			' ', '') AS BIGINT) AS min_value,
	CAST(
		REPLACE(REPLACE(REPLACE(REPLACE(SUBSTRING(pricerange_raw,
			CASE WHEN CHARINDEX('-', pricerange_raw) > 0
				 THEN CHARINDEX('-', pricerange_raw) + 1
				 WHEN CHARINDEX('to', pricerange_raw) > 0
				 THEN CHARINDEX('to', pricerange_raw) + 2
				 ELSE 1 
			END, LEN(pricerange_raw)
		),
			',', ''),
			' ', ''),
			'L', '00000'),
			NCHAR(8377), '') AS BIGINT) AS max_value,
	CONVERT(DATE, date_created_raw) AS date_enquired,
	CONVERT(DATE, sale_date_raw) AS sale_date,
	REPLACE(REPLACE(status_raw, '-', ''), 'pending', '') AS lead_status,
	REPLACE(lead_source_raw, '-', '') AS lead_source
  FROM trimmed
 ),
  mapped AS (
    SELECT *, 
	(min_value + max_value)/2 AS budget
  FROM cleaned
)
SELECT 
lead_id_raw,
name_raw,
phone_raw,
email_raw,
ptype_raw,
lead_source,
budget,
date_enquired,
lead_status,
sale_date,
location_raw,
agent_name,
source_file
FROM mapped
GO
	
-----------------------------------------------------------------------------------------
-- DATA VALIDATION CHECKIN FOR NULLS

CREATE OR ALTER VIEW required_details AS
	SELECT * FROM raw_leads_normalized
	WHERE name_raw IS NULL OR name_raw = ''
	OR phone_raw IS NULL OR phone_raw = ''
	OR date_enquired IS NULL OR date_enquired = ''
	OR ptype_raw IS NULL OR ptype_raw = ''
	OR budget IS NULL OR budget = ''

-----------------------------------------------------------------------------------------
--INSERT NULL ROWS INTO ANOTHER TABLE leads_rejected FOR UPDATING 

INSERT INTO leads_rejected
	(lead_id, name, phone_number, email, proptype, lead_source, budget,
	 date_enquired, lead_status, sale_date,location, agent_name, source_file)
SELECT * FROM required_details




-----------------------------------------------------------------------------------------
-- Sent Rejected leads into and excel file to be updated by agents later

TRUNCATE TABLE leads_rejected

-----------------------------------------------------------------------------------------
-- DELETE REJETCTED LEADS FROM CLEANED AND NORMALISED VIEW 

DELETE FROM raw_leads_normalized
WHERE lead_id_raw IN (SELECT lead_id_raw FROM required_details)


-----------------------------------------------------------------------------------------
-- UPSERT AND DE-DUP LEADS INTO MASTER TABLE

WITH remove_duplicates AS(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY lead_id_raw ORDER BY lead_id_raw) as row_num
	FROM raw_leads_normalized
)
INSERT INTO master_leads
	(lead_id, name, phone_number, email, proptype, lead_source, budget,
	 date_enquired, lead_status, sale_date,location, agent_name, source_file)
SELECT 
	lead_id_raw,
	name_raw,
	phone_raw,
	email_raw,
	ptype_raw,
	lead_source,
	budget,
	date_enquired,
	lead_status,
	sale_date,
	location_raw,
	agent_name,
	source_file
FROM remove_duplicates
WHERE row_num = 1
	
SELECT * FROM master_leads
-----------------------------------------------------------------------------------------
-- MARKING ALL THE ROWS WHICH ARE PROCESSED = 1

TRUNCATE TABLE raw_leads

-----------------------------------------------------------------------------------------
-- Daily Process
--1. Import New Leads

--Receive Excel files from agents.

--Load them into raw_leads table

--Update source_file and agents_name columns.

--2. Normalize & Clean

--Use raw_leads_normalized view to standardize phone, email, property type, date, and budget.

--This step ensures consistent formatting for all leads.

--3. Validate & Reject Invalids

--Use required_details view to find rows with missing name, phone, date, property type, or budget.

--Insert these rows into leads_reject table.

--delete them from raw_leads_normalized and send back to update.

--4. Deduplicate

--Remove duplicate leads (based on lead_id_raw ) so only one clean record per lead remains.

--5. Insert into Master Table

--Insert the cleaned, deduplicated leads into master_leads.

--Exclude any leads present in leads_reject.

--6. Drop rows from raw_leads to make it ready for fresh data






