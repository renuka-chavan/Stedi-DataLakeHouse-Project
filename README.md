# STEDI Human Balance Analytics

In this project, as a data engineer for the STEDI team we will build a data lakehouse solution for sensor data that trains a machine learning model.

The solution for this data lakehouse is we will convert raw sensor data into Landing, Trusted and Curated Zones.

## Project Directory

- `/glue job scripts` - AWS Glue generated ETL spark job python scripts. 

- `/sql` - AWS Athena generated SQL DDL queries for tables.

- `/starter` - This folder should contain public project starter code.

- `/screenshots` - Screenshots of output queries result in Athena.

- `/images` - Flowchart, dataset for reference 

- `/README.md` - Project Documentation.

## Steps to be followed:

### Step 1: Preparing Landing zone :

The Landing zone is created by using S3 Bucket and copy the data there as a starting point. In this we upload the raw data into S3 bucket. 

The following S3 directories:
- accelerometer/landing
- customer/landing
- step_trainer/landing

We will create tables for the raw data customer_landing, accelerometer_landing, and step_trainer_landing using Glue.

Query these tables using Athena.

### Step 2: Preparing Trusted Zone:
 
The Data Science team has found that Accelerometer Records is only for valid customers.

Now we create two AWS Glue Jobs that do the following:

#### 1. Customer Landing -> Customer Trusted
- Customer data from website who agreed to share their data for research purpose.
- Remove those who doesno't share data.
- Output table - customer_trusted

#### 2. Accelerometer Landing -> Accelerometer Trusted

- Sanitize the Accelerometer data from the Mobile App (Landing Zone) 
- Store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) 
- Output table - accelerometer_trusted

Query these tables using Athena.

### Step 3: Preparing Curated Zone

Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased.

Now we create AWS Glue Jobs that do the following:

#### 3. Customer Trusted -> Customer Curated

- Sanitize the Customer data (Trusted Zone). 
- Include unique customers who have accelerometer data and have agreed to share their data for research
- Output table - customers_curated

Query the table using Athena.

### Step 4: Preparing Machine learning Curated

Now we create two AWS Glue Jobs that do the following:

#### 4. Step Trainer Landing -> Step Trainer Trusted

- Step Trainer IoT data stream (S3) and populate a Trusted Zone
- Include Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research.
- Output table - step_trainer_trusted

#### 5. Step Trainer Trusted -> Accelerometer Trusted

- Step Trainer and accelerometer reading data for same timestamp, for customers who have agreed to share their data

- Output table - machine_learning_curated.

Query these tables using Athena.

## Project Summary 

As a data engineer on the STEDI Step Trainer team, we have extracted the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the machine learning model.


