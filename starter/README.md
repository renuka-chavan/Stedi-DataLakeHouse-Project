# Purpose of this Folder

This folder should contain public project starter code.

The S3 bucket have the following directory structure:

```
customer/
- landing/
- trusted/
- curated/
accelerometer/
- landing/
- trusted/
step_trainer/
- landing/
- trusted/
- curated/
```

**Note:** `step_trainer/curated/` contains the data files for the `machine_learning_curated` table.

## Customer Records
This is the data from fulfillment and the STEDI website.

This file contains the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

## Step Trainer Records
This is the data from the motion sensor.

This file contains the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject


## Accelerometer Records
This is the data from the mobile app.

This file contains the following fields:

- timeStamp
- user
- x
- y
- z
