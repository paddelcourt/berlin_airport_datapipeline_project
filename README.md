# Berlin Airport Data Engineering Project 

This is a project that is part of the data engineering 2024 Zoomcamp Project


# Problem Statement

As someone who is dependent on the Berlin Airport and has flown a decent number of times over the last 2 years, I've witnessed how chaotic flying in Europe can be, and especially at this airport. This airport has been known for multiple delays and issues in both its development
and day to day operations. As a result, I decided to create a project that would 


# Dashboard

[Link](https://lookerstudio.google.com/reporting/d7836225-a780-4303-91ee-855ddbca4f5a)

Note: Due to budgetary and time constraints, the dashboard will only contain data for 2024-02-28 for demoing purposes. Due to the rate limiter of the API, the DAG was designed to collect data only one day, and with cloud costs running high,
it was decided that only one day of data will be shown for demosntration purposes. 

# Design
![Data pipeline design](https://github.com/paddelcourt/berlin_airport_datapipeline_project/blob/main/data_architecture.jpeg)

This project is an end to end project from API to Visual Dashboard. 
1. Terraform is used as "infrastructre as code"
2. Mage is used here throughout the whole ELT process from pulling arrivals and depatrues data from API to Combining Data in Bigquery
3. GCP Cloudfile Store is used as the data lake
4. Bigquery is used as the querying analytic engine
5. Pyspark (Set up on Dataproc) is used as a data transformation tool to combine arrivals and departures 


# Requirements and Installations 
For this project, the requirements will be needed as follows

1. Docker
2. GCS Buckets (GCP)
3. Bigquery (GCP)
4. Mage 
5. Java (Optional for Option B)
6. Apache Spark (Optional for Option B)

Please follow the [tutorials](https://github.com/DataTalksClub/data-engineering-zoomcamp) on weeks 1 (Docker), 2 (Mage) and 5 (Spark) for proper installation and setup as these are quite extensive if the pre-requisite installations have not already been done. Notably for this project, Mage and Pyspark will be used as the primary tools. You can also use the notes made by the students who cover extensively the setup process if text is the preferred form of instruction. 

Additionally, for Mage Orchestration, you will need to install the python packages on requirements.txt for the DAG to run. 


# Setup

## Terraform

Terraform will be used to setup the GCS bucket. To do so, follow these steps:
1. CD into the terraform directory.
2. Apply the correct variables on the `variables.tf` file. This will be used to create the GCS bucket. 
3. On the terminal, intialize terraform by entering `terraform init`.
4. To apply the `main.tf` file, enter `terraform apply`.
5. To destroy the setup, enter `terraform destroy` into the terminal. 

## Mage

### Mage Local Setup

To begin mage, please cd into the `mage-zoomcamp` directory and then do the following:
1. `docker compose build`
2. `docker pull mageai/mageai:latest` (To update to latest mage image)
3. `docker compose up`

Please make sure to have Docker and the container mage-zoom running. Then you can type in your browswer `http://localhost:6789/` to run Mage locally.

Tutorial on Local Mage setup can be found [here](https://www.youtube.com/watch?v=tNiV7Wp08XE&ab_channel=Mage)

### GCP Setup

To connect Mage with GCS, you will need to set up a service account with proper access, and also have GCS credentials prepared for Mage to use. 
Please refer to this [tutorial](https://www.youtube.com/watch?v=00LP360iYvE&list=PL_ItKjYd0DsggZs-aPVsZMkJOOGeHaXge&index=8&ab_channel=Mage) by Mage for the GCS setup. 
Additionally, it may be helpful to view the `ETL: API to GCS` and `ETL: API to Bigquery` for full explanation. 


### Mage Pipeline Setup

The pipelines should all be connected and setup, you will see three pipelines

1. berlin_arrivals_etl_to_gcs_bq: This pipeline pulls the arrivals data from the API and uploads the data as a CSV onto a GCS Bucket and Corresponding BigQuery table.
2. berlin_departures_etl_to_gcs_bq: This pipeline pulls the departures data from the API and uploads the data as a CSV onto a GCS Bucket and Corresponding BigQuery table.
3. berlin_union_departures_arrivals: This pipeline pulls the

   


### Deplyoing to Google Cloud

To run Mage on Google Cloud so it can run autonomously without needing a local machine, you will need to follow the set up by Matt Palmer here:

1. [Google Cloud Permissions](https://www.youtube.com/watch?v=O_H7DCmq2rA&list=PL_ItKjYd0DsggZs-aPVsZMkJOOGeHaXge&index=14&ab_channel=Mage)
2. [Google Cloud Pre-Requisites](https://www.youtube.com/watch?v=zAwAX5sxqsg&list=PL_ItKjYd0DsggZs-aPVsZMkJOOGeHaXge&index=13&ab_channel=Mage)
3. [Deployment Part 1](https://www.youtube.com/watch?v=9A872B5hb_0&list=PL_ItKjYd0DsggZs-aPVsZMkJOOGeHaXge&index=15&ab_channel=Mage)
4. [Deployment Part 2](https://www.youtube.com/watch?v=0YExsb2HgLI&list=PL_ItKjYd0DsggZs-aPVsZMkJOOGeHaXge&index=16&ab_channel=Mage)

Note: You will need to recreate the pipeplines and schedules on the cloud. 


# Contributions and Gratitude 

I would like to thank Jean Loui Bernard Silva de Jesus (JeanExtreme002) for the FlightRadarAPI that was used to pull the flight data. 

Github Repository can be found here: https://github.com/JeanExtreme002/FlightRadarAPI?tab=MIT-1-ov-file

Special thank you to the team at Data Engineering Zoomcamp for organizing this project. They are great folks, and you can check them out here:

1. Github: https://github.com/DataTalksClub/data-engineering-zoomcamp
2. Slack: https://datatalks.club/slack.html

Lastly, I would like to give a shoutout to Mage for partnering with the Zoomcamp and providing a smooth tutorial and experience on using the tool. 

You can find the Mage Zoomcamp here:
https://github.com/mage-ai/mage-zoomcamp

