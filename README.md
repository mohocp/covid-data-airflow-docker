# Genearte COVID 19 Reports for UK, AND Move Date from Postgresql to MongoDB using Airflow and Docker

this Repository contains Docker-compose for Airflow, Postgresql, pgAdmin, JupyterLab and MongoDB. also have Airflow dags, notebooks for Genearte COVID 19 Reports for UK and Move data from Postgres to MongoDB

## Prerequisites 
* Install Python 3.6 or above
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)

## Installation
Clone the Repository and navigate to docker-airflow-assigment-two directory<br>
Then run the docker-compose file <br><br>
`docker-compose up -d`

## Files
* The Dags located in <b>./notebooks/src</b> directory
* The generated reports located in  <b>./notebooks/output</b> directory
* The Jupyter notebooks located in <b>./notebooks</b> directory

## Usage
* Airflow => [localhsot:8080](http://localhsot:8080)
* JupyterLab => [localhsot:8888](http://localhsot:8888)
* PGAdmin => [localhsot:8888](http://localhsot:80)

### Genearte COVID 19 Reports for UK
* Using PGAdmin create `Covid_DB` database
* Using Airflow Open `covid_data` DAG
* Tragger the DAG manually
* The following output will be genareted as following: 1- `uk_scoring_report.png`, `uk_scoring_report.csv` and `uk_scoring_report_NotScaled.csv`


### Move data from Postgres to MongoDB
* Using PGAdmin create `Faker_DB` database
* Create Table with any data
* Tragger the DAG manually
* the Output will be a collection with the data in MongoDB

## Information
The description for each process in both workflows as the following

### Genearte COVID 19 Reports for UK
The following operation used to achieve the purpose <br>
* **Get_uk_data** Load all the data from [Johns Hopkins University Data from Github Repo](https://github.com/CSSEGISandData/COVID-19) and store it in Postgrss with clean process applied and filter the data
* **report_data** The result of proccess genearted and located on ./notebooks/output directory

### Move Data from Postgrs to MongoDB
The following operation used to achieve the purpose <br>
* **extract_load** extract the data using pandas from and dump it to mongoDB
