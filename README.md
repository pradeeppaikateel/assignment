# Postman Assignment
 Assignment for data engineer role at Postman India,
 
##Table of content


## Introduction
The instruction given in the assignment was to build a non-blocking parallel processing ETL pipeline that would ingest
data from a .csv file, process the data and load to a database table, following which the the data from the products
table needs to be aggregated and further inserted into another table. I have used **spark**, with scripts written in 
pyspark, as my ETL tool and have used PostGres as the target database.  

###Following were the points to achieve 
- Code should follow concept of OOPS
- Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale 
  of what should happen if the file is to be processed in 2 mins.
- Support for updating existing products in the table based on `sku` as the primary key.
- All product details are to be ingested into a single table
- An aggregated table on above rows with `name` and `no. of products` as the columns

###Assumptions made during the development of the ETL framework are as follows :
- No column was provided with input data in products.csv that would allow one to identify the latest update record for a
  particular `sku` value, thus a `request_id` column has been derived in spark, which is a concatenation of timestamp 
  and row number. The assumption that is made that every data that is input would natively have its own timestamp based 
  on which you can filter out only the latest values.
- One of the other assumptions or inferences that has been made is that it's been expected that update should happen on
target table treating `sku` like a primary key i.e only one record of a `sku` value can exist, but this does not 
  necessarily imply that column `sku` should be made the primary key in the table
  
##Folder Structure
### A typical top-level directory layout

    .
    ├── build                   # Compiled files (alternatively `dist`)
    ├── docs                    # Documentation files (alternatively `doc`)
    ├── src                     # Source files (alternatively `lib` or `app`)
    ├── test                    # Automated tests (alternatively `spec` or `tests`)
    ├── tools                   # Tools and utilities
    ├── LICENSE
    └── README.md
## Setting up the Environment
There are three containers in total that host the whole framework on docker. Each for spark, postgresql and pgadmin4.
###Requirements: 
- Docker
- Docker Compose
- Git

1. Clone the code base from this github repo using command


  ```
  git clone https://github.com/pradeeppaikateel/postman-assignment.git 
  ```
2. Change directory to folder `postman-assignment`
```
cd postman-assignment
```
**NOTE: All commands in cmd/terminal are to be run from this directory**

As this framework is built in a windows machine, I have created a batch file that would execute all initial commands.

3. Execute the following command to run the batch file in windows cmd
```
new.bat
```
the batch file contains the following commands that can be run individually **if needed**
```
docker build -t pradeep/spark -f Dockerfile .
docker build -t pradeep/pgadmin4 -f Dockerfile2 .
docker network inspect develop >$null 2>&1 || docker network create --subnet=172.18.0.0/16 develop
docker start postgres > $null 2>&1 || docker run --name postgres --restart unless-stopped -e POSTGRES_PASSWORD=postgres --net=develop --ip 172.18.0.22 -e PGDATA=/var/lib/postgresql/data/pgdata -v /opt/postgres:/var/lib/postgresql/data -p 5432:5432 -d postgres:11
docker-compose up -d
docker run -p 5050:80 --volume=pgadmin4:/var/lib/pgadmin -e PGADMIN_DEFAULT_EMAIL=postman@sample.com -e PGADMIN_SERVER_JSON_FILE=/pgadmin4/servers.json -e PGADMIN_DEFAULT_PASSWORD=postman --name="pgadmin4" --hostname="pgadmin4" --network="develop" --detach pradeep/pgadmin4
```
4. Once the above commands are successful , the command given below can be used to submit the pyspark script to trigger the ETL job from local windows machine cmd
```
docker exec spark-master spark-submit --master spark://spark:7077 --py-files /opt/bitnami/spark/postman-assignment/code.zip /opt/bitnami/spark/postman-assignment/jobs/etl_job.py --source=PRODUCTS --env=dev --job_run_date=2021-08-07T02:00:00+00:00
```



