# Transaction Processor
- Tech
- Approach
- Docker

## Tech
  - JDK 1.8
  - maven
  - Scala 2.4.6
  - Spark 3.0.1 (Docker container)
  
## Approach
 This spark application reads the data from the respective directory & processes the transaction data , have used dockerisation in order to run the Jobs over the container.
 
 STEP 1
- Account Data :  As  only the transaction data is needed ,  filtered only the data which is relevant , ie i have used only 'u' update operation data  as it contains the linked accounts for savings & credit cards .
- Credit Card : Credit information also is available in update operations only , so filtering the same gives the transactions .
- Savings Account : Savings also is avaialble in update operations only , so filtering the same gives the transactions in savings account.

STEP 2    
- In order to get the transaction info first we need to get the 'u' operation data from Accounts (ie accounts linked ) and Join the same with the 'c' create operation in savings / credit cards which helps to get the db_id  for the respective transaction in respect to the credit / saving account.
   eg : Accounts Data 

    | id |op |ts|set|data
    |------| -| ------ | ------ |-
    | a1globalid |u| 1577890800000|"set": {"savings_account_id": "sa1"}
    | a1globalid |u| 1577890800000|""set": {"card_id": "c1"}
        
    Credit Data 
         
    | id |op |ts|set|data
    |------| -| ------ | ------ |-
    | c1globalid |c| 1577926800000| |"data": {"card_id": "c1","card_number": "11112222","credit_used": 0}
    
    Once data is joined on the "c" of Credit card (c1) then we get "c1globalid" which can be used to fetch the conscutive transactions.
    | id |op |ts|set|data
    |------| -| ------ | ------ |-
    | c1globalid |u| 1578313800000|"set": {"credit_used": 12000} |
    | c1globalid |u| 1578420000000|"set": {"credit_used": 19000} |
    | c1globalid |u| 1578420000000|"set": {"credit_used": 0} |

STEP 3
- After fetching the respective transactions for Savings & Credit accounts i processed them individually and used Windowing function and applied lag function on the same to calculate the transaction amount , below th diff column shows the transaction amounts.
      
    | accountId|bank_acc_id|cr_trn_amt|transaction_time|account_type|sv_trn_amt|      freindly_date|lagCol|  diff|
    |----------|-----------|----------|----------------|------------|----------|-------------------|------|------|
        |a1globalid| c1globalid|     12000|   1578313800000| credit_card|         0|2020-01-06 20:30:00|  null| 12000|
        |a1globalid| c1globalid|     19000|   1578420000000| credit_card|         0|2020-01-08 02:00:00| 12000|  7000|
        |a1globalid| c1globalid|         0|   1578654000000| credit_card|         0|2020-01-10 19:00:00| 19000|-19000|
    
Step 4
 Same steps are carried for Savings account as well , and then data is clubed together to get the transaction details , and this transaction details are written as Csv output.
 
 ## Build
  ```sh 
1. git checkout 
```  
```sh 
2. cd assignment
```  
```sh
3.  mvn clean install -DskipTests
```
```sh
4. cd assignment/target/
``` 
 - In step 4  this  {}/apps path will be mapped as volume -> /opt/spark-apps in docker 

```sh
5.  cp assignment-1.0-SNAPSHOT-jar-with-dependencies.jar ../../apps 
``` 
 - In step 5  this  {}/data path will be mapped as volume -> /opt/spark-data in docker , as this folder contains transaction data , or you can add more data here .
 ```sh
6. cd ../../data/
``` 
  -  Run Docker following below section (Docker)
  ```sh
   7. cd ..
``` 
    
- After docker is Up  , run the below command
 ```sh    
  8.    docker  exec -it   docker-spark-cluster_spark-master_1 /opt/spark/bin/spark-submit --deploy-mode client  --master spark://spark-master:7077 --num-executors 1 --total-executor-cores 1 --executor-cores 1 --executor-memory 1g --driver-memory 1g --class transactions.TransactionProcessor  /opt/spark-apps/assignment-1.0-SNAPSHOT-jar-with-dependencies.jar
 ```  
- After running the above command , output folder will be created in the {}/data which is the volume mapped in step 5
 ```sh   
9. cd data/output
 ```      
 ## Docker
   
A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create the following containers:

container|Exposed ports
---|---
spark-master|9090 7077
spark-worker-1|9091

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites
* Docker installed
* Docker compose  installed

## Build the image
```sh
docker build -t cluster-apache-spark:3.0.2 .
```

## Run the docker-compose
The final step to create your test cluster will be to run the compose file:
```sh
docker-compose up -d
```

## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master
http://localhost:9090/

### Spark Worker 1
http://localhost:9091/


# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart ,
this will help to make changes to the jar from outside the container  as well as changes to transaction data .

local Mount|Container Mount|Purposse
---|---|---
{}/apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
{}/data|/opt/spark-data| Used to make available your app's data on all workers & master
{}/data/output|/opt/spark-data/output| Used to make available your app's data from container to local machine
