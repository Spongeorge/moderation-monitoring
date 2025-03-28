# Installation
To install the project requirements, run
```
pip install -r requirements.txt
```

To run tasks concurrently, Airflow's metadata database will need to be replaced with either Postgres or MySQL. To do this you will need to host one of these, locate `airflow.cfg` in your Airflow directory and set
```
executor = LocalExecutor
```
and update the database connection string to your new database:
```
sql_alchemy_conn = [DB CONNECTION STRING HERE]
```

Next, to initialize the database
```
airflow db init
```

and set up a user
```
airflow users create -r Admin -f FirstName -l LastName -e email@domain.com -u username -p password
```

Once airflow has been installed, the files in the `dags` folder of this repo should be moved to your airflow installation's dags folder.

## Setting up MongoDB

By default, the DAG assumes MongoDB will be running on `http://localhost:27017/`.

Instructions for installing and running MongoDB can be found here: [https://www.mongodb.com/docs/manual/installation/](https://www.mongodb.com/docs/manual/installation/).

# Running the DAG
Once all of the requirements are installed, run the scheduler and the webserver:
```
airflow scheduler
```

```
airflow webserver
```

at which point you should be able to access airflow at [http://localhost:8080/](http://localhost:8080/)


# Optional: Grafana/Prometheus Monitoring Stack
[TODO]
