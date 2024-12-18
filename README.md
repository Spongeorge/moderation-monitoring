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

# Running the DAG
Once all of the requirements are installed, run the scheduler and the webserver:
```
airflow scheduler
```

```
airflow webserver
```

at which point you should be able to access airflow at [http://localhost:8080/](http://localhost:8080/)
