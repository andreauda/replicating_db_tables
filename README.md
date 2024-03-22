# Replicating DB Tables

With this script you can duplicate tables from Postgres to Redshift.
In particular, we replicated tables from Tableau Server.

The code includes classes to connect to the database, execute queries, load data, and more.

## Dependencies

The dependencies for this project are listed in the `requirements.txt` file. To install them, run:
pip install -r requirements.txt


## Configuration

Make sure you have the correct credentials to access PostgreSQL and Amazon Redshift databases. Credentials must be provided via separate Python files for security.

### PostgreSQL credentials

Create a `credentials/postgres.py` file with the following variables:

```python
hostname = 'hostname'
database = 'database'
username = 'username'
pas = 'password'
port_id = 'port'
```

The same holds for Redshift
