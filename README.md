# BeerInventoryData

## Developer Setup

* Create a virtual environment named `.venv`
* `pip install -r requirements.txt`
* Create a file `.env` in the root of the project with the form:

  ```
  DB_HOST=<hostname>
  DB_USER=beer
  DB_PASSWORD=<password for beer user>
  DB_NAME=beer
  ```


## Create Database and User

The `beer` database and `beer` user are created manually on the server by the `postgres` user:

* Create the database: `createdb beer`
* Create the user: `createuser --interactive --pwprompt`
* Allow remote access: `nano /etc/postgresql/11/main/pg_hba.conf`  add:

  ```
  host    all             beer            all                     password
  ``` 
* Restart server: `/etc/init.d/postgresql restart`

If configured properly, the `verify_connection.py` script should produce output similar to:

  ```
  ('PostgreSQL 11.5 (Ubuntu 11.5-1.pgdg18.04+1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 7.4.0-1ubuntu1~18.04.1) 7.4.0, 64-bit',)

  SUCCESS!
  ```

## Create and Populate Tables

The code in `src/beer/ingest/db_setup.py` will:

* Delete `inventory`, `products`, `sizes`, and `categories` tables, if present
* Create the four tables mentioned above


The code in `src/beer/ingest/ingest.py` will:

* Require a folder name as its parameter.  This folder should contain all the CSV files.
* Read each file and:
  * Add entries to `producdts`, `sizes` and `categories`, whenever a new value is found
  * Convert the name, size, and category to the values in the corresponding tables
  * For each row in the file, add a row to the `inventory` table
