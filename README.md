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

## Data Wrangling 

There are currently 3 scripts: 

- **GenerateKeys** will loop through every beer data file and create key files for unique names, sizes and categories.
- **UpdateKeys** creates another file for sizes that don't include information such as deposites (but this script is not used now).
- **RewriteFiles** loops through every beer data file and will convert everything to a numeric value using the key files and epoch time converter, then rewrites to new files.


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

The code in `db_setup.py` will:

* Delete `inventory`, `products`, `sizes`, and `categories` tables, if present
* Create the four tables mentioned above
* Populate `products`, 'sizes', and 'categories' using the files created by `GenerateKeys.py`
* Populate `inventory` from data in the files created by `RewriteFiles.py`
