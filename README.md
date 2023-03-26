# Brazilian E-Commerce

This project's objective is to provide a POSTGRESQL database with a set of tables that can be used to analyze the brazilian e-commerce market. 

Data source is obtained from [Kaggle](https://www.kaggle.com/olistbr/brazilian-ecommerce).

Another data source which should be named as `cities.csv` is obtained from this [repository](https://github.com/datasets-br/city-codes/tree/master/data) (`br-city-code.csv`). 

All the csv files are stored in the `data/raw` folder.

## Data Model

This is the ERD diagram of the POSTGRESQL database that was created after preprocessing the data.

![Data Model](assets/DataBase-ERD-Diagram.png)
## Running the project

To run the project, follow these steps:

1. Download or clone the project from the GitHub repository.
2. Navigate to the project root directory in your terminal.
3. Create a .env file in the project root directory and copy the following information into it:
    ```bash
    POSTGRES_DB=brazil
    POSTGRES_USER=main
    POSTGRES_PASSWORD=password
    POSTGRES_PORT=5432
    POSTGRES_HOST=host.docker.internal
    PGADMIN_DEFAULT_EMAIL=exampleuser@example.com
    PGADMIN_DEFAULT_PASSWORD=password
    ```
4. In the terminal, run the command `docker-compose up` to build and start the Docker containers.
5. Once the Docker containers are up and running, you can analyze the PostgreSQL database by opening the pgAdmin client in your web browser and signing in using the default email and password specified in the .env file.
6. To connect the pgAdmin client to the PostgreSQL server, use the host name address obtained by running the command `docker inspect postgresql | grep "IPAddress"` in your terminal. Use the other information from the .env file to complete the server connection details.


## Project Structure

```
.
├── assets
│   └── DataBase-ERD-Diagram.png
├── data
│   ├── preprocessed
│   └── raw
├── database
│   ├── utils
│   │   └── __init__.py
|   |   └── alter_tables_query.py
|   |   └── create_tables_query.py
│   ├── create_database.sql
│   └── create_db.py
├── load
│   ├── __init__.py
│   └── insert_db.py
├── transform
│   ├── transform.py
│   └── utils.py
├── .env
├── .gitignore
├── Dockerfile
├── README.md
├── analysis.py
├── docker-compose.yml
├── main.py
└── requirements.txt

```