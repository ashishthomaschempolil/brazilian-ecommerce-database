import database as db
import transform.transform as transform


def check_if_table_exists(table_name):
    """
    Check if a table exists in the database
    """
    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
    return db.execute_query(query)

def create()->None:
    """
    Funciton to call all the functions to create the tables in the databases if it does not exist
    """
    
    

def main(mode: ):

