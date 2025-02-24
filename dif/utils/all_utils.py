from pyspark.sql.functions import *
import mysql.connector
from azure.storage.blob import BlobServiceClient

class AllUtils:

      def __init__(self,source):
          self.source = source

      def connect_to_database(host, user, password, database):
    """
    Connects to a MySQL database and returns the connection and cursor objects.

    Args:
        host (str): The hostname of the MySQL server.
        user (str): The username to connect with.
        password (str): The password for the user.
        database (str): The name of the database to connect to.

    Returns:
        tuple: A tuple containing the connection object and the cursor object.
               Returns (None, None) if the connection fails.
    """
    try:
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        mycursor = mydb.cursor()
        return mydb, mycursor
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None, None

     def connect_to_azure_storage(connection_string):
    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    print("Successfully connected to Azure Storage account.")
    return blob_service_client

