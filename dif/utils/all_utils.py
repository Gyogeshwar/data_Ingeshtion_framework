from pyspark.sql.functions import *
import mysql.connector
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import (
    ClientAuthenticationError,
    ResourceNotFoundError,
    ServiceRequestError
)

class AllUtils:

      def __init__(self,source):
          self.source = source

      def connect_to_database(host, user, password, database):
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
          try:
              # Create a BlobServiceClient object
              blob_service_client = BlobServiceClient.from_connection_string(connection_string)
              print("Successfully connected to Azure Storage account.")
              return blob_service_client
          except ClientAuthenticationError as e:
              # Handle authentication errors
              print(f"Authentication error: {e}")
          except ResourceNotFoundError as e:
              # Handle resource not found errors
              print(f"Resource not found error: {e}")
          except ServiceRequestError as e:
              # Handle service request errors
              print(f"Service request error: {e}")
          except Exception as e:
              # Handle any other unexpected exceptions
              print(f"An unexpected error occurred: {e}")
          return None  # Return None if an exception occurs
