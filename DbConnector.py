import mysql.connector as mysql
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.pooling import PooledMySQLConnection


class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(
        self,
        HOST: str = "tdt4225-xx.idi.ntnu.no",
        DATABASE: str = "DATABASE_NAME",
        USER: str = "TEST_USER",
        PASSWORD: str = "test123",
    ):
        self.db_connection: PooledMySQLConnection | MySQLConnection
        self.cursor: MySQLCursor

        # Connect to the database
        try:
            self.db_connection = mysql.connect(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306
            )
        except mysql.Error as e:
            print("ERROR: Failed to connect to db:", e)
            raise

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        _ = self.cursor.execute("SELECT DATABASE();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self) -> None:
        # close the cursor
        _ = self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
