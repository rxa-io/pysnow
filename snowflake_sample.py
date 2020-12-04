import logging
import os
import sys


# -- (> ---------------------- SECTION=import_connector ---------------------
import snowflake.connector
# -- <) ---------------------------- END_SECTION ----------------------------


class PythonVeritasBase:
    """
    PURPOSE:
        This is the Base/Parent class for programs that use the Snowflake
        Connector for Python.
        This class is intended primarily for:
            * Sample programs, e.g. in the documentation.
            * Tests.
    """

    def __init__(self, p_log_file_name = None):

        """
        PURPOSE:
            This does any required initialization steps, which in this class is
            basically just turning on logging.
        """

        file_name = p_log_file_name
        if file_name is None:
            file_name = '/tmp/snowflake_python_connector.log'

        # -- (> ---------- SECTION=begin_logging -----------------------------
        logging.basicConfig(
            filename=file_name,
            level=logging.INFO)
        # -- <) ---------- END_SECTION ---------------------------------------

    # -- (> ---------------------------- SECTION=main ------------------------
    def main(self, argv):

        """
        PURPOSE:
            Most tests follow the same basic pattern in this main() method:
               * Create a connection.
               * Set up, e.g. use (or create and use) the warehouse, database,
                 and schema.
               * Run the queries (or do the other tasks, e.g. load data).
               * Clean up. In this test/demo, we drop the warehouse, database,
                 and schema. In a customer scenario, you'd typically clean up
                 temporary tables, etc., but wouldn't drop your database.
               * Close the connection.
        """

        # Read the connection parameters (e.g. user ID) from the command line
        # and environment variables, then connect to Snowflake.
        connection = self.create_connection(argv)

        # Set up anything we need (e.g. a separate schema for the test/demo).
        self.set_up(connection)

        # Do the "real work", for example, create a table, insert rows, SELECT
        # from the table, etc.
        self.do_the_real_work(connection)

        # Clean up. In this case, we drop the temporary warehouse, database, and
        # schema.
        self.clean_up(connection)

        print("\nClosing connection...")
        # -- (> ------------------- SECTION=close_connection -----------------
        connection.close()
        # -- <) ---------------------------- END_SECTION ---------------------

    # -- <) ---------------------------- END_SECTION=main --------------------

    def args_to_properties(self, args):

        """
        PURPOSE:
            Read the command-line arguments and store them in a dictionary.
            Command-line arguments should come in pairs, e.g.:
                "--user MyUser"
        INPUTS:
            The command line arguments (sys.argv).
        RETURNS:
            Returns the dictionary.
        DESIRABLE ENHANCEMENTS:
            Improve error detection and handling.
        """

        connection_parameters = {}

        i = 1
        while i < len(args) - 1:
            property_name = args[i]
            # Strip off the leading "--" from the tag, e.g. from "--user".
            property_name = property_name[2:]
            property_value = args[i + 1]
            connection_parameters[property_name] = property_value
            i += 2

        return connection_parameters

    def create_connection(self, argv):

        """
        PURPOSE:
            This connects gets account and login information from the
            environment variables and command-line parameters, connects to the
            server, and returns the connection object.
        INPUTS:
            argv: This is usually sys.argv, which contains the command-line
                  parameters. It could be an equivalent substitute if you get
                  the parameter information from another source.
        RETURNS:
            A connection.
        """

        # Get account and login information from environment variables and
        # command-line parameters.
        # Note that ACCOUNT might require the region and cloud platform where
        # your account is located, in the form of
        #     '<your_account_name>.<region>.<cloud>'
        # for example
        #     'xy12345.us-east-1.azure')
        # -- (> ----------------------- SECTION=set_login_info ---------------

        # Get the password from an appropriate environment variable, if
        # available.
        PASSWORD = os.getenv('SNOWSQL_PWD')

        # Get the other login info etc. from the command line.
        if len(argv) < 11:
            msg = "ERROR: Please pass the following command-line parameters:\n"
            msg += "--warehouse <warehouse> --database <db> --schema <schema> "
            msg += "--user <user> --account <account> "
            print(msg)
            sys.exit(-1)
        else:
            connection_parameters = self.args_to_properties(argv)
            USER = connection_parameters["user"]
            ACCOUNT = connection_parameters["account"]
            WAREHOUSE = connection_parameters["warehouse"]
            DATABASE = connection_parameters["database"]
            SCHEMA = connection_parameters["schema"]
            # Optional: for internal testing only.
            try:
                PORT = connection_parameters["port"]
                PROTOCOL = connection_parameters["protocol"]
            except:
                PORT = ""
                PROTOCOL = ""

        # If the password is set by both command line and env var, the
        # command-line value takes precedence over (is written over) the
        # env var value.

        # If the password wasn't set either in the environment var or on
        # the command line...
        if PASSWORD is None or PASSWORD == '':
            print("ERROR: Set password, e.g. with SNOWSQL_PWD environment variable")
            sys.exit(-2)
        # -- <) ---------------------------- END_SECTION ---------------------

        # Optional diagnostic:
        print("USER:", USER)
        print("ACCOUNT:", ACCOUNT)
        print("WAREHOUSE:", WAREHOUSE)
        print("DATABASE:", DATABASE)
        print("SCHEMA:", SCHEMA)
        print("PASSWORD:", PASSWORD)
        print("PROTOCOL:" "'" + PROTOCOL + "'")
        print("PORT:" + "'" + PORT + "'")

        print("Connecting...")
        if PROTOCOL is None or PROTOCOL == "" or PORT is None or PORT == "":
            # -- (> ------------------- SECTION=connect_to_snowflake ---------
            conn = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA
                )
            # -- <) ---------------------------- END_SECTION -----------------
        else:
            conn = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA,
                # Optional: for internal testing only.
                protocol=PROTOCOL,
                port=PORT
                )

        return conn

    def set_up(self, connection):

        """
        PURPOSE:
            Set up to run a test. You can override this method with one
            appropriate to your test/demo.
        """

        # Create a temporary warehouse, database, and schema.
        self.create_warehouse_database_and_schema(connection)

    def do_the_real_work(self, conn):

        """
        PURPOSE:
            Your sub-class should override this to include the code required for
            your documentation sample or your test case.
            This default method does a very simple self-test that shows that the
            connection was successful.
        """

        # Create a cursor for this connection.
        cursor1 = conn.cursor()
        # This is an example of an SQL statement we might want to run.
        command = "SELECT PI()"
        # Run the statement.
        cursor1.execute(command)
        # Get the results (should be only one):
        for row in cursor1:
            print(row[0])
        # Close this cursor.
        cursor1.close()

    def clean_up(self, connection):

        """
        PURPOSE:
            Clean up after a test. You can override this method with one
            appropriate to your test/demo.
        """

        # Create a temporary warehouse, database, and schema.
        self.drop_warehouse_database_and_schema(connection)

    def create_warehouse_database_and_schema(self, conn):

        """
        PURPOSE:
            Create the temporary schema, database, and warehouse that we use
            for most tests/demos.
        """

        # Create a database, schema, and warehouse if they don't already exist.
        print("\nCreating warehouse, database, schema...")
        # -- (> ------------- SECTION=create_warehouse_database_schema -------
        conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
        conn.cursor().execute("USE DATABASE testdb_mg")
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")
        # -- <) ---------------------------- END_SECTION ---------------------

        # -- (> --------------- SECTION=use_warehouse_database_schema --------
        conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
        conn.cursor().execute("USE DATABASE testdb_mg")
        conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")
        # -- <) ---------------------------- END_SECTION ---------------------

    def drop_warehouse_database_and_schema(self, conn):

        """
        PURPOSE:
            Drop the temporary schema, database, and warehouse that we create
            for most tests/demos.
        """

        # -- (> ------------- SECTION=drop_warehouse_database_schema ---------
        conn.cursor().execute("DROP SCHEMA IF EXISTS testschema_mg")
        conn.cursor().execute("DROP DATABASE IF EXISTS testdb_mg")
        conn.cursor().execute("DROP WAREHOUSE IF EXISTS tiny_warehouse_mg")
        # -- <) ---------------------------- END_SECTION ---------------------


# ----------------------------------------------------------------------------

if __name__ == '__main__':
    # pvb = PythonVeritasBase()
    # pvb.main(sys.argv)
    print('hello world')