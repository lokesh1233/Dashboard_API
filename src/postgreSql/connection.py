import psycopg2
from config.env import env as config

class Utility(object):
    """postgre db connection"""

    def connect_execute(self,query_type, dbQuery, params=None):
        # Set up a connection to the postgres server.
        conn_string = "host=" + config.postgreSql_dbhost + " port=" + config.postgreSql_dbport + " dbname=" + config.postgreSql_dbname + " user=" + config.postgreSql_dbuser \
                      + " password=" + config.postgreSql_dbpswd
        result = None
        try:
            with psycopg2.connect(conn_string) as connection:
                cursor = connection.cursor()
                cursor.execute(dbQuery, params)
                if query_type== config.select_query_type:
                    result = self.handleResponses(cursor)
                elif query_type== config.create_query_type:
                    result ="Table Creted successfully"
        except psycopg2.Error as error:
            print(f'error while connect to PostgreSQL : '
                  f'{error}')
            result = self.handleErrorResponse(error)
        finally:
            if connection:
                cursor.close()
                connection.close()

        return result

    def handleResponses(self, cursor):
        try:
            result =cursor.fetchall()
            return result;
        except:
            return {"message": cursor.statusmessage}

    def handleErrorResponse(self, res):
        try:
            return {"message": f'{res}'.split("DETAIL: ")[1],
                    "type": "E"}
        except:
            return {"message": f'{res}'}