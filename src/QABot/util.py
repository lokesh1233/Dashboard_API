import psycopg2
from config.env import env as config

class utility():

    def __init__(self):
        self._db_connection()

    def _db_connection(self):
        # Set up a connection to the postgres server.
        try:
            conn_string = "host=" + config.postgreSql_dbhost + " port=" + config.postgreSql_dbport + " dbname=" + config.postgreSql_dbname + " user=" + config.postgreSql_dbuser \
                          + " password=" + config.postgreSql_dbpswd
            self._connection = psycopg2.connect(conn_string)
            self._connection.autocommit = True
            # self._connectionCursor = conn.cursor()
        except Exception as err:
            print(f'Connection err: {err}')

    def selectQuery(self, table, query="*", parameter="", limit=0):

        # conn = self.connect()
        _id  = "" if parameter == "" else "where "+ parameter
        _limit = "" if limit == 0 else str(limit)
        selQuery = "SELECT {query} FROM {table} {_limit} {_id};".format(query=query, table=table, _limit=_limit, _id=_id)
        # cursor = conn.execute(selQuery)
        return self.connect_execute(selQuery)


    def connect_execute(self, dbQuery, params = None):
        # Set up a connection to the postgres server.
        conn_string = "host=" + config.postgreSql_dbhost + " port=" + config.postgreSql_dbport + " dbname=" + config.postgreSql_dbname + " user=" + config.postgreSql_dbuser \
                      + " password=" + config.postgreSql_dbpswd


        if self._connection.closed > 0:
            self._db_connection()

        result = None
        try:
            # with psycopg2.connect(conn_string) as connection:
            #     cursor = connection.cursor()
            with self._connection, self._connection.cursor() as cur:
                cur.execute(dbQuery, params)
                result = self.handleResponses(cur)
        except psycopg2.Error as error:
            print(f'error while connect to PostgreSQL : '
                  f'{error}')
            result = self.handleErrorResponse(error)
        except Exception as error:
            print(f'error while connect to PostgreSQL : '
                  f'{error}')
            result = self.handleErrorResponse(error)

        finally:
            # if connection:
            #     cursor.close()
            #     connection.close()
            # print('PostgreSQL connection to is closed')
            return result
        return result

    def handleResponses(self, cursor):
        try:
            result = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
            return result;
        except:
            return {"message":cursor.statusmessage,"type":"S"}

    def handleErrorResponse(self, res):
        try:
            return {"message":f'{res}'.split("DETAIL: ")[1],
             "type":"E"}
        except Exception as error:
            print(f"{error}")
            return {"message": f'{res}',
             "type": "E"}


