
from pymongo import MongoClient
from config.env import env


class mongoquery():
    def __init__(self):
        self.env = env
        self.dbuser = env.mongodb_dbuser
        self.dbpswd = env.mongodb_dbpswd
        self.dbhost = env.mongodb_dbhost
        self.dbport = env.mongodb_dbport
        self.dbname = env.mongodb_dbname
        self.tableconversation = env.mongodb_table_conversation
        self.createclient()

    # client connection
    def createclient(self):
        dbclient = MongoClient(
            'mongodb://' + self.dbuser + ':' + self.dbpswd + '@' + self.dbhost + ':' + self.dbport)
        self.database = dbclient[self.dbname]

    # get query details
    def getquery(self, table='conversations', query={}, aggregate=[]):

        if len(aggregate)>0:
            return list(self.database[table].aggregate(aggregate))
        else:
            qry, selection = self.validatequery(query)
            return list(self.database[table].find(qry, selection))

    def validatequery(self, query):
        if type(query) == tuple:
            return query
        elif type(query) == dict:
            return query, {}
        else:
            return {}, {}

