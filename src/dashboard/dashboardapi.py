from mongoDB.mongoquery import mongoquery
from config.env import env

class dashboardapi(mongoquery):
    def __init__(self):
        super().__init__()

    def dashboardusermessages(self):
        query = {"events.event" :{'$all':["user","bot"]}}, {'_id':0, 'sender_id':1, "latest_event_time":1, "events.event":1, "events.timestamp":1,"events.text":1, "events.metadata.environment":1, "events.parse_data.intent":1}
        selection = {'_id':0, 'sender_id':1, "latest_event_time":1, "events.event":1, "events.timestamp":1,"events.text":1, "events.metadata.environment":1, "events.parse_data.intent":1}
        aggregate = [{"$project":selection},
                     {"$unwind": "$events"},
                     {"$unwind": "$events.event"},
                     {"$match": {'$or':[{'events.event': "bot"},{'events.event': "user"}]}},
                     {"$group":{"_id":"$sender_id","sender_id":{"$first":"$sender_id"}, "events":{"$push":"$events"}, "latest_event_time":{"$first":"$latest_event_time"}}}
                     ]
        table = env.mongodb_table_conversation
        qrydetails = self.getquery(table=table, query=query, aggregate=aggregate)
        return qrydetails

    def dashboardemployeedet(self):
        query = {}, {'_id': False}
        table = env.mongodb_empdetails
        qrydetails = self.getquery(table=table, query=query, aggregate=[])
        return qrydetails

    def dashboardconversation(self):
        return {}

