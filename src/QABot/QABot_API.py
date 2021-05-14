import psycopg2
from flask import jsonify
import csv, io, json
from config.env import env as config
from .util import utility
from datetime import datetime


class generatedQuestion:

    def __init__(self):
        self.psdatabase = utility()
        pass

    # def databaseConnection(self):
    #     connection = psycopg2.connect(user=env.postgreSql_dbuser,
    #                                   password=env.postgreSql_dbpswd,
    #                                   host=env.postgreSql_dbhost,
    #                                   port=env.postgreSql_dbport,database=env.postgreSql_dbname)
    #
    #     connection.autocommit = True
    #     return connection

    # def executeQuery(self,query,parameter):
    #     connection = self.databaseConnection()
    #     cursor = connection.cursor()
    #     if parameter:
    #         cursor.execute(query,parameter)
    #         # return cursor
    #     else:
    #         cursor.execute(query)
    #     return cursor
    #     cursor.close()
    #     connection.close()

    def generatedQuestion(self,content,id):
        if type(content) == dict:
            dt = datetime.now()
            keys = ["answer", "type", "para_id", "user_id"]
            updateString = 'update_ind = %s,  updated_on = %s'
            values = ['x', dt]
            for key in keys:
                if content.get(key, None) != None:
                    updateString += f", {key} = %s"
                    values.append(content[key])

        updateQuery = f"update {config.qabotTable} set {updateString} where sid=%s"
        values.append(id)
        return self.psdatabase.connect_execute(updateQuery, values)

        # for key in content:
        #     value = content[key]
        #     val=(value,id)
        #     updateQuery=str("""update qnatable set {0} = %s where sid=%s""").format(key)
        #     try:
        #         self.executeQuery(updateQuery,val)
        #     except (Exception, psycopg2.Error) as error:
        #         print("Error while connecting to PostgreSQL", error)
        #
        # return json.dumps({"message":"Updated "+str(id)+" Successfully"})

    def deleteRow(self,id):
        deleteQuery=f"delete from {config.qabotTable} where sid={id}"
        # try:
        return self.psdatabase.connect_execute(deleteQuery, id)
        # except (Exception, psycopg2.Error) as error:
        #     print("Error while connecting to PostgreSQL", error)
        #
        # return json.dumps({"message":"Deleted "+str(id)+" Successfully"})

    def insertData(self,content):

        if type(content) == dict:
            content['update_ind'] = 'x'
            content['updated_on'] = datetime.now()
        return self.InsertQABotInbox(content)


        # print(list(content.values()))
        # insertQuery = f"""insert into qnatable(question,answer) values(%s,%s)"""
        # try:
        #     cursor=self.executeQuery(insertQuery,list(content.values(),))
        # except (Exception, psycopg2.Error) as error:
        #     print("Error while connecting to PostgreSQL", error)
        # return json.dumps({"message":"Insert successfull"})


    def displayData(self,*args):
        offset, limit=args[0],args[1]
        data=None
        qry = "ORDER BY updated_on ASC"
        # try:
        if offset != None:
            qry = " order by updated_on OFFSET %s ROWS FETCH NEXT %s ROWS ONLY"
            data = (offset,limit)
        selectQna = f"""select sid, question, answer, type, para_id, source_type  from {config.qabotTable} WHERE update_ind IS NOT NULL {qry}"""
        result = self.psdatabase.connect_execute(selectQna, data)
        return result
        #
        #
        #
        #     if (offset is None) and (limit is None):
        #         selectQna = f"""select sid, question, answer, type, para_id  from {config.qabotTable}"""
        #         cursor=self.executeQuery(selectQna,None)
        #     else:
        #         selectQna = f"""select sid, question, answer, type, para_id  from {config.qabotTable} order by uid OFFSET %s ROWS FETCH NEXT %s ROWS ONLY"""
        #         cursor = self.executeQuery(selectQna,data)
        #     qna = cursor.fetchall()
        #     row_headers = [x[0] for x in cursor.description]  # this will extract row headers
        #     json_data = []
        #     for result in qna:
        #         json_data.append(dict(zip(row_headers, result)))
        #     return jsonify(json_data)
        #
        # except (Exception, psycopg2.Error) as error:
        #     print("Error while connecting to PostgreSQL", error)

    def QABotInbox(self):
        inboxQuery = f'select sid, question, answer, type, para_id, source_type from {config.qabotTable} WHERE update_ind IS NULL ORDER BY updated_on ASC;'
        return self.psdatabase.connect_execute(inboxQuery)


    def createQABotTable(self, req):
        if req != None and req.get("token", None) == config.authenticateId:
            question_answer_bot = f"""CREATE TABLE IF NOT EXISTS {config.qabotTable} (
	            sid serial PRIMARY KEY,
	            question VARCHAR(400) UNIQUE NOT NULL,
	            answer VARCHAR(400),
	            user_id  VARCHAR(20),
	            created_on TIMESTAMP DEFAULT now(),
	            updated_on TIMESTAMP,
	            source_type  VARCHAR(20) DEFAULT 'bot',
	            para_id SMALLINT,
	            update_ind VARCHAR(1)
                );"""
            return self.psdatabase.connect_execute(question_answer_bot)
            # return {"message": "Success.", "type": "S"}
        else:
            return {"message":"You are not authenticated to use this service.", "type":"E"}

    def InsertQABotInbox(self, req):
        values = self.validateFields(req)
        if len(values) == 7:
            # question, answer, user_id, type, para_id = values
            insertQry = f"insert into {config.qabotTable}(question, answer, user_id, type, para_id, update_ind, source_type) values(%s,%s,%s,%s,%s,%s,%s)"
            return self.psdatabase.connect_execute(insertQry, list(values))
        else:
            return {"message":"No question, answer fields found in data", "type":"E"}

    def QABotFileUpload(self, fileString, isInbox='inbox'):
        try:
            reader = csv.DictReader(io.StringIO(str(fileString, 'utf-8')))
            json_data = json.loads(json.dumps(list(reader)))
            for dta in json_data:
                dta['source_type'] = dta.get("source_type", "portal")
                if isInbox == 'inbox':
                    self.InsertQABotInbox(dta)
                else:
                    self.insertData(dta)
            return {"message": "Updated data successfully ", "type":'S'}
        except Exception as error:
            print(f'error while connect to PostgreSQL : '
                  f'{error}')
            return {"message":f'{error}', "type":'E'}

    def validateFields(self, req):
        if req != None:
            question=req.get("question", None)
            answer=req.get("answer", None)
            user_id = req.get("user_id", None)
            type = req.get("type", None)
            source_type = req.get("source_type", 'bot')
            para_id =req.get("para_id", None)
            update_ind = req.get("update_ind", None)
            return (question, answer, user_id, type, para_id, update_ind, source_type)
        else:
            return (None)










