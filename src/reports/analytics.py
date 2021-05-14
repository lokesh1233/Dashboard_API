import json
from reports.postgreDashboard import DashBoard
from config.env import env
from postgreSql.connection import Utility
from postgreSql.postgresqlQuery import EmployeeDetails

psdatabase=Utility()
_dashBoard=DashBoard()
_employeeDetails=EmployeeDetails()

class analyticsappi():

    def __init__(self):
        self.env = env
        self.psdatabase = Utility()

    def getSupportAndLiveChatDetails(self):

        previousId = """select table_id from dashboard_api_logDetails where type='Support and Live Chat Details'"""
        id = ("".join(json.dumps(psdatabase.connect_execute(query_type="select",dbQuery=previousId)))).strip("[]").strip('"')

        if len(id)==0:

            support_chat_data = """select id ,intent_name,data from conversation_event 
                                            where intent_name is not null and intent_name!='feedback'"""

        else:
            support_chat_data = str("""select id ,intent_name,data from conversation_event 
                                                        where intent_name is not null and intent_name!='feedback' and id={0}""").format(id)


        support_chat_data_event_data = self.psdatabase.connect_execute(query_type="select",dbQuery=support_chat_data)

        _dashBoard.prepareSupportAndChatData(support_chat_data_event_data)

        support_chat_data = str("""select sender_id,event,timestamp,text,table_id,user_intent,confidence,environment,main_intent from getSupportAndLiveChatDetails""")
        support_chat_data_event_data = psdatabase.connect_execute(query_type="select",dbQuery=support_chat_data)

        support_action_list=[]

        for row in support_chat_data_event_data:
            final_dict = {}
            final_dict['sender_id'] = row[0]
            final_dict['event'] = row[1]
            final_dict['timestamp'] = row[2]
            final_dict['text'] = row[3]
            final_dict['table_id'] = row[4]
            final_dict['user_intent'] = row[5]
            final_dict['confidence']=row[6]
            final_dict['environment']=row[7]
            final_dict['main_intent']=row[8]
            support_action_list.append(final_dict)

        return support_action_list

    def getLikeandDislikeDetails(self):

        previousId = """select table_id from dashboard_api_logDetails where type='Like and Dislike'"""
        id = ("".join(json.dumps(psdatabase.connect_execute(query_type="select",dbQuery=previousId)))).strip("[]").strip('"')

        if len(id)==0:

            like_and_dislike = """select id ,type_name,intent_name,action_name,data from conversation_event"""

        else:
            like_and_dislike = str("""select id ,type_name,intent_name,action_name,data from conversation_event where id={0}""").format(id)

        like_and_dislike_data = self.psdatabase.connect_execute(query_type="select", dbQuery=like_and_dislike)

        _dashBoard.preparelikeAndDislike(like_and_dislike_data)

        conversation_event_data = str("""select sender_id,timestamp,table_id,like_dislike,environment,main_intent from getLikeDislikeDetails""")

        like_and_dislike = psdatabase.connect_execute(query_type="select",dbQuery=conversation_event_data)

        like_and_dislike_list=[]
        for row in like_and_dislike:
            final_dict = {}
            final_dict['sender_id'] = row[0]
            final_dict['timestamp'] = row[1]
            final_dict['table_id'] = row[2]
            final_dict['like_dislike'] = row[3]
            final_dict['environment'] = row[4]
            final_dict['main_event'] = row[5]
            like_and_dislike_list.append(final_dict)

        return like_and_dislike_list

    def getFallbackData(self):

        previousId = """select table_id from dashboard_api_logDetails where type='Fallback Details'"""
        id=("".join(json.dumps(psdatabase.connect_execute(query_type="select",dbQuery=previousId)))).strip("[]").strip('"')

        if len(id)==0:
            select_conversation_event = "select id,data from conversation_event"
        else:
            select_conversation_event = str("""select id,data from conversation_event where id={0}""").format(id)

        conversation_event_data = self.psdatabase.connect_execute(query_type="select",dbQuery=select_conversation_event)

        _dashBoard.prepareFallBackData(conversation_event_data)

        select_conversation_event = str("""select sender_id,timestamp,text,intent,confidence,environment,action,table_id from getfallbackDetails""")
        conversation_event_data = psdatabase.connect_execute(query_type="select",dbQuery=select_conversation_event)

        fallback_action_list=[]
        for row in conversation_event_data:
            final_dict = {}
            final_dict['sender_id']=row[0]
            final_dict['timestamp']=row[1]
            final_dict['text']=row[2]
            final_dict['intent']=row[3]
            final_dict['confidence']=row[4]
            final_dict['environment']=row[5]
            final_dict['action']=row[6]
            final_dict['table_id']=row[7]
            fallback_action_list.append(final_dict)

        return fallback_action_list

    def postgreAnalyticsuserMessage(self):

        previousId = """select table_id from dashboard_api_logDetails where type='Analytics Data'"""
        id = ("".join(json.dumps(psdatabase.connect_execute(query_type="select", dbQuery=previousId)))).strip("[]").strip('"')

        if len(id)==0:
            select_conversation_event = "select id,data from conversation_event"
        else:
            select_conversation_event = str("""select id,data from conversation_event where id={0}""").format(id)

        conversation_event_data = self.psdatabase.connect_execute(query_type="select",dbQuery=select_conversation_event)

        select_conversation = "select sender_id ,latest_event_time from conversation "

        conversation_data = self.psdatabase.connect_execute(query_type="select",dbQuery=select_conversation)

        _dashBoard.prepareData(conversation_data, conversation_event_data)

        select_conversation_event = str("""select sender_id,event,timestamp,text,table_id,name,confidence,environment,latest_event_time from getmessages""")

        conversation_event_data = psdatabase.connect_execute(query_type="select",dbQuery=select_conversation_event)

        final_list=[]
        for row in conversation_event_data:
            final_dict = {}
            final_dict['sender_id']=row[0]
            final_dict['event']=row[1]
            final_dict['timestamp']=row[2]
            final_dict['text']=row[3]
            final_dict['table_id']=row[4]
            final_dict['name']=row[5]
            final_dict['confidence']=row[6]
            final_dict['environment']=row[7]
            final_dict['last_event_time']=row[8]
            final_list.append(final_dict)

        return final_list

    def getEmpInfo(self):

        employee_details_list = []

        postgres_employee_select_query = "select completeinfo from o360_employee_details"

        emp_data = psdatabase.connect_execute(query_type="select",dbQuery=postgres_employee_select_query)

        for emp in emp_data:

            employee_details_list.append(emp[0])

        print(employee_details_list)

        return employee_details_list

    def insertEmployeeDB(self):

        allEmployeeData = _employeeDetails.getAllEmployeeDetails(env.batchSize)

        _employeeDetails.create_tables()

        _employeeDetails.insertEmployeeData(allEmployeeData)

        return "Employee data inserted in Employee , HR and Manager tables"

    def getEmployeeData(self):

        message = _employeeDetails.EmployeeData()

        return message

    def createAndinsertEmployeeCompleteData(self):

        _employeeDetails.createCompleteEmployeeInfoTable()

        message = _employeeDetails.insertCompleteEmployeeData(_employeeDetails.employeeIDs(_employeeDetails.getEmployeeCount()))

        return message

    def getCompleteEmployeeData(self):

        all_employee_data_from_file = _employeeDetails.getEmpInfoJsonFile(_employeeDetails.getEmployeeCount())

        return all_employee_data_from_file



