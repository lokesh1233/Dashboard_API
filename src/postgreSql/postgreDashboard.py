import json
from config.env import env
from postgreSql.connection import Utility

class DashBoard():
    def __init__(self):
        self.env = env
        self.psdatabase=Utility()

    def prepareFallBackData(self, conversation_event_data):

        fallback_action_list = []

        i = 1
        for event_data in conversation_event_data:

            try:

                table_id = event_data[0]

                inner_dictionary = json.loads(event_data[1])

                if "event" in inner_dictionary:

                    if inner_dictionary["event"] == 'user':

                        wanted_keys = (
                            "sender_id", "name", "event", "latest_event_time", "text", "timestamp")

                        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                        user_dict = dictfilt(inner_dictionary, wanted_keys)

                        user_dict['table_id'] = table_id

                        if "parse_data" in inner_dictionary and "metadata" in inner_dictionary:

                            user_dict['intent'] = inner_dictionary['parse_data']['intent']['name']

                            user_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']

                            if 'environment' in inner_dictionary["metadata"]:

                                user_dict['environment'] = inner_dictionary["metadata"]['environment']

                            else:

                                user_dict['environment'] = "others"

                    if inner_dictionary["event"] == 'action':

                        wanted_keys = ("sender_id", "name", "latest_event_time", "text", "timestamp")

                        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                        final_dict = dictfilt(inner_dictionary, wanted_keys)

                        action_table_id = table_id

                        if final_dict["name"] == 'my_fallback_action':

                            if final_dict['sender_id'] == user_dict['sender_id'] and action_table_id == int(

                                    user_dict['table_id']) + 1:

                                final_dict['text'] = user_dict['text']

                                final_dict['intent'] = user_dict['intent']

                                final_dict['confidence'] = user_dict['confidence']

                                if "environment" in user_dict:
                                    final_dict['environment'] = user_dict['environment']

                                final_dict['action'] = final_dict.pop('name')
                                # to check table id , helpful for debugging
                                final_dict['table_id'] = table_id

                                user_dict = {}

                                fallback_action_list.append(final_dict)

                if i == len(conversation_event_data):
                    data = json.loads(event_data[1])
                    if "timestamp" in data and "sender_id" in data:
                        delete_query="DELETE FROM dashboard_api_logDetails where type='Fallback Details'"
                        self.psdatabase.connect_execute(env.delete_query_type,dbQuery=delete_query)
                        insert_fallback_details = """INSERT INTO dashboard_api_logDetails ( sender_id, timestamp,type,table_id)values(%s,%s,%s,%s)"""
                        data = (data["sender_id"], data["timestamp"], "Fallback Details", event_data[0])
                        self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_fallback_details, params=data)

                i = i + 1


            except Exception as e:

                print("Exception occured !!!")

                print(e)

        try:
            insert_fallback_details = """INSERT INTO getfallbackDetails ( sender_id, timestamp, text, intent, confidence, environment,action,table_id)
            values(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for list in fallback_action_list:
                fallback_details = []
                for val in list.values():
                    fallback_details.append(val)
                self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_fallback_details,
                                           params=fallback_details)
                print(fallback_details)
        except Exception as e:
            print("Exception occured")
            print(e)

        return fallback_action_list

    def prepareSupportAndChatData(self, support_chat_data_event_data):

        support_action_list = []

        test_dict = {}

        get_main_intent = False

        i = 1
        for event_data in support_chat_data_event_data:

            table_id = event_data[0]

            intent_name = event_data[1]

            inner_dictionary = json.loads(event_data[2])

            if intent_name == 'email_support' or intent_name == 'live_agent':

                get_main_intent = True

                if "event" in inner_dictionary:

                    if inner_dictionary["event"] == 'user':

                        wanted_keys = (
                            "sender_id", "name", "event", "latest_event_time", "text", "timestamp")

                        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                        test_dict = dictfilt(inner_dictionary, wanted_keys)

                        test_dict['table_id'] = table_id

                        if "parse_data" in inner_dictionary and "metadata" in inner_dictionary:

                            test_dict['user_intent'] = inner_dictionary['parse_data']['intent']['name']

                            test_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']

                            if 'environment' in inner_dictionary["metadata"]:

                                test_dict['environment'] = inner_dictionary["metadata"]['environment']

                            else:

                                test_dict['environment'] = "others"

                        get_main_intent = True

            else:

                if get_main_intent and intent_name != None:
                    test_dict['main_intent'] = intent_name

                    support_action_list.append(test_dict)

                    test_dict = {}

                    get_main_intent = False

            if i == len(support_chat_data_event_data):
                data = json.loads(event_data[2])
                if "timestamp" in data and "sender_id" in data:
                    delete_query = "DELETE FROM dashboard_api_logDetails where type='Support and Live Chat Details'"
                    self.psdatabase.connect_execute(env.delete_query_type,dbQuery=delete_query)
                    insert_fallback_details = """INSERT INTO dashboard_api_logDetails ( sender_id, timestamp,type,table_id)values(%s,%s,%s,%s)"""
                    data = (data["sender_id"], data["timestamp"], "Support and Live Chat Details", event_data[0])
                    self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_fallback_details, params=data)

            i = i + 1

        try:
            insert_support_livefeedback = """INSERT INTO getSupportAndLiveChatDetails ( sender_id,event,timestamp, text,table_id ,user_intent, confidence, environment,main_intent)
            values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for list in support_action_list:
                live_feedback = []
                for val in list.values():
                    live_feedback.append(val)
                self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_support_livefeedback,params=live_feedback)

        except Exception as e:
            print("Exception occured")
            print(e)

        return support_action_list

    def preparelikeAndDislike(self, like_and_dislike_data):

        like_dislike_list = []

        user_dict = {}

        get_main_intent = False

        main_intent = None

        like_dislike = None

        test_dict = {}

        i = 1
        for event_data in like_and_dislike_data:

            table_id = event_data[0]
            intent_type = event_data[1]
            intent_name = event_data[2]
            action_name = event_data[3]

            data_dictionary = json.loads(event_data[4])

            if intent_type == 'slot' and action_name == 'confirm':

                for data in data_dictionary:

                    if data == 'value':

                        if data_dictionary['value'] == "true":

                            like_dislike = "like"

                        elif data_dictionary['value'] == "false":

                            like_dislike = "dislike"

            if data_dictionary["event"] == 'user':

                wanted_keys = (
                    "sender_id", "name", "event", "latest_event_time", "text", "timestamp")

                dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                user_dict = dictfilt(data_dictionary, wanted_keys)

                user_dict['table_id'] = table_id

                if "parse_data" in data_dictionary and "metadata" in data_dictionary:

                    user_dict['intent'] = data_dictionary['parse_data']['intent']['name']

                    user_dict['confidence'] = data_dictionary['parse_data']['intent']['confidence']

                    if 'environment' in data_dictionary["metadata"]:

                        user_dict['environment'] = data_dictionary["metadata"]['environment']

                    else:

                        user_dict['environment'] = "others"

            if action_name == 'utter_like_dislike':

                if "event" in data_dictionary:

                    if data_dictionary["event"] == 'action':

                        wanted_keys = (
                            "sender_id", "latest_event_time", "text", "timestamp")

                        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                        test_dict = dictfilt(data_dictionary, wanted_keys)

                        action_table_id = table_id

                        if bool(user_dict):

                            if test_dict['sender_id'] == user_dict['sender_id']:

                                test_dict['table_id'] = table_id

                                test_dict['like_dislike'] = like_dislike

                                if "environment" in user_dict:
                                    test_dict['environment'] = user_dict['environment']

                                if "parse_data" in data_dictionary:
                                    test_dict['user_intent'] = data_dictionary['parse_data']['intent']['name']

                                    test_dict['confidence'] = data_dictionary['parse_data']['intent']['confidence']
                                # to get the main intent in the upcoming iterations
                                get_main_intent = True

                                like_dislike = None
            else:

                if get_main_intent and intent_name != None:
                    test_dict['main_intent'] = intent_name

                    like_dislike_list.append(test_dict)

                    test_dict = {}

                    get_main_intent = False

            if i == len(like_and_dislike_data):
                data = json.loads(event_data[4])
                if "timestamp" in data and "sender_id" in data:
                    delete_query = "DELETE FROM dashboard_api_logDetails where type='Like and Dislike'"
                    self.psdatabase.connect_execute(env.delete_query_type,dbQuery=delete_query)
                    insert_fallback_details = """INSERT INTO dashboard_api_logDetails ( sender_id, timestamp,type,table_id)values(%s,%s,%s,%s)"""
                    data = (data["sender_id"], data["timestamp"], "Like and Dislike", event_data[0])
                    self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_fallback_details, params=data)

            i = i + 1

        try:
            insert_like_dislike = """INSERT INTO getLikeDislikeDetails ( sender_id, timestamp, table_id, like_dislike, environment, main_intent)
            values(%s,%s,%s,%s,%s,%s)"""

            for list in like_dislike_list:
                if not (("like_dislike", None) in list.items()):
                    like_dislike = []
                    for val in list.values():
                        like_dislike.append(val)
                    self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_like_dislike, params=like_dislike)

        except Exception as e:
            print("Exception occured")
            print(e)

        return like_dislike_list

    def prepareData(self, conversation_data, conversation_event_data):

        conversation_list = []

        for data in conversation_data:
            inner_dictionary = {}

            inner_dictionary['sender_id'] = data[0]

            inner_dictionary['latest_event_time'] = data[1]

            conversation_list.append(inner_dictionary)

        conversation_event_list = []

        env_dict = {}

        i=1
        for conversation in conversation_event_data:

            table_id = conversation[0]

            inner_dictionary = json.loads(conversation[1])

            if "event" in inner_dictionary:

                if inner_dictionary["event"] == 'bot':

                    wanted_keys = (
                        "sender_id", "name", "event", "confidence", "environment", "latest_event_time",
                        "text", "timestamp")

                    dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                    final_dict = dictfilt(inner_dictionary, wanted_keys)

                    final_dict["table_id"] = table_id

                    if 'env' in env_dict:

                        final_dict['environment'] = env_dict['env']

                    else:

                        final_dict['environment'] = 'others'

                    conversation_event_list.append(final_dict)

                # name and confidence are present only incase of users

                elif inner_dictionary["event"] == 'user':

                    wanted_keys = (
                        "sender_id", "name", "event", "confidence", "environment", "latest_event_time",
                        "text", "timestamp")

                    dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])

                    final_dict = dictfilt(inner_dictionary, wanted_keys)

                    final_dict["table_id"] = table_id

                    if final_dict['text'] == '/home' or final_dict['text'] == '/restart':
                        pass

                    else:

                        if "parse_data" in inner_dictionary:

                            final_dict['name'] = inner_dictionary['parse_data']['intent']['name']

                            final_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']

                            if 'environment' in inner_dictionary["metadata"]:

                                final_dict['environment'] = inner_dictionary["metadata"]['environment']

                                env_dict['env'] = inner_dictionary["metadata"]['environment']

                            else:

                                final_dict['environment'] = "others"

                                env_dict['env'] = "others"

                            conversation_event_list.append(final_dict)
            if i == len(conversation_event_data):
                data = json.loads(conversation[1])
                if "timestamp" in data and "sender_id" in data:
                    delete_query = "DELETE FROM dashboard_api_logDetails where type='messages'"
                    self.psdatabase.connect_execute(env.delete_query_type,dbQuery=delete_query)
                    insert_fallback_details = """INSERT INTO dashboard_api_logDetails ( sender_id, timestamp,type,table_id)values(%s,%s,%s,%s)"""
                    data = (data["sender_id"], data["timestamp"], "messages", conversation[0])
                    self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_fallback_details, params=data)

            i = i + 1

        for convObj in conversation_list:

            for eventObj in conversation_event_list:

                if convObj['sender_id'] == eventObj['sender_id']:
                    eventObj['latest_event_time'] = convObj['latest_event_time']

        try:
            for list in conversation_event_list:
                messages = []
                for key, val in list.items():
                    if len(list.items()) == 9:
                        insert_support_livefeedback = """INSERT INTO getmessages ( sender_id,event,timestamp, text,table_id ,name,confidence,environment,latest_event_time)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                        if key in ['sender_id', 'event', 'timestamp', 'text', 'table_id', 'environment',
                                   'latest_event_time', 'name', 'confidence']:
                            messages.append(val)
                    elif len(list.items()) == 7:
                        insert_support_livefeedback = """INSERT INTO getmessages ( sender_id,event,timestamp, text,table_id ,environment,latest_event_time)
                        values(%s,%s,%s,%s,%s,%s,%s)"""
                        messages.append(val)
                self.psdatabase.connect_execute(env.insert_query_type, dbQuery=insert_support_livefeedback, params=messages)
        except Exception as e:
            print("Exception occured")
            print(e)

        return conversation_event_list


    # def prepareData(self,conversation_data,conversation_event_data):
    #     conversation_list = []
    #     for data in conversation_data:
    #         inner_dictionary = {}
    #         inner_dictionary['sender_id'] = data[0]
    #         inner_dictionary['latest_event_time'] = data[1]
    #         conversation_list.append(inner_dictionary)
    #
    #     conversation_event_list = []
    #     for data in conversation_event_data:
    #         inner_dictionary = json.loads(data[0])
    #         if "event" in inner_dictionary:
    #             if inner_dictionary["event"] == 'bot':
    #                 wanted_keys = (
    #                 "sender_id", "name", "event", "confidence", "environment", "latest_event_time",
    #                 "text", "timestamp")
    #                 dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                 final_dict = dictfilt(inner_dictionary, wanted_keys)
    #                 # hard coded as of now
    #                 final_dict['environment'] = 'WEB'
    #                 conversation_event_list.append(final_dict)
    #             # name and confidence are present only incase of users
    #             elif inner_dictionary["event"] == 'user':
    #                 wanted_keys = (
    #                 "sender_id", "name", "event", "confidence", "environment", "latest_event_time",
    #                 "text", "timestamp")
    #                 dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                 final_dict = dictfilt(inner_dictionary, wanted_keys)
    #                 # hard coded as of now
    #                 final_dict['environment'] = 'WEB'
    #                 if "parse_data" in inner_dictionary:
    #                     final_dict['name'] = inner_dictionary['parse_data']['intent']['name']
    #                     final_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']
    #                     conversation_event_list.append(final_dict)
    #
    #     for convObj in conversation_list:
    #         for eventObj in conversation_event_list:
    #             if convObj['sender_id'] == eventObj['sender_id']:
    #                 eventObj['latest_event_time'] = convObj['latest_event_time']
    #
    #     return conversation_event_list
    #
    # # user and event data are next to each other , so first getting the user data , then if the corresponding action is my_fallback_action , then merge and returning the data
    # def prepareFallBackData(self, conversation_event_data):
    #
    #     fallback_action_list = []
    #
    #     user_dict = {}
    #
    #     for event_data in conversation_event_data:
    #         table_id = event_data[0]
    #         inner_dictionary = json.loads(event_data[1])
    #         if "event" in inner_dictionary:
    #             if inner_dictionary["event"] == 'user':
    #                 wanted_keys = (
    #                     "sender_id", "name", "event", "latest_event_time", "text", "timestamp")
    #                 dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                 test_dict = dictfilt(inner_dictionary, wanted_keys)
    #                 test_dict['table_id'] = table_id
    #                 if "parse_data" in inner_dictionary:
    #                     test_dict['intent'] = inner_dictionary['parse_data']['intent']['name']
    #                     test_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']
    #                     user_dict = test_dict
    #
    #             if inner_dictionary["event"] == 'action':
    #                 wanted_keys = ("sender_id", "name", "latest_event_time", "text", "timestamp")
    #                 dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                 final_dict = dictfilt(inner_dictionary, wanted_keys)
    #                 action_table_id = table_id
    #                 if final_dict["name"] == 'my_fallback_action':
    #                     if final_dict['sender_id'] == user_dict['sender_id'] and action_table_id == int(
    #                             user_dict['table_id']) + 1:
    #                         # hard coded as of now
    #                         final_dict['environment'] = 'WEB'
    #                         final_dict['text'] = user_dict['text']
    #                         final_dict['intent'] = user_dict['intent']
    #                         final_dict['confidence'] = user_dict['confidence']
    #                         final_dict['action'] = final_dict.pop('name')
    #                         # to check table id , helpful for debugging
    #                         final_dict['table_id']=table_id
    #                         user_dict = {}
    #                         fallback_action_list.append(final_dict)
    #
    #     return fallback_action_list
    #
    #
    #
    # def prepareSupportAndChatData(self, support_chat_data_event_data):
    #
    #     support_action_list = []
    #
    #     test_dict = {}
    #
    #     get_main_intent = False
    #
    #     for event_data in support_chat_data_event_data:
    #         table_id = event_data[0]
    #         intent_name = event_data[1]
    #         inner_dictionary = json.loads(event_data[2])
    #         if intent_name == 'email_support' or intent_name == 'live_agent':
    #             get_main_intent = True
    #             if "event" in inner_dictionary:
    #                 if inner_dictionary["event"] == 'user':
    #                     wanted_keys = (
    #                         "sender_id", "name", "event", "latest_event_time", "text", "timestamp")
    #                     dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                     test_dict = dictfilt(inner_dictionary, wanted_keys)
    #                     test_dict['table_id'] = table_id
    #                     test_dict['environment'] = 'WEB'
    #                     if "parse_data" in inner_dictionary:
    #                         test_dict['user_intent'] = inner_dictionary['parse_data']['intent']['name']
    #                         test_dict['confidence'] = inner_dictionary['parse_data']['intent']['confidence']
    #                     get_main_intent = True
    #         else :
    #             if get_main_intent and intent_name != None :
    #                 test_dict['main_intent'] = intent_name
    #                 support_action_list.append(test_dict)
    #                 test_dict={}
    #                 get_main_intent=False
    #
    #     return support_action_list
    #
    #
    #
    # def preparelikeAndDislike(self, support_chat_data_event_data):
    #     """
    #     operation is done on action 'utter_like_dislike'
    #     Before working on the action first , we are capturing the slot value for like and dislike ,
    #     as it appears before action 'utter_like_dislike'
    #     Then we are capturing the main intent in the following iterations
    #
    #     """
    #     support_action_list = []
    #
    #     user_dict = {}
    #
    #     get_main_intent = False
    #
    #     main_intent=None
    #
    #     like_dislike = None
    #
    #     test_dict = {}
    #
    #     for event_data in support_chat_data_event_data:
    #
    #         table_id = event_data[0]
    #         intent_type = event_data[1]
    #         intent_name = event_data[2]
    #         action_name = event_data[3]
    #
    #         data_dictionary = json.loads(event_data[4])
    #
    #         if intent_type=='slot' and action_name =='confirm':
    #             for data in data_dictionary:
    #                 if data =='value':
    #                     if data_dictionary['value']=="true":
    #                         like_dislike="like"
    #                     elif data_dictionary['value']=="false":
    #                         like_dislike = "dislike"
    #
    #         if action_name == 'utter_like_dislike' :
    #
    #             if "event" in data_dictionary:
    #                 if data_dictionary["event"] == 'action':
    #                     wanted_keys = (
    #                         "sender_id", "latest_event_time", "text", "timestamp")
    #                     dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
    #                     test_dict = dictfilt(data_dictionary, wanted_keys)
    #                     test_dict['table_id'] = table_id
    #                     test_dict['like_dislike']=like_dislike
    #                     test_dict['environment'] = 'WEB'
    #                     if "parse_data" in data_dictionary:
    #                         test_dict['user_intent'] = data_dictionary['parse_data']['intent']['name']
    #                         test_dict['confidence'] = data_dictionary['parse_data']['intent']['confidence']
    #                     # to get the main intent in the upcoming iterations
    #                     get_main_intent = True
    #                     like_dislike=None
    #         else :
    #             if get_main_intent and intent_name != None :
    #                 test_dict['main_intent'] = intent_name
    #                 support_action_list.append(test_dict)
    #                 test_dict={}
    #                 get_main_intent=False
    #
    #     return support_action_list
