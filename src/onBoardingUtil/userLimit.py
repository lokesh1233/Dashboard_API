from sanic import response
import json
from config.env import env
from postgreSql.connection import Utility
from mail.sendMail import generateEmail

_connectionDB=Utility()
email=generateEmail()

def triggerMail(function):
    def inner_function(request):
        if request.method=='POST':
            dbCountQuery = "SELECT count(*) FROM o360_employee WHERE empid ~* '^[A-Z]'"
            present_count = int(("".join(json.dumps(_connectionDB.connect_execute(env.select_query_type,dbCountQuery)))).strip("[]"))
            dbCountQuery = "select license_count from param"
            max_count = int(("".join(json.dumps(_connectionDB.connect_execute(env.select_query_type,dbCountQuery)))).strip("[]"))
            if present_count > max_count:
                # email.SendMail()
                return response.json({"message": "User limit exceeded"})
            else:
                return function(request)
        else:
            return function(request)

    return inner_function



