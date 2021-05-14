import datetime
import csv
import io
import json
from postgreSql.connection import Utility
from onBoardingUtil.utility import DBProcessUtil

class OnBoarding:
    def __init__(self):
        self._connectionDB = Utility()
        self.dbProcessUtil=DBProcessUtil()

    def insertEmployeeData(self, employee):

        fields, values = self.validateFields(employee, False)
        print(values)
        if fields == None or values == None or (type(values) == list and len(values) == 0):
            return {"message": "No records to insert", "type": "E"}
        insertQuery = f""" INSERT INTO employee {str(tuple(fields)).replace("'", "")} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        res = self._connectionDB.connect_execute(insertQuery, tuple(values))
        msg, mType = self.dbProcessUtil.handleResponses(res, "INSERT 0 1")
        if mType == "S":
            return {"message": "Employee records inserted", "type": "S"}
        return {"message": msg, "type": "E"}


    def deleteEmployeeData(self, id):
        postgres_delete_employee=str("""update employee set isactive=0 where empid={0}""").format(id)
        self._connectionDB.connect_execute(postgres_delete_employee)
        return {"message": "Employee records deleted successfully", "type": "S"}

    def updateEmployee(self, employee,id):

        fields, values = self.validateFields(employee, True)
        if id == None or fields == None or values == None or (type(values) == list and len(values) == 0):
            return {"message": "No records to update", "type": "E"}
        updateQuery = f"""update employee set {fields} where empid=%s"""
        values.append(id)
        res = self._connectionDB.connect_execute(updateQuery, values)
        msg, mType = self.dbProcessUtil.handleResponses(res, "UPDATE 1")
        if mType == "S":
            return {"message": "Employee records updated", "type": "S"}
        return {"message": msg, "type": "E"}


    def valudateDate(self, dte):
        if dte != None and dte != "":
            return datetime.datetime.fromtimestamp(int(dte)).strftime("%Y-%m-%d")
        return None


    def displayEmployee(self,id=None):

        if id is None:
            selectEmployee = """select * from employee"""
        else:
            selectEmployee =str("""select * from employee where empid={0}""").format(id)
        return json.loads(json.dumps(self._connectionDB.connect_execute(selectEmployee), default=self.myconverter))

    def myconverter(sefl, o):
        if isinstance(o, datetime.date):
            return o.__str__()


    def readUploadFile(self,fileString):
        try:
            reader = csv.DictReader(io.StringIO(str(fileString, 'utf-8')))
            json_data = json.loads(json.dumps(list(reader)))
            for dta in json_data:
                res = self.insertEmployeeData(dta)
            return res
        except Exception as error:
            print(f'error while connect to PostgreSQL : '
                  f'{error}')
            return {"message":f'{error}', "type":'E'}


    def validateFields(self, emp, isUpdate=True):
        if type(emp) != dict:
            return None, None
        keys = ["bandcode", "banddescription", "businessunitname", "dateofbirth", "dateofjoining", "dateofwedding",
                  "department", "email", "empid", "employeebandid", "employeefullname", "firstname", "gender", "isactive",
                  "lastname", "location", "managerid", "managername", "middlename", "mobileno", "username"]
        validateFields = {
            "dateofjoining": self.valudateDate
        }
        return self.dbProcessUtil.validateFields(emp, keys, isUpdate, validateFields)













