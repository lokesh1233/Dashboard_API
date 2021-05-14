import psycopg2
from config.env import env
import requests
from requests.auth import HTTPBasicAuth
import json
import os
import pathlib
from pathlib import Path
from postgreSql.connection import Utility

class EmployeeDetails():
    def __init__(self):
        self.env = env
        self.dbuser = env.postgreSql_dbuser
        self.dbpswd = env.postgreSql_dbpswd
        self.dbhost = env.postgreSql_dbhost
        self.dbport = env.postgreSql_dbport
        self.dbname = env.postgreSql_dbname
        self.tableconversation = env.postgreSql_table_conversation
        self.successfactor_username = env.successfactor_username
        self.successfactor_password = env.successfactor_password
        self.successfactor_host = env.successfactor_host
        self.psdatabase = Utility()

    def insertintoColleagueDb(self,colleague_json_file_path):

        employee_colleague_details = self.getDataFromJsonFile(colleague_json_file_path)

        self.create_colleague_table()

        self.insert_colleague_data(employee_colleague_details)

        return "Colleagues data inserted in ColleagueDetails table"

    def createColleagueDataForDbinsert(self):
        colleagueList = self.getAllColleagueDetails(self.EmployeeData())
        my_path = Path(__file__).parent.parent
        os.path.abspath(os.path.dirname(__file__))
        colleague_json_file_path=str(my_path) + "\\" + "jsonFiles" + "\\" + 'Demo_Colleague'
        # self.writeIntoJsonFile('Demo_Colleague.json',colleagueList)
        if pathlib.Path(colleague_json_file_path + '.json'):
            os.remove(colleague_json_file_path + '.json')
        self.writeIntoJsonFile(colleague_json_file_path, colleagueList)

        return "colleague data created successfully"

    ## Create Table
    def create_tables(self):

        """ create tables in the PostgreSQL database"""
        commands = ("""

                CREATE TABLE IF NOT EXISTS Demo_Employee (
                    userId VARCHAR(255) PRIMARY KEY,
                    username VARCHAR(255) ,
                    division VARCHAR(255) ,
                    defaultFullName VARCHAR(255),
                    country VARCHAR(255) ,
                    firstName VARCHAR(255), 
                    lastName VARCHAR(255) ,
                    jobCode VARCHAR(255) ,
                    location VARCHAR(255) ,
                    department VARCHAR(255),
                    title VARCHAR(255) ,
                    businessPhone VARCHAR(255) ,
                    email VARCHAR(255) ,
                    cellPhone VARCHAR(255),
                    managerId VARCHAR(255),
                    hrId VARCHAR(255)
                    ) """,

                """ CREATE TABLE IF NOT EXISTS Demo_Manager (
                        managerId VARCHAR(255) PRIMARY KEY,
                        division VARCHAR(255) ,
                        defaultFullName VARCHAR(255),
                        country VARCHAR(255) ,
                        firstName VARCHAR(255), 
                        lastName VARCHAR(255) ,
                        jobCode VARCHAR(255) ,
                        location VARCHAR(255) ,
                        department VARCHAR(255),
                        email VARCHAR(255)
                        ) """,
                """ CREATE TABLE IF NOT EXISTS Demo_HR (
                       hrId VARCHAR(255) PRIMARY KEY,
                       division VARCHAR(255) ,
                       defaultFullName VARCHAR(255),
                       country VARCHAR(255) ,
                       firstName VARCHAR(255), 
                       lastName VARCHAR(255) ,
                       jobCode VARCHAR(255) ,
                       location VARCHAR(255) ,
                       department VARCHAR(255),
                       email VARCHAR(255)
                       ) """)


        for command in commands:

            print(command)

            self.psdatabase.connect_execute(env.create_query_type, command)

        print("Demo_Employee , Demo_Manager and Demo_HR tables are created")


    def insertEmployeeData(self,allEmployeeData):
        try:
            connection = psycopg2.connect(user=self.dbuser,
                                          password=self.dbpswd,
                                          host=self.dbhost,
                                          port=self.dbport,
                                          database=self.dbname)

            cursor = connection.cursor()
            for employee in allEmployeeData:
                postgres_employee_insert_query = """ INSERT INTO Demo_Employee (userId, username, division,defaultFullName,country,firstName,lastName,jobCode,location,department,title,businessPhone,email,cellPhone,managerId,hrId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                if employee['hr'] is not None:
                    hrId=employee['hr']['userId']
                else:
                    hrId=None
                if employee['manager'] is not None:
                    managerId=employee['manager']['userId']
                else:
                    managerId=None

                cursor.execute("""select exists(select 1 from Demo_Employee where userId = %s)""", (employee['userId'],))
                row = cursor.fetchone()
                # need to check , getting duplicate user id 100009
                #  if row[0] == False and employee['userId'] != '100009':
                if row[0] == False :
                    employee_record_to_insert = (
                    employee['userId'], employee['username'], employee['division'], employee['defaultFullName'],
                    employee['country'], employee['firstName'], employee['lastName'], employee['jobCode'], employee['location'],
                    employee['department'], employee['title'], employee['businessPhone'], employee['email'],
                    employee['cellPhone'], managerId, hrId)
                    cursor.execute(postgres_employee_insert_query, employee_record_to_insert)
                # First checking if the record already exists
                cursor.execute("""select exists(select 1 from Demo_Manager where managerId = %s)""",(managerId,))
                row = cursor.fetchone()
                if row[0] == False and managerId is not None :
                    postgres_manager_insert_query = """ INSERT INTO Demo_Manager (managerId, division,defaultFullName,country,firstName,lastName,jobCode,location,department,email) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
                    manager_record_to_insert = (
                    managerId, employee['division'], employee['manager']['defaultFullName'], employee['manager']['country'],
                    employee['manager']['firstName'], employee['manager']['lastName'], employee['manager']['jobCode'], employee['manager']['location'],
                    employee['manager']['department'],None)

                    cursor.execute(postgres_manager_insert_query, manager_record_to_insert)
                # First checking if the record already exists
                cursor.execute("""select exists(select 1 from Demo_HR where hrId = %s)""", (hrId,))
                row =cursor.fetchone()
                if row[0] == False and hrId is not None :
                    postgres_hr_insert_query = """ INSERT INTO Demo_HR (hrId, division,defaultFullName,country,firstName,lastName,jobCode,location,department,email) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    hr_record_to_insert = (
                    hrId, employee['hr']['division'], employee['hr']['defaultFullName'], employee['hr']['country'],
                    employee['hr']['firstName'], employee['hr']['lastName'], employee['hr']['jobCode'], employee['hr']['location'],
                    employee['hr']['department'], employee['hr']['email'])

                    cursor.execute(postgres_hr_insert_query, hr_record_to_insert)
                # # commit the changes
                connection.commit()

        except (Exception, psycopg2.Error) as error:
                print("Error while connecting to PostgreSQL", error)

        finally:
        # closing database connection.
            if (connection):
                cursor.close()
                connection.close()

        return "records inserted"

    def create_colleague_table(self):

        """ create tables in the PostgreSQL database"""
        command = """CREATE TABLE IF NOT EXISTS Demo_ColleagueDetails (
                    userId VARCHAR(255) PRIMARY KEY,
                    colleagues TEXT []
                    ) """
        self.psdatabase.connect_execute(env.create_query_type, command)

        print("Demo_ColleagueDetails table is created")

    def insert_colleague_data(self,ComleteEmloyeeDetails_Test):
        try:
            connection = psycopg2.connect(user=self.dbuser,
                                          password=self.dbpswd,
                                          host=self.dbhost,
                                          port=self.dbport,
                                          database=self.dbname)

            cursor = connection.cursor()

            postgres_colleague_insert_query = """ INSERT INTO Demo_ColleagueDetails (userId, colleagues) VALUES (%s,%s) """


            for colleague in ComleteEmloyeeDetails_Test:
                cursor.execute("""select exists(select 1 from Demo_ColleagueDetails where userId = %s)""", (colleague['userId'],))
                row = cursor.fetchone()
                if row[0] == False:
                    manager_record_to_insert = (colleague['userId'],colleague['colleague_List'])
                    cursor.execute(postgres_colleague_insert_query, manager_record_to_insert)

            connection.commit()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
        # closing database connection.
            if (connection):
                cursor.close()
                connection.close()

    def getEmployeeDetailsInBatch(self,top, skip):

        employee_Details_url = self.successfactor_host + "/User?$format=json&$expand=manager,hr&$select=userId,email,jobCode,location,country,title,username,division,defaultFullName,department,firstName,lastName,businessPhone,cellPhone,manager/userId,hr/email,manager/country,manager/firstName,manager/lastName,manager/location,manager/jobCode,manager/division,manager/defaultFullName,manager/department,hr/userId,hr/email,hr/country,hr/firstName,hr/lastName,hr/location,hr/jobCode,hr/division,hr/defaultFullName,hr/department&$top={0}&$skip={1}".format(
            top, skip)

        employee_Details_res = requests.get(url=employee_Details_url, auth=HTTPBasicAuth(self.successfactor_username, self.successfactor_password))
        # print("batch education done for employees :"+skip)

        print(employee_Details_url)

        print(employee_Details_res)

        employee_Details_json_data = employee_Details_res.json()

        employee_Details_results = employee_Details_json_data['d']['results']
        # print(employee_time_results)

        return employee_Details_results

    def getEmployeeCount(self):

        Employee_count_url = self.successfactor_host + "/User/$count?$format=json"

        picture_count = requests.get(url=Employee_count_url, auth=HTTPBasicAuth(self.successfactor_username, self.successfactor_password))

        return picture_count.json()

    def getAllEmployeeDetails(self,batchSize):

        #To get Employee Details in Batch
        All_Employee_Details = []
        count = 0
        employeeCount = self.getEmployeeCount()
        for i in range(1, employeeCount):
            if i % batchSize == 0 or i == 0 :
                count = count + 1
                top = batchSize
                employees = self.getEmployeeDetailsInBatch(top, i)
                All_Employee_Details.extend(employees)
            if i == (employeeCount - 1):
                top = i - count * batchSize
                employees = self.getEmployeeDetailsInBatch(top, i)
                All_Employee_Details.extend(employees)
        return All_Employee_Details

    def getColleagueList(self,userId):
        if self.getManagerId(userId):
            colleague_list_url = self.successfactor_host + "/User?$format=json&$filter=userId eq '{0}'&$expand=directReports&$select=userId,directReports/userId,directReports/defaultFullName".format(
                self.getManagerId(userId))

            res = requests.get(url=colleague_list_url, auth=HTTPBasicAuth(self.successfactor_username, self.successfactor_password))
            json_data = res.json()
            results = json_data['d']['results']
            # print(results)
            colleague_List = []
            data = {}
            for r in results:
                for result in r['directReports']['results']:
                    # print(result['userId'])
                    colleague_List.append(result['userId'])

            # data['colleagueList']=colleague_List
            # print("userId "+str(userId)+"colleague_List"+str(colleague_List))
            if userId in colleague_List:
                colleague_List.remove(userId)
            data['userId'] = userId
            data['colleague_List'] = colleague_List

            return data
        else:
            return {"userId": userId, "colleague_List": []}

    def getManagerId(self,userId):
            employee_time_url = self.successfactor_host + "/User?$format=json&$filter=userId eq '{0}'&$expand=manager&$select=userId,manager/userId".format(
                userId)
            employee_time_res = requests.get(url=employee_time_url,
                                             auth=HTTPBasicAuth(self.successfactor_username, self.successfactor_password))
            employee_time_json_data = employee_time_res.json()
            employee_time_results = employee_time_json_data['d']['results']
            managerId = ''
            if employee_time_results is not None:

                for manager in employee_time_results:
                    # print(manager['manager']['userId'])
                    try:
                        managerId = manager['manager']['userId']
                    except TypeError:
                        print("Manager Id is not found for: " + userId)

                return managerId
            else:
                print("Manager Id is not found for: " + userId)
                return managerId

    def getAllColleagueDetails(self,allEmployeeData):
            colleagueList=[]
            count=0
            for employee in range(len(allEmployeeData)):
                 employee=self.getColleagueList(allEmployeeData[employee]['userId'])

                 colleagueList.append(employee)
                 count = count + 1

                 if count % 100 == 0:
                     print("Completed finding colleague Details for {0} number of employees".format(count))

            return colleagueList

    def EmployeeData(self):

          postgres_employee_select_query = """select
                                                json_build_object(
                                                        'userId', emp.userId,
                                                        'username', emp.username,
                                                        'division', emp.division,
                                                        'defaultFullName', emp.defaultFullName,
                                                        'country', emp.country,
                                                        'firstName', emp.firstName,
                                                        'lastName', emp.lastName,
                                                        'jobCode', emp.jobCode,
                                                        'location', emp.location,
                                                        'department', emp.department,
                                                        'title', emp.title,
                                                        'businessPhone', emp.businessPhone,
                                                        'email', emp.email,
                                                        'cellPhone', emp.cellPhone,
                                                        'manager', json_build_object(
                                                                'userId', man.managerId,
                                                                'division', man.division,
                                                                'defaultFullName', man.defaultFullName,
                                                                'country', man.country,
                                                                'lastName', man.lastName,
                                                                'firstName', man.firstName,
                                                                'jobCode', man.jobCode,
                                                                'location', man.location,
                                                                'department', man.department
                                                        ),
                                                        'hr', json_build_object(
                                                                'userId', hr.hrId,
                                                                'division', hr.division,
                                                                'defaultFullName', hr.defaultFullName,
                                                                'country', hr.country,
                                                                'lastName', hr.lastName,
                                                                'firstName', hr.firstName,
                                                                'jobCode', hr.jobCode,
                                                                'location', hr.location,
                                                                'department', hr.department,
                                                                'email', hr.email
                                                        )
                                            )
                                        from Demo_Employee emp
                                        INNER JOIN Demo_Manager man ON emp.managerId = man.managerId
                                        INNER JOIN Demo_HR hr ON emp.hrId = hr.hrId """

          employee_records_json=[]

          employee_records = self.psdatabase.connect_execute(env.select_query_type,postgres_employee_select_query)

          for employee in employee_records:

             employee_records_json.append(employee[0])

          return employee_records_json

    def employeeIDs(self,employeeCount):

        employee_detils_list = self.EmployeeData()
        empoyeeIdList = []
        # taking top 10 values as of now
        for employee in employee_detils_list[0:employeeCount]:
            for element in employee:
                if element == 'userId':
                    empoyeeIdList.append(employee.get(element))
                    # print(empDetails)
        for emp in empoyeeIdList[:]:
            if len(emp) == 0:
                empoyeeIdList.remove(emp)

        return empoyeeIdList

    def getColleagueDetailsWithOutImage(self,colleagues):

        detailedList = []
        try:
            connection = psycopg2.connect(user=self.dbuser,
                                          password=self.dbpswd,
                                          host=self.dbhost,
                                          port=self.dbport,
                                          database=self.dbname)

            cursor = connection.cursor()
            for emp in colleagues:
                dictionary = {}
                postgres_employee_select_query = '''select
                                                           json_build_object(
                                                                                      'userId', emp.userId,
                                                                                      'username', emp.username,
                                                                                      'division', emp.division,
                                                                                      'defaultFullName', emp.defaultFullName,
                                                                                      'country', emp.country,
                                                                                      'firstName', emp.firstName,
                                                                                      'lastName', emp.lastName,
                                                                                      'jobCode', emp.jobCode,
                                                                                      'location', emp.location,
                                                                                      'department', emp.department,
                                                                                      'title', emp.title,
                                                                                      'businessPhone', emp.businessPhone,
                                                                                      'email', emp.email,
                                                                                      'cellPhone', emp.cellPhone,
                                                                                      'manager', json_build_object(
                                                                                              'userId', man.managerId,
                                                                                              'division', man.division,
                                                                                              'defaultFullName', man.defaultFullName,
                                                                                              'country', man.country,
                                                                                              'lastName', man.lastName,
                                                                                              'firstName', man.firstName,
                                                                                              'jobCode', man.jobCode,
                                                                                              'location', man.location,
                                                                                              'department', man.department
                                                                                      ),
                                                                                      'hr', json_build_object(
                                                                                              'userId', hr.hrId,
                                                                                              'division', hr.division,
                                                                                              'defaultFullName', hr.defaultFullName,
                                                                                              'country', hr.country,
                                                                                              'lastName', hr.lastName,
                                                                                              'firstName', hr.firstName,
                                                                                              'jobCode', hr.jobCode,
                                                                                              'location', hr.location,
                                                                                              'department', hr.department,
                                                                                              'email', hr.email
                                                                                      )
                                                                          )
                                                                      from Demo_Employee emp
                                                                      INNER JOIN Demo_Manager man ON emp.managerId = man.managerId
                                                                      INNER JOIN Demo_HR hr ON emp.hrId = hr.hrId 
                                                                      WHERE userId = %s '''
                cursor.execute(postgres_employee_select_query, (emp,))
                emp_data = cursor.fetchall()
                if len(emp_data) >0:
                    detailedList.append(emp_data[0][0])

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()
        return detailedList

    def getEmployeeDetailsWithOutImage(self,emp):
        dictionary = {}
        try:
            connection = psycopg2.connect(user=self.dbuser,
                                          password=self.dbpswd,
                                          host=self.dbhost,
                                          port=self.dbport,
                                          database=self.dbname)

            cursor = connection.cursor()
            postgres_employee_select_query = '''select
                                                              json_build_object(
                                                                      'userId', emp.userId,
                                                                      'username', emp.username,
                                                                      'division', emp.division,
                                                                      'defaultFullName', emp.defaultFullName,
                                                                      'country', emp.country,
                                                                      'firstName', emp.firstName,
                                                                      'lastName', emp.lastName,
                                                                      'jobCode', emp.jobCode,
                                                                      'location', emp.location,
                                                                      'department', emp.department,
                                                                      'title', emp.title,
                                                                      'businessPhone', emp.businessPhone,
                                                                      'email', emp.email,
                                                                      'cellPhone', emp.cellPhone,
                                                                      'manager', json_build_object(
                                                                              'userId', man.managerId,
                                                                              'division', man.division,
                                                                              'defaultFullName', man.defaultFullName,
                                                                              'country', man.country,
                                                                              'lastName', man.lastName,
                                                                              'firstName', man.firstName,
                                                                              'jobCode', man.jobCode,
                                                                              'location', man.location,
                                                                              'department', man.department
                                                                      ),
                                                                      'hr', json_build_object(
                                                                              'userId', hr.hrId,
                                                                              'division', hr.division,
                                                                              'defaultFullName', hr.defaultFullName,
                                                                              'country', hr.country,
                                                                              'lastName', hr.lastName,
                                                                              'firstName', hr.firstName,
                                                                              'jobCode', hr.jobCode,
                                                                              'location', hr.location,
                                                                              'department', hr.department,
                                                                              'email', hr.email
                                                                      )
                                                          )
                                                      from Demo_Employee emp
                                                      INNER JOIN Demo_Manager man ON emp.managerId = man.managerId
                                                      INNER JOIN Demo_HR hr ON emp.hrId = hr.hrId 
                                                      WHERE userId = %s '''
            cursor.execute(postgres_employee_select_query,(emp,))
            emp_data=cursor.fetchall()
            dictionary.update(emp_data[0][0])

            while ("" in dictionary):
                dictionary.remove("")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()

        return dictionary

    def insertCompleteEmployeeData(self,empList):

        finalList = []

        count =0

        for emp in empList:
            try:
                empDictionary = self.getEmployeeDetailsWithOutImage(emp)

                obj = self.getColleagueList(emp)

                colleagues = obj['colleague_List']

                colleagueList = self.getColleagueDetailsWithOutImage(colleagues)

                for col in colleagueList[:]:
                    if len(col) == 0:
                        colleagueList.remove(col)

                empDictionary["colleagueDetailsList"] = colleagueList

                count=count+1

                self.insert_complete_data(empDictionary)

                if count%100 ==0:

                    print("Completed finding Employee Details for {0} number of employees".format(count))

            except Exception as ex:

                print(ex)

        return "Employee complete information get Created and stored in the DataBase"

    def createCompleteEmployeeInfoTable(self):

        command = """CREATE TABLE IF NOT EXISTS demo_employee_details (
                           userId VARCHAR(255) PRIMARY KEY,
                           fullName VARCHAR(255) ,
                           location VARCHAR(255),
                           completeInfo json
                           ) """

        self.psdatabase.connect_execute(env.create_query_type, command)

        print("demo_employee_details table created")

    def insert_complete_data(self, emp):
        try:
            connection = psycopg2.connect(user=self.dbuser,
                                          password=self.dbpswd,
                                          host=self.dbhost,
                                          port=self.dbport,
                                          database=self.dbname)

            cursor = connection.cursor()

            postgres_colleague_insert_query = """ INSERT INTO demo_employee_details (userId,fullName,location, completeInfo) VALUES (%s,%s,%s,%s) """


            cursor.execute("""select exists(select 1 from demo_employee_details where userId = %s)""",
                               (emp['userId'],))

            row = cursor.fetchone()

            if row[0] == False:

                manager_record_to_insert = (emp['userId'],emp['defaultFullName'],emp['location'], json.dumps(emp))

                cursor.execute(postgres_colleague_insert_query, manager_record_to_insert)

            connection.commit()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

        finally:
            # closing database connection.
            if (connection):
                cursor.close()
                connection.close()

    def getEmpInfoJsonFile(self,limit):

        employee_details_list=[]

        postgres_employee_select_query="select completeinfo from demo_employee_details limit " + str(limit)

        emp_data = self.psdatabase.connect_execute(env.select_query_type, postgres_employee_select_query)

        for emp in emp_data:

            employee_details_list.append(emp[0])

        return employee_details_list
