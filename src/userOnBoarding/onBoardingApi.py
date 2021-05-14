from config.env import env
from userOnBoarding.onBoardingUtil import OnBoarding
from sanic import Sanic,request,Blueprint,response
from sanic_cors import CORS
import logging
from onBoardingUtil.userLimit import triggerMail

app = Sanic(__name__)
app.is_dead = False
CORS(app)
log=logging.getLogger(__name__)

o360 = Blueprint('o360-Dashboard', __name__ )
_onbardingapi=OnBoarding()


@o360.route('/userOnBoarding',methods=['GET','POST','PUT','DELETE'])
@triggerMail
def userOnBoarding(request):

    if request.method=='POST':
        print("Post method")
        if 'file' in request.files:
            return response.json({"message": "No requested file to update data", "type": "E"})
        elif 'employee' in request.files:
            employee = request.files.get('employee')
            return response.json(_onbardingapi.readUploadFile(employee.body))
        else:
            request_data=request.json
            return response.json(_onbardingapi.insertEmployeeData(request_data))

    if request.method=='DELETE':
        id = request.args.get('id')
        _onbardingapi.deleteEmployeeData(id)
        # return text("Employee data deleted successfully")
        return response.json(_onbardingapi.deleteEmployeeData(id))

    if request.method=='PUT':
        request_data = request.json
        id = request_data.get('empid', None)

        if id == None:
            return response.json({"message": "No id to update details", "type": "E"})
        del request_data['empid']
        return response.json(_onbardingapi.updateEmployee(request_data, id))

    if request.method=='GET':
        id = request.args.get('id')
        if id is None:
            return response.json(_onbardingapi.displayEmployee())
        else:
            return response.json(_onbardingapi.displayEmployee(id))

