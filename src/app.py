from exceptions.shutdown import ShutdownMiddleware
from config.env import env
from threading import RLock
from flask import Flask, request, send_file, jsonify, Blueprint
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from sanic import Sanic, Blueprint, response
from sanic_cors import CORS, cross_origin


from services import QABot_services
from dashboard.dashboardapi import dashboardapi
from analytics.analytics import analyticsappi


_dashboardapi = dashboardapi()
_analyticsappi = analyticsappi()


app = Sanic(__name__)

# app = Flask(__name__)
app.is_dead = False
lock = RLock()
CORS(app)

# app = Flask(__name__)
# app.is_dead = False
# app.wsgi_app = ShutdownMiddleware(app)
# lock = RLock()
# CORS(app)

bp = Blueprint('analytics', __name__)

zee = Blueprint('zee', __name__)

### swagger specific ###
# SWAGGER_URL = '/swagger'
# API_URL = '/static/swagger.json'
# SWAGGERUI_BLUEPRINT = get_swaggerui_blueprint(
#     SWAGGER_URL,
#     API_URL,
#     config={
#         'app_name': "Employee-Details-Api"
#     }
# )
# app.blueprint(SWAGGERUI_BLUEPRINT, url_prefix=SWAGGER_URL)
### end swagger specific ###

@app.route("/")
@app.route("/healthz")
def healthz(request):
    if app.is_dead:
        return ("DEAD", 500)
    acquired = lock.acquire(timeout=5)
    if not acquired:

        return ("BUSY", 503)
    lock.release()

    return ("OK", 200)

# This api is not used by the DashBoard Team
@app.route('/messages')
def dashboardusermessages(request):

    return response.json(_dashboardapi.dashboardusermessages())

@bp.route('/getfallbackDetails')
def analyticsFallBackActions(request):

    return response.json(_analyticsappi.getFallbackData())

@bp.route('/getSupportAndLiveChatDetails')
def supportAndLiveChatDetails(request):

    return response.json(_analyticsappi.getSupportAndLiveChatDetails())

@bp.route('/getLikeDislikeDetails')
def likeDislikeDetails(request):

    return response.json(_analyticsappi.getLikeandDislikeDetails())

@bp.route('/messages')
def analyticsusermessages(request):

    return response.json(_analyticsappi.postgreAnalyticsuserMessage())

@bp.route('/employeedetails')
def analyticsemployeedetails(request):

    return response.json(_analyticsappi.getEmpInfo())
    # return jsonify(_analyticsappi.getCompleteEmployeeData())

@bp.route('/conversations')
def analyticsconversations(request):

    return response.json(_analyticsappi.analyticsconversation())

# first insertEmployeeData() into the db then insertAdditionalEmployeeData()

@bp.route('/insertEmployeeData',methods=['POST'])
def EmployeeData(request):

    return _analyticsappi.insertEmployeeDB()

@bp.route('/getEmployeeData')
def getEmployees(request):

    return response.json(_analyticsappi.getEmployeeData())

@bp.route('/insertAdditionalEmployeeData',methods=['POST'])
def createCompleteEmployeeData(request):

    return response.json(_analyticsappi.createAndinsertEmployeeCompleteData())

app.blueprint(bp, url_prefix="/Analytics")
app.blueprint(QABot_services.get_blueprint(), url_prefix='/QABot')

if __name__ == '__main__':
    app.run(port=env.appport, debug=False, host='0.0.0.0')
