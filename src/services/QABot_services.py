from sanic import Blueprint, response
from QABot.QABot_API import generatedQuestion
import json

QABot_API = Blueprint('QABot', __name__)

gq = generatedQuestion()

def get_blueprint():
    """Return the blueprint for the main app module"""
    return QABot_API


# id=None
@QABot_API.route('/Qna', methods=['GET','POST','PUT','DELETE'])
@QABot_API.route('/Qna/<id>', methods=['GET','POST','PUT','DELETE'])
async def generatedQuestion(request, id=None):
    if request.method=='PUT':
        content = request.json
        print(content)
        # id=request.args.get('id')
        return response.json(gq.generatedQuestion(content,id))

    if request.method=='DELETE':
        # id = request.args.get('id')
        return response.json(gq.deleteRow(id))

    if request.method=='GET':
        offset=request.args.get('offset')
        limit=request.args.get('limit')
        # if (offset,limit) is None:
        #     return gq.displayData()
        # else:
        return response.json(gq.displayData(offset,limit))

    if request.method=='POST':
        return response.json(gq.insertData(request.json))


@QABot_API.route('/QABotCreate', methods=['POST'])
async def generatedQuestion(request):
    if request.method=='POST':
        return response.json(gq.createQABotTable(request.json))

# http://localhost:5020/QABot/QABotInbox  --> GET
@QABot_API.route('/QABotInbox', methods=['GET', 'POST'])
async def generatedQuestion(request):
    if request.method=='GET':
        return response.json(gq.QABotInbox())
    elif request.method=='POST':
        return response.json(gq.InsertQABotInbox(request.json))


# @QABot_API.route('/QAFileUploadInbox', methods=['OPTIONS','GET', 'POST'])
# async def generatedQuestion(request):
#     if request.method=='POST':
#         QABotInbox = request.files.get('QABotInbox', None)
#         if QABotInbox == None:
#             return response.json({"message":"No requested file to update data", "type":"E"})
#         return response.json(gq.QABotFileUpload(QABotInbox.body, True))

@QABot_API.route('/QAFileUpload', methods=['GET', 'POST'])
async def generatedQuestion(request):
    if request.method=='POST':
        QABotInbox = request.files.get('QABotData', None)
        isInbox = request.form.get('isInbox', "inbox")
        if QABotInbox == None:
            return response.json({"message": "No requested file to update data", "type": "E"})
        return response.json(gq.QABotFileUpload(QABotInbox.body, isInbox))




