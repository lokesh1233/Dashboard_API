class DBProcessUtil():

    def __init__(self):
        pass

    def validateFieldsUpdate(self, data, keys, validateFields):
        values = []
        updateString = ""
        for key in keys:
            if data.get(key, None) != None:
                updateString += f", {key} = %s"
                if validateFields.get(key, None) != None and callable(
                        validateFields.get(key, None)) and data.get(key, None) != None:
                    values.append(validateFields[key](data[key]))
                else:
                    values.append(data[key])
        updateString = updateString[1:]
        return updateString, values

    def validateFields(self, data, fields, isUpdate=False, validateFields= None):
        if isUpdate == True:
            return self.validateFieldsUpdate(data, fields, validateFields)
        return self.validateFieldsInsert(data, fields, validateFields)

    def validateFieldsInsert(self, data, keys, validateFields):
        values = []
        for key in keys:
            print(key,data[key])
            if validateFields.get(key, None) != None and callable(
                        validateFields.get(key, None)) and data.get(key, None) != None:
                values.append(validateFields[key](data[key]))
            else:
                values.append(data.get(key, None))

        return keys, values

    def handleResponses(self, res, value):
        if type(res) == dict:
            msg = res.get('message', None)
            if res.get('type', None) == "S" and type(msg) == str and value.upper() == msg.upper():
                return msg, "S"
            return msg, "E"


