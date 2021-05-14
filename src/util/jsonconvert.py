
# def flatten_json(y):
#     out = {}
#
#     def flatten(x, name=''):
#         if type(x) is dict:
#             for a in x:
#                 flatten(x[a], name + a + '_')
#         elif type(x) is list:
#             i = 0
#             for a in x:
#                 flatten(a, name + str(i) + '_')
#                 i += 1
#         else:
#             out[name[:-1]] = x
#     flatten(y)
#     return out


def flatten_json(y):
    out = {}
    arr = []
    carr = []

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], a)
        elif type(x) is list:
            arr.append(x)
            return out;
        else:
            out[name] = x
        # return out
    flatten(y)
    cout = out.copy()
    for a in arr[0]:
        out = cout.copy()
        flatten(a)
        carr.append(out)

    return cout if len(carr) == 0 else carr

def flatten_nested_json(y):
    if type(y) == list:
        nested_json = []
        for item in y:
            nested_json.extend(flatten_json(item))
        return nested_json
    elif type(y) == dict:
        return flatten_json(y)
    else:
        return "only list and dictionary allowed in nested json"

