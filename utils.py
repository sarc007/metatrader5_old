
def remove_dash_add_underscore(str_dash):
    '''

    :param str_dash: # print(remove_dash_add_underscore('2020-08'))
    :return: '2020_08'
    '''
    str_splited = str_dash.split("-")
    str_underscore =''
    for str in str_splited:
        if len(str_underscore) == 0:
            str_underscore = str
        else:
            str_underscore = str_underscore + '_' + str

    return str_underscore

def remove_underscore_add_dash(str_underscore):
    '''

    :param str_underscore: # print(remove_underscore_add_dash('2020_08'))
    :return: '2020-08'
    '''
    str_splited = str_underscore.split("_")
    str_dash =''
    for str in str_splited:
        if len(str_dash) == 0:
            str_dash = str
        else:
            str_dash = str_dash + '-' + str

    return str_dash




