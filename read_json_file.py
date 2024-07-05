import json


def read_json_file(json_file_path, file_no=0):
    """
    Read a json file and return the python object
    :param file_no:
    :param json_file_path:
    :return: python object
    """
    print()
    print('> Reading file # {} - {} '.format(file_no, json_file_path))

    out_ds = None
    try:
        with open(json_file_path) as json_file:
            out_ds = json.load(json_file)
    except IOError:
        print("Error in reading input json file : {}", json_file_path)
        raise FileNotFoundError
    return out_ds
