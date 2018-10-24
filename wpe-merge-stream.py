from pandas import to_datetime #good flexible date interpereter
from requests import get
from time import sleep
from sys import argv
import os
import csv

def verify_and_clean_input(input_dict):
    """
    given dictionary with ["Account ID", "Created On", "First Name"] keys,
    convert Account ID to integer and Created On to formatted datestring (or "" if invalid)
    
    >>> verify_and_clean_input({"Account ID": 314159,"Account Name": "superman","First Name": "Ka-el","Created On": "1/12/13"})
    (True, {'Account ID': 314159, 'Created On': '01/12/13', 'First Name': 'Ka-el'}, '')
    >>> verify_and_clean_input({'Account ID': 'asdf', 'First Name': '', 'Created On': ' None'})
    (False, {}, 'not a valid account id')
    >>> verify_and_clean_input({'Account ID': '', 'First Name': '', 'Created On': ' whenever'})
    (False, {}, 'not a valid account id')
    >>> verify_and_clean_input({'Account ID': 210, 'First Name': '', 'Created On': ' whenever'})
    (True, {'Account ID': 210, 'Created On': '', 'First Name': ''}, '')
    """
    
    output_dict = {}
    try:
        output_dict["Account ID"] = int(input_dict["Account ID"])
        if output_dict["Account ID"] < 0:
            return False, {}, "not a valid account id"
    except ValueError:
        return False, {}, "not a valid account id"
    
    try:
        output_dict["Created On"] = to_datetime(input_dict["Created On"], errors='coerce').strftime("%x")
    except ValueError:
        output_dict["Created On"] = ""
    
    output_dict["First Name"] = input_dict["First Name"]
        
    return True, output_dict, ""

def extend(input_dict, response_dict):
    """
    folds status and created_on from response into input and returns a status message
    input_dict = {"Account ID": int} 
    response_dict = {"message": str, "status": str, "created_on": str} from a query response dict
    -> {"Account ID": int, "Status": str, "Status Set On": str}, message
    where "Status Set On" is created_on and message is a status message from the response or from lack of status/date
    
    examples:       # verified with doctest
    >>> extend({'Account ID': 314159, 'Created On': '01/12/13', 'First Name': 'Ka-el'}, {'account_id': 314159, 'status': 'good', 'created_on': '2012-01-12', 'message': '', 'valid': True})
    ({'Account ID': 314159, 'Created On': '01/12/13', 'First Name': 'Ka-el', 'Status': 'good', 'Status Set On': '01/12/12'}, 'Processed Account ID: 314159 - Status: good ')
    >>> extend({"Account ID": 271},{'message': "", "valid": True, 'status': 'good', 'created_on': '2011-03-22'})
    ({'Account ID': 271, 'Status': 'good', 'Status Set On': '03/22/11'}, 'Processed Account ID: 271 - Status: good ')
    >>> extend({"Account ID": 21},{'detail': 'Not found.', 'message': 'Account ID - 21 - Not found.', 'valid': False})
    ({'Account ID': 21, 'Status': '', 'Status Set On': ''}, 'Account ID - 21 - Not found.')
    >>> extend({"Account ID": 21},{'detail': 'Not found.', 'message': 'Account ID - 21 - Not found.', 'valid': True})
    ({'Account ID': 21, 'Status': '', 'Status Set On': ''}, 'No created_on date included for Account ID - 21')

    """
    
    output_dict = input_dict
    output_dict["Status"] = ""
    output_dict["Status Set On"] = ""
    message = ""
    account_id = input_dict["Account ID"]

    if response_dict["valid"]: 
        # set Status Set On to response's created_on value for account_id
        # could introduce check for status in set ("good", "bad", "")
        if "status" in response_dict:
            output_dict["Status"] = response_dict["status"]
            message = f"Processed Account ID: {account_id} - Status: {response_dict['status']} "
        else:
            message = f"No status data included for Account ID - {account_id} "
            
        # only try and set status date if status successfully set
        if "created_on" in response_dict and "status" in response_dict:
            try:
                # set Status Set On to response's created_on value for account_id
                # here choosing to write "" in event of bad date
                output_dict["Status Set On"] = \
                    to_datetime(response_dict["created_on"], errors='raise')\
                    .strftime("%x")
            except ValueError:
                # has status and created on but created on not valid
                message = f"Invalid date included for Account ID - {account_id}"
        else:
            if "status" not in response_dict:
                message = f"No created_on date included for Account ID - {account_id}"
    else:
        message = response_dict["message"]
    return output_dict, message

# query accounts/account_id and determine if non-error json is returned
# returns validity of response and the response
def query(account_id, base_url="http://interview.wpengine.io/v1/accounts"):
    
    data_is_valid = True
    query_success = False
    returned_dict = {}
    message = ""
    attempt_num = 0
    
    while (attempt_num < 3) and not query_success:
        sleep(3**attempt_num-1) #backoff [0,2,8] seconds
        attempt_num += 1
        query_string = f"{base_url}/{account_id}"
        request = get(query_string)

        #successful query case
        if request.status_code == 200:
            returned_dict = request.json()
            data_is_valid = True
            query_success = True
        else:
            data_is_valid = False
        
        if request.status_code > 200:
            try: # check parseable response
                returned_dict = request.json()
                message = f"Query - {query_string} - {returned_dict['detail']}"
                query_success = True # but data not found
            except ValueError:
                message = f"Response not json parseable for query {query_string}"
                query_success = False # requery
    
    returned_dict["message"] = message
    returned_dict["valid"] = data_is_valid
    return returned_dict

def handle_arguments(args):
    """
    check for validity of included arguments, create directory for output, 
    determine whether or not to overwrite the base url and whether url is valid
    args[0] meta data
    args[1] input_filepath
    args[2] output_filepath
    args[3] url *optional
    
    few examples because exhaustive examples would rely on an specified
    input file existing
    example:
    nontesting >> handle_arguments(["","test-data.csv", "out.csv"])
    (True, 'test-data.csv', 'out.csv', 'http://interview.wpengine.io/v1/accounts', '')
    nontesting >> handle_arguments(["","test-data.csv", "out.csv", "www.google.com"])
    (True, 'test-data.csv', 'out.csv', 'www.google.com', '')
    >>> handle_arguments(["",""])
    (False, '', '', '', 'ERROR - invoke program as: wpe_merge <input_filepath.csv> <output_filepath.csv> <optional: base_url for queries>')

    """
    
    if __name__ == "__main__":
    import doctest
    doctest.testmod()
    
    valid = False
    
    if len(args) != 3 and len(args) != 4:
        return False,"","","","ERROR - invoke program as: wpe_merge <input_filepath.csv> <output_filepath.csv> <optional: base_url for queries>"
    input_filepath = args[1]
    output_filepath = args[2]
    
    
    if len(args) == 4:
        base_url = args[3]
        # / between base_url and account_id added in query
        if base_url[-1] == "/": 
            base_url=base_url[:-1]
        #validate url
        try:
            test_status = get(base_url).status_code
            if test_status != 200:
                return False,"","","","ERROR - Included Url invalid, returns {test_status}"
        except: #MissingSchema exception not found
            return False,"","","","ERROR - Included Url invalid"
    else:
        base_url = "http://interview.wpengine.io/v1/accounts"
    
    if not os.path.exists(input_filepath):
        return  False,"","","", f"ERROR - input filepath {input_filepath} does not exist"
        
    if input_filepath[-4:] != ".csv" or output_filepath[-4:] != ".csv" or input_filepath == output_filepath: 
        return  False,"","","", "ERROR - filepaths must be unique valid csv files"
    
    path_to_output = output_filepath.rpartition("/")[0]
    if path_to_output != "":
        os.makedirs(path_to_output, exist_ok=True)
    valid = True
    return valid, input_filepath, output_filepath, base_url,""

def main(args):
    
    """
    given an input csv and and output path, read input csv 
    and combine with data from api and stream results to csv file at output
    
    input csv header form of:
        ["Account ID", "First Name", "Created On"]
    output csv header form of:
        ["Account ID", "First Name", "Created On", "Status", "Status Set On"]
    """
    
    args_are_valid, input_filepath, output_filepath, base_url, message = handle_arguments(args)
    if not args_are_valid:
        return print(message)
    
    with open(input_filepath, newline="") as input_csv:
        csvreader = csv.reader(input_csv, delimiter=",",)

        needed_input_columns = ["Account ID","First Name", "Created On"]
        needed_output_columns = ["Account ID","First Name", "Created On", "Status", "Status Set On"]
        headers = next(csvreader) #grab first row as headers
        if not set(needed_input_columns).issubset(headers):
            print('ERROR - input csv must contain columns ["Account ID","First Name", "Created On"] as headers')

        with open(output_filepath, mode = "w", newline = "") as output_csv:
            csvwriter = csv.DictWriter(output_csv, fieldnames = needed_output_columns)
            csvwriter.writeheader()

            index_of = {}
            for index,header in enumerate(headers):
                index_of[header] = index
            write_dict = {}

            #Loop through inputfile
            for row in csvreader:
                still_valid = True
                if len(row) != len(headers):
                    message = "ERROR - csv row has incomplete data"
                    still_valid = False
                if still_valid:
                    # extract data from row, columns can be in any order
                    for column in needed_input_columns:
                        write_dict[column] = row[index_of[column]]
                    still_valid, write_dict, message = verify_and_clean_input(write_dict)
                if still_valid:
                    write_dict, message = extend(write_dict, query(write_dict["Account ID"], base_url))
                    #only write to csv if all input data valid, query data nulled out if invalid
                    csvwriter.writerow(write_dict) 
                print(message)

            output_csv.close() 
        input_csv.close()
        
main(argv)
