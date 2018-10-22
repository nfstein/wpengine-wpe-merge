from pandas import to_datetime #good flexible date interpereter
from requests import get
import os
from sys import argv
import csv

# given dict with ["Account ID", "Created On", "First Name"] keys,
# convert Account ID to integer and Created On to formatted datestring
def verify_and_clean_input(input_dict):
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

# queries api for "Status" and "Created On"(renamed "Status Set On") given a dict with "Account ID" key
# returns filled out dict and success/failure message
def query_and_extend(input_dict):
    output_dict = input_dict
    output_dict["Status"] = ""
    output_dict["Status Set On"] = ""
    account_id = input_dict["Account ID"]
    is_valid, returned_dict, message = query(account_id)
        
    # go ahead and try again once after backoff in case of connection problem, 
    # having problem with first query being denied bc of network problems
    if not is_valid: 
        time.sleep(1)
        is_valid, returned_dict, message = query(account_id)

    if is_valid: 
        try:
            # set Status Set On to response's created_on value for account_id
            # could introduce check for status in set ("good", "bad", "")
            output_dict["Status"] = returned_dict["status"]
            status_returned = True
            message = f"Processed Account ID: {account_id} - Status: {returned_dict['status']} "
        except KeyError:
            status_returned = False
            message = f"No status data included for Account ID - {account_id} "
            
        if status_returned: # only try and set status date if status successfully set
            try:
                # set Status Set On to response's created_on value for account_id
                # here choosing to write nan in event of bad date
                output_dict["Status Set On"] = \
                    to_datetime(returned_dict["created_on"], errors='raise')\
                    .strftime("%x")
            except KeyError:
                message = f"No created_on date included for Account ID - {account_id}"
            except ValueError:
                message = f"Invalid date included for Account ID - {account_id}"
    return output_dict, message

# query accounts/account_id and determine if non-error json is returned
# returns validity of response and the response
def query(account_id):
    is_valid = True
    returned_dict = {}
    message = ""
    request = get(f"http://interview.wpengine.io/v1/accounts/{account_id}")
    
    try: # check parseable response
        returned_dict = request.json()
    except ValueError:
        message = f"response not json parseable for account id {account_id}"
        is_valid = False
        
    try: # check not error message
        message = f"Account ID - {account_id} - {returned_dict['details']}"
        is_valid = False
    except KeyError:
        pass
    return is_valid, returned_dict, message

def main(args):
    
    if len(args) != 3:
        return print("ERROR - invoke program as: wpe_merge <input_file.csv> <output_file.csv>")
    input_filepath = args[1]
    output_filepath = args[2]
    
    if not os.path.exists(input_filepath):
        return print(f"ERROR - input filepath {input_filepath} does not exist")
    
    path_to_output = output_filepath.rpartition("/")[0]
    if path_to_output != "":
        os.makedirs(path_to_output, exist_ok=True)
    
    if input_filepath[-4:] != ".csv" or output_filepath[-4:] != ".csv": 
        return print("ERROR - filepaths must be valid csv files")
    
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
                    write_dict, message = query_and_extend(write_dict)
                    #only write to csv if all input data valid, query data nulled out if invalid
                    csvwriter.writerow(write_dict) 
                print(message)

            output_csv.close()
        input_csv.close()
        
main(argv)
