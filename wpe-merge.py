import pandas as pd
from numpy import nan
import requests
import os
import sys

# clean data, set types, remove nan account_ids, create columns needed for output
def data_cleaner(raw_data):
    #pick columns
    clean_data = raw_data
    clean_data = pd.DataFrame(clean_data[["Account ID","First Name", "Created On"]])

    # might not necessarily want to assume the type of the Account ID, 
    # but definitely dont want .0s on the end for api query
    # coercing yields NaNs where errors occur which are filtered later
    clean_data["Account ID"] = pd.to_numeric(clean_data["Account ID"], errors='coerce')
    # set as datetime to check validity and match "Status Set On" data
    clean_data["Created On"] = pd.to_datetime(clean_data["Created On"], errors='coerce')

    # clean data, create contract account_id is unique integer not na, and one other property must be non na.
    clean_data = clean_data.dropna(axis = 0, how="any",subset=["Account ID"]) # must have Account ID
    clean_data = clean_data.dropna(axis = 0, how="all",subset=["First Name","Created On"]) # account with all empty data
    clean_data["Account ID"] = clean_data["Account ID"].astype("int", errors='ignore')

    # create columns for output, its a merge, so if there's data in the columns originally, keep it
    try:
        clean_data["Status"] = raw_data["Status"]
        clean_data["Status Set On"] = raw_data["Status Set On"]
    except KeyError:
        clean_data["Status"] = nan
        clean_data["Status Set On"] = nan

    # account id acts as index and must be unique
    clean_data = clean_data.drop_duplicates(subset=["Account ID"])
    clean_data = clean_data.set_index("Account ID")
    return clean_data

# query accounts/account_id and determine if non-error json is returned
# returns validity of response and the response
def query(account_id):
    is_valid = True
    returned_dict = {}
    request = requests.get(f"http://interview.wpengine.io/v1/accounts/{account_id}")
    try: # check parseable response
        returned_dict = request.json()
    except ValueError:
        print(f"response not json parsable for account id {account_id}")
        is_valid = False
    try: # check not error message
        print(str(account_id) + " " + returned_dict["details"])
        is_valid = False
    except KeyError:
        pass
    return is_valid, returned_dict

# query for each account_id and merge is_valid  
def query_each_and_merge(df_in):
    
    df_out = df_in
    total_ids = df_out.index.size
    
    for index, account_id in enumerate(df_out.index.tolist()):
        is_valid, returned_dict = query(account_id)
        
        # go ahead and try again once after backoff in case of connection problem, 
        # having problem with first query being denied bc of network problems
        if not is_valid: 
            time.sleep(1)
            is_valid, returned_dict = query(account_id)

        if is_valid: #set new df_out values
            try:
                # set Status Set On to response's created_on value for account_id
                # could introduce check for status in set ("good", "bad", "")
                df_out.loc[account_id, "Status"] = returned_dict["status"]
                print(f"processed id - {account_id} ", end="\t")
            except KeyError:
                print(f"no status data included for account id - {account_id}", end="\t")
            
            # only try and set status date if status successfully set
            try:
                # set Status Set On to response's created_on value for account_id
                # here choosing to write nan in event of bad date
                df_out.loc[account_id, "Status Set On"] = pd.to_datetime(returned_dict["created_on"], errors='coerce') 
            except KeyError:
                print(f"no created_on date included for account id - {account_id}", end="\t")
        print("--" + str(index+1) + "/" + str(total_ids)) #progress

    return df_out

def main(args):
    
    # could break out into func
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
        
    if input_filepath[-4:] != ".csv" or output_filepath[-4:] != ".csv": 
        return print("ERROR - filepaths must be valid csv files")

    # importing with pandas assuming reasonable size of data
    raw_data = pd.read_csv(input_filepath)
    if not set( ["Account ID","First Name", "Created On"] ).issubset(raw_data.columns.tolist()):
        return print('ERROR - input csv must contain columns: "Account ID","First Name", "Created On"')
    
    df = data_cleaner(raw_data)
    df = query_each_and_merge(df)
    df["Status Set On"] = pd.to_datetime(df["Status Set On"], errors='coerce') #drops seconds
    df.to_csv(output_filepath)
    
main(sys.argv)