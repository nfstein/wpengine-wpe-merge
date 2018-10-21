# WPEngine Coding Review

## Problem Statement
The goal of the coding exercise is to write a program that reads from the provided CSV file and combines its information with data from the API (<http://interview.wpengine.io/v1/docs/>) and outputs a new CSV file.

Given a CSV file with the following columns including a header row containing
Account ID, Account Name, First Name, and Created On
And a Restful Status API:
<http://interview.wpengine.io/v1/accounts/>{account_id}
that returns information in a JSON format of:
{"account_id": 12345, "status": "good", "created_on": "2011-01-12"}
where the “Account ID” in the CSV lines up with the “account_id” in the API
and the “created_on” in API represents when the status was set


For every line of data in the CSV, we want to:

Pull the information from the API for the Account ID
Merge it with the CSV data to output into a new CSV with columns of Account ID, First Name, Created On, Status, and Status Set On
The program must be invoked as follows (we have it scripted, so the format is important). Depending on your language choice, wpe_merge may need to be bash script (or a .bat) that actually invokes your code.

wpe_merge \<input_file\> \<output_file\>

For example:  wpe_merge data/input.csv output.csv
You may use any open source / publicly available libraries for that language that you need, just include what they are. Feel free to send us any questions that you have or clarifications that you need to rd-interview@wpengine.com. We can evaluate your submission on macOS, Linux, or Windows.
