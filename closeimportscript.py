#From standard library
import sqlite3
import csv
from email.utils import getaddresses
from datetime import datetime
import statistics
import re

#From PIP
from nameparser import HumanName
from email_validator import validate_email, EmailNotValidError

#Close SDK
from closeio_api import Client

# Create a in-memory SQLite database (simplifies some of the data searching, particularly date/time)

con = sqlite3.connect(":memory:")
cur = con.cursor()

# Ask user for API key
apikey = input("Enter the API Key: ")
api = Client(apikey)

def fix_name_capitalization(name):
    # This function parses the name using the nameparser library, which is a library
    # can normalize names. It then outputs a name with corrected capitalization
    parsed_name = HumanName(name)
    parsed_name.capitalize(force=True)
    fixed_name = parsed_name.full_name
    return fixed_name

def extract_emails(email_string):
    # This function takes an input string with one or more email addresses
    # It extracts and validates those email addresses. 
    
    # Get a list of everything that looks like an email address
    addresses = getaddresses([email_string])

    email_list = []
    for _, email in addresses:
        try:
            # Validate the email address using the email-validator library
            valid_email = validate_email(email)['email']
            email_list.append(valid_email)
        except EmailNotValidError:
            # Ignore invalid email addresses
            pass

    return email_list

def extract_phone_numbers(phone_string):
    # This function uses a regular expression to extract all of the phone numbers
    # It looks for the following pattern [country code]-[area code]- [three numbers]-[four numbers]
    return re.findall(r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', phone_string)


def return_custom_field_id_by_name(api,fieldname):
    # This function looks up the ID of a lead's custom field given the name 
    # It downloads a list of all custom fields, and goes through them until 
    # it finds one with the correct name 

    resp = api.get('custom_field/lead')
    for field in resp["data"]:
        if field["name"] == fieldname:
            return "custom." + field["id"]


company_founded_id = return_custom_field_id_by_name(api,"Company Founded")
company_revenue_id = return_custom_field_id_by_name(api,"Company Revenue") 

# Load the CSV into the sqlite DB
cur.execute("""
CREATE TABLE contacts (
    Company TEXT,
    "Contact Name" TEXT,
    "Contact Emails" TEXT,
    "Contact Phones" TEXT,
    custom_Company_Founded DATE,
    custom_Company_Revenue REAL,
    "Company US State" TEXT
);
""")

with open('datacsv.csv', newline='') as csvfile:
     datacsvreader = csv.reader(csvfile)
     headers = next(datacsvreader, None)
     for contact in datacsvreader:
        #print(contact)
        if contact[4] != "":
            contact[4] = datetime.strptime(contact[4], "%d.%m.%Y").strftime("%Y-%m-%d") #SqLite expects YYYY-MM-DD
        cur.execute("""
        INSERT INTO contacts (
            Company,
            "Contact Name",
            "Contact Emails",
            "Contact Phones",
            custom_Company_Founded,
            custom_Company_Revenue,
            "Company US State"
         ) VALUES (?, ?, ?, ?, ?, ?, ?);
       """, contact)

# Execute the SELECT DISTINCT query to get unique company values
cur.execute("SELECT DISTINCT Company FROM contacts;")

# Fetch all unique company values
unique_companies = cur.fetchall()

#create the lead objects and the contacts

for company in unique_companies:
    # Query the in memory SQL database 
    valid_contacts_with_company_query = cur.execute("""SELECT * FROM contacts WHERE ("Contact Name" != '' OR "Contact Emails" != '' OR "Contact Phones" != '') AND Company = ?;""", (company[0],))
    valid_contacts_with_company = cur.fetchall()

    contacts_with_company_query = cur.execute("""SELECT * FROM contacts WHERE Company = ?;""", (company[0],))
    contacts_with_company = cur.fetchall()

    lead_data = {
    "name": company[0]
    }

    # Get the reveneue, address, and state for the lead from the first contact with the lead/company name 
    # Prefer the first valid contact (ie. one with at least one of a name, an email, or a phone number)
    # if one exists for that lead/company  
    # Fall back to the first item in the CSV for that company even if it is invalid and can't be used 
    # to create a contact 

    if len(valid_contacts_with_company) > 0:
        first_contact_with_company = valid_contacts_with_company[0]
    else:
       first_contact_with_company = contacts_with_company[0]     
    #add the company state 
    if first_contact_with_company[6] != "":
        lead_data["addresses"] = [{"state": first_contact_with_company[6],}]
    #Add the company founding date
    if first_contact_with_company[4] != "": 
        lead_data[company_founded_id] = datetime.strptime(first_contact_with_company[4], "%Y-%m-%d").strftime("%d-%m-%Y") #Convert from the date ordering format stored in the temp SQL database to the format stored in the CSV (and possibly used in Close)
    #Add the company revenue
    if first_contact_with_company[5] != "":
            lead_data[company_revenue_id] = float(first_contact_with_company[5][1:].replace(",", ""))    

    # Create the lead
    resp = api.post('lead', data=lead_data)
    new_lead_id = resp["id"]

    # For every item from the CSV with the minimum data (at least one of name, email, or phone)
    # Create a contact tied to the lead

    for valid_contact in valid_contacts_with_company: 
        new_contact_data = {
            "lead_id": new_lead_id
        }
        #add the name 
        if valid_contact[1] != "":
            new_contact_data["name"] = fix_name_capitalization(valid_contact[1])
        #add the emails 
        if valid_contact[2] != "":
            lead_emails = []
            for lead_email in extract_emails(valid_contact[2]):
                lead_emails.append({"email":lead_email})
            new_contact_data["emails"] = lead_emails
        #add the phone numbers 
        if valid_contact[3] != "":
            lead_phones = []
            for lead_phone in extract_phone_numbers(valid_contact[3]):
                lead_phones.append({"phone":lead_phone})
            new_contact_data["phones"] = lead_phones
        resp = api.post('contact', data=new_contact_data)
            

# Get the date range input from the user in DD-MM-YYYY format
start_date_input = input("Enter the start date (DD.MM.YYYY): ")
end_date_input = input("Enter the end date (DD.MM.YYYY): ")

# Convert the input dates to YYYY-MM-DD format (expeted by Sqlite)
start_date = datetime.strptime(start_date_input, "%d.%m.%Y").strftime("%Y-%m-%d")
end_date = datetime.strptime(end_date_input, "%d.%m.%Y").strftime("%Y-%m-%d")

# Execute the SQLite query to retrieve distinct companies within the given date range
cur.execute("""
SELECT DISTINCT Company
FROM contacts
WHERE custom_Company_Founded BETWEEN ? AND ?;
""", (start_date, end_date))

# Create dictionaries to tract the companies/leads 
# The script will iterate through each of the companies, adding new states to the blank dictionaries 
# as new states are seen, and filling in/updating the data
distinct_companies = cur.fetchall() #List of all companies
states_by_rev = {} # Dictionary that maps states to total revenue 
lead_with_most_rev = {} # Dictonary that maps the states to the lead with the most revenue 
lead_with_most_rev_how_much_rev = {} # Dictionary that keeps track of the revenue produced by the most productive lead. Not used directly in output, but used to decide if the current best lead needs to be updated as the script iterates through each lead
list_of_lead_revs_in_state = {} # A dictionary that maps each state to a list of the revenue of each company, this is an unordered list with just the revenue numbers, this is enough to compute the median revenue of all leads in that state, along with the total revenue.

for company in distinct_companies:
    valid_contacts_with_company_query = cur.execute("""SELECT * FROM contacts WHERE ("Contact Name" != '' OR "Contact Emails" != '' OR "Contact Phones" != '') AND Company = ?;""", (company[0],))
    valid_contacts_with_company = cur.fetchall()

    contacts_with_company_query = cur.execute("""SELECT * FROM contacts WHERE Company = ?;""", (company[0],))
    contacts_with_company = cur.fetchall()

    # Get data about the company from the first item corresponding to that contact in the CSV
    # Ideally use the first valid entry from which a contact was created
    # Fall back to an invalid item if needed 
    if len(valid_contacts_with_company) > 0:
        first_contact_with_company = valid_contacts_with_company[0]
    else:
       first_contact_with_company = contacts_with_company[0]

    company_state = first_contact_with_company[6]

    #if there is not a revenue item, skip to the next company
    if first_contact_with_company[5] == "":
        continue

    current_company_rev = float(first_contact_with_company[5][1:].replace(",", ""))  

    if company_state not in states_by_rev:
        # If this state isn't in the dictionaries, create the items 
        states_by_rev[company_state] = 0
        lead_with_most_rev[company_state] = company
        lead_with_most_rev_how_much_rev[company_state] = current_company_rev
        list_of_lead_revs_in_state[company_state] = []

    # Add the revenue to the aggegrate of the corresponding state 
    states_by_rev[company_state] = states_by_rev[company_state] + current_company_rev

    list_of_lead_revs_in_state[company_state].append(current_company_rev) 

    if lead_with_most_rev_how_much_rev[company_state] < current_company_rev:
        lead_with_most_rev[company_state] = company
        lead_with_most_rev_how_much_rev[company_state] = current_company_rev


with open("output.csv", "w") as file:
    file.write("US State,Total number of leads,The lead with most revenue,Total revenue,Median revenue\n")
    for key in states_by_rev:
        if key != "":
            file.write(key + "," + str(len(list_of_lead_revs_in_state[key])) + "," + lead_with_most_rev[key][0] + "," + str(sum(list_of_lead_revs_in_state[key])) + "," + str(statistics.median(list_of_lead_revs_in_state[key])) + "\n")
          



# Commit the changes and close the connection
con.commit()
con.close()