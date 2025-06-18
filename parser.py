import json
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import mailer


# API URL
url = "https://rest.avatax.com/api/v2/companies/323489/customers?$include=active_certificates&$top=1000000"

# Authentication credentials
username = "2000170427"
password = "D825A1369ED81F8F"

# Make the GET request with Basic Authentication
response = requests.get(url, auth=HTTPBasicAuth(username, password))

print(f"{response}")

response_dict = {
    'status_code': response.status_code,
    'headers': dict(response.headers),
    'json_data': response.json() if response.status_code == 200 else None,
    'text': response.text
}

# Write the whole response to a .txt file in pretty JSON format
with open('response.txt', 'w') as file:
    json.dump(response_dict, file, indent=4, separators=(',', ': '), ensure_ascii=False)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response if successful
    data = response.json()
    
    with open("data.txt", "w") as file:
            # Save the data as a pretty-printed JSON string in the text file
            json.dump(data, file, indent=4)
        
    print("Data saved to 'customers_data.txt'.")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")



# Step 1: Read the JSON file
with open('data.txt', 'r') as file:
    data = json.load(file)

# Step 2: Extract necessary fields
extracted_data = []

# Loop through the "value" list in the JSON structure
for record in data['value']:
    # Extract top-level fields
    customer_code = record.get('customerCode')
    alternate_id = record.get('alternateId')
    name = record.get('name')
    created_date = record.get('createdDate')
    modified_date = record.get('modifiedDate')
    
    # Initialize values for exemption number, certificate ID, and expiration date
    exemption_number = None
    certificate_id = None
    expiration_date = None
    
    # Check if there are active certificates
    if record.get('activeCertificates'):
        for cert in record['activeCertificates']:

            expzone = cert.get('exposureZone')
            expid = expzone.get('id', None)
            expname = expzone.get('name', None)
            exptag = expzone.get('tag', None)

            certificate = cert.get('certificate')  # Access certificate info
            if certificate:
                
                exemption_number = certificate.get('exemptionNumber', None)
                if exemption_number in ["NA", "N/A", "N\\A", "na"]:  # Check if it's one of the invalid values
                    exemption_number = 'Not available'  # Replace with 'Not available'
                certificate_id = certificate.get('id', None)  # Access certificate ID
                expiration_date = certificate.get('expirationDate', None)  # Access expiration date
                    # Assuming there is only one certificate per customer

                # Append extracted data to the list
                extracted_data.append({
                    'customerCode': str(customer_code),
                    'alternateId': str(alternate_id),
                    'name': str(name),
                    'createdDate': str(created_date),
                    'modifiedDate': str(modified_date),
                    'exemptionNumber': exemption_number,
                    'certificateId': certificate_id,  # Add certificate ID
                    'expirationDate': expiration_date,  # Add expiration date
                    'exposureZone_id' : expid,
                    'exposureZone_name' : expname,
                    'exposureZone_tag' : exptag
                })


# Step 3: Convert extracted data to a pandas DataFrame
df = pd.DataFrame(extracted_data)

# step 4: Save the file in the local path 
original_df = df.copy()
original_df.to_excel("All_entries_without_exemption_df.xlsx", index=False)

original_df = original_df[original_df["exemptionNumber"].notna()]
original_df.to_excel("Original_df.xlsx", index=False)

old_df = pd.read_excel('extracted_data.xlsx')

# Replace NaN with empty string after loading the data
original_df = original_df.where(pd.notnull(original_df), "")
old_df = old_df.where(pd.notnull(old_df), "")

# Convert 'alternateId' column to numeric, coercing errors to NaN, then fill NaN with a default value like 0
original_df['alternateId'] = pd.to_numeric(original_df['alternateId'], errors='coerce').fillna(0).astype(int)
old_df['alternateId'] = pd.to_numeric(old_df['alternateId'], errors='coerce').fillna(0).astype(int)
original_df['customerCode'] = pd.to_numeric(original_df['customerCode'], errors='coerce').fillna(0).astype(int)
old_df['customerCode'] = pd.to_numeric(old_df['customerCode'], errors='coerce').fillna(0).astype(int)
original_df['certificateId'] = pd.to_numeric(original_df['certificateId'], errors='coerce').fillna(0).astype(int)
old_df['certificateId'] = pd.to_numeric(old_df['certificateId'], errors='coerce').fillna(0).astype(int)

# Convert 'customerCode' column to string in both DataFrames (assuming it's the merge key)
original_df['customerCode'] = original_df['customerCode'].astype(str)
old_df['customerCode'] = old_df['customerCode'].astype(str)
original_df['alternateId'] = original_df['alternateId'].astype(str)
old_df['alternateId'] = old_df['alternateId'].astype(str)
original_df['certificateId'] = original_df['certificateId'].astype(str)
old_df['certificateId'] = old_df['certificateId'].astype(str)

print(old_df.dtypes)
print(original_df.dtypes)

# 1. Finding records that exist in both dataframes
common_df = pd.merge(original_df, old_df, how='inner')

# 2. Finding records that exist only in original_df and not in old_df
only_in_original_df = pd.merge(original_df, old_df, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

# 3. Finding records that exist only in old_df and not in original_df
only_in_old_df = pd.merge(original_df, old_df, how='right', indicator=True).query('_merge == "right_only"').drop('_merge', axis=1)

common_df["deleted_flag"] = "common"
only_in_original_df["deleted_flag"] = "Original"
only_in_old_df["deleted_flag"] = "Old"

concatenated_df = pd.concat([common_df, only_in_original_df, only_in_old_df], ignore_index=True)

# Convert '0' back to None (or np.nan if you prefer)
concatenated_df['alternateId'] = concatenated_df['alternateId'].astype(int)
concatenated_df['customerCode'] = concatenated_df['customerCode'].astype(int)
concatenated_df['certificateId'] = concatenated_df['certificateId'].astype(int)


concatenated_df['alternateId'] = concatenated_df['alternateId'].replace(0, None)
concatenated_df['customerCode'] = concatenated_df['customerCode'].replace(0, None)
concatenated_df['certificateId'] = concatenated_df['certificateId'].replace(0, None)

concatenated_df.to_excel("D:\\Jason_api_parsing\\final_data\\Full_data_processed.xlsx", index=False)

# Step 5: save to excel without index
original_df.astype(str)
original_df.to_excel('extracted_data.xlsx', index=False)


mailer.sender("D:\\Jason_api_parsing\\final_data")