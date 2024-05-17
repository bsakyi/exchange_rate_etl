# %%
import requests
import json
import pandas as pd
import 
# %%
url = 'https://api.coinbase.com/v2/currencies'
response = requests.get(url)

# %%
api_data = response.text

# %%
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Wrap the JSON string in StringIO
    # json_string_io = StringIO(response.text)
    list_data = json.loads(api_data)
    
    currency_data = list_data.get('data', [])

    # Read the JSON data into a Pandas DataFrame
    df = pd.DataFrame(currency_data)
    
    # Specify the file path where you want to save the CSV file
    csv_file_path = 'scratch_csv.csv'
    
    # Write the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)
    
    print("CSV file saved successfully:", csv_file_path)
else:
    print("Failed to retrieve data. Status code:", response.status_code)


 # Upload the CSV file to S3 bucket
s3.upload_file('links.csv', 'your-s3-bucket-name', 'links.csv')
print("CSV file uploaded to S3 bucket successfully for URL:", url)