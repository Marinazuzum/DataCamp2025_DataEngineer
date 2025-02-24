import os
import requests
import pandas as pd
import shutil
from io import BytesIO
from zipfile import ZipFile

# Create a temporary directory to save files
tmp_dir = './tmp'
os.makedirs(tmp_dir, exist_ok=True)

# Define the base URL and the months for 2019 and 2020
years = [2019, 2020]
months = [f"{i:02d}" for i in range(1, 13)]

# Initialize the total line count
total_lines = 0

# Function to download and process each file
def process_file(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Save the file to tmp folder
            file_name = url.split('/')[-1]
            file_path = os.path.join(tmp_dir, file_name)
            with open(file_path, 'wb') as f:
                f.write(response.content)

            # Read the CSV into a DataFrame
            df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='ignore', on_bad_lines='warn')
            # Count lines (rows)
            print(f"Lines {file_name} : {len(df)}")
            return len(df)
        else:
            print(f"Failed to download {url}")
            return 0
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return 0

# Iterate through all the years and months, downloading and processing each file
for year in years:
    for month in months:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"
        total_lines += process_file(url)

        # Delete the file after processing
        file_name = url.split('/')[-1]
        file_path = os.path.join(tmp_dir, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)

# Output the total line count
print(f"Total number of lines in all processed files: {total_lines}")

# Cleanup: Remove the tmp directory if no longer needed
shutil.rmtree(tmp_dir)
