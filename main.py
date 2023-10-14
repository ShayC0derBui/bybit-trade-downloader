import requests
import os
import gzip
import re
from time import sleep

# Replace this with the base URL of your directory listing
base_url = "https://public.bybit.com/trading/"

# Create a directory to store the downloaded files
data_directory = "data"
os.makedirs(data_directory, exist_ok=True)

# Create a directory to store the extracted files
extracted_directory = "extracted"
os.makedirs(extracted_directory, exist_ok=True)

# Get the page content and parse it to extract links
response = requests.get(base_url)
if response.status_code == 200:
    page_content = response.text

    # Use regular expressions to extract links to subdirectories
    subdirectory_links = re.findall(r'<a href="([^"]+/)">.*?</a>', page_content)

    for subdirectory_link in subdirectory_links:
        subdirectory_url = base_url + subdirectory_link

        # Create a subdirectory for the current subdirectory_link
        subdirectory_name = os.path.join(data_directory, os.path.basename(subdirectory_link.rstrip('/')))
        os.makedirs(subdirectory_name, exist_ok=True)

        # Create a separate folder for the subdirectory link in the "extracted" directory
        extracted_subdirectory = os.path.join(extracted_directory, os.path.basename(subdirectory_link.rstrip('/')))
        os.makedirs(extracted_subdirectory, exist_ok=True)

        # Get the page content of the subdirectory and parse it to extract file links
        subdirectory_response = requests.get(subdirectory_url)
        if subdirectory_response.status_code == 200:
            subdirectory_content = subdirectory_response.text

            # Use regular expressions to extract filenames
            file_links = re.findall(r'<a href="([^"]+)">.*?</a>', subdirectory_content)

            for file_link in file_links:
                sleep(0.2)
                file_url = subdirectory_url + file_link
                file_name = os.path.join(subdirectory_name, os.path.basename(file_link))

                # Download the file
                response = requests.get(file_url)
                if response.status_code == 200:
                    with open(file_name, 'wb') as file:
                        file.write(response.content)

                    # Check if the downloaded file is a GZIP file and extract it using gzip library
                    if file_name.endswith('.gz'):
                        extracted_file_name = os.path.join(extracted_subdirectory, os.path.basename(file_link)[:-3])
                        with open(file_name, 'rb') as f_in, open(extracted_file_name, 'wb') as f_out:
                            with gzip.open(f_in, 'rb') as g_in:
                                f_out.write(g_in.read())
                        print(f"Downloaded and extracted: {extracted_file_name}")
                    else:
                        print(f"Downloaded: {file_name}")
                else:
                    print(f"Failed to download: {file_name}")

        else:
            print(f"Failed to access the subdirectory: {subdirectory_url}")

else:
    print("Failed to access the directory listing.")
