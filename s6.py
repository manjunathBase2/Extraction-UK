import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load the Excel file using pandas
file_path = 'input.xlsx'
df = pd.read_excel(file_path)

# Define a function to extract data for a single URL
def extract_data(index, row):
    base_url = row['Source of Truth']  # Assuming 'Source of Truth' is the column name
    result = {'index': index}

    if pd.notna(base_url):  # Check if the URL is not empty
        try:
            # Fetch the base URL content for <p class="lead"> extraction
            response_base = requests.get(base_url)
            response_base.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the HTML content for the base URL
            soup_base = BeautifulSoup(response_base.content, 'html.parser')

            # Extract the text from the <p class="lead"> tag for "Text Extract"
            lead_paragraph = soup_base.find('p', class_='lead')
            if lead_paragraph:
                lead_text = lead_paragraph.get_text(strip=True)
                result['Text Extract'] = lead_text
            else:
                result['Text Extract'] = 'Lead paragraph not found'

            # Try to fetch the "Ch3 Text" by appending "/chapter/3" to the base URL
            ch3_url = base_url + "/chapter/3"
            response_ch3 = requests.get(ch3_url)
            response_ch3.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the HTML content for "Ch3 Text"
            soup_ch3 = BeautifulSoup(response_ch3.content, 'html.parser')

            # Find the <div> with title="3 The technologies"
            ch3_div = soup_ch3.find('div', class_='chapter', title="3 The technologies")

            # Extract the text from the <p> tags within the <div>
            if ch3_div:
                ch3_paragraphs = ch3_div.find_all('p')
                ch3_text = ' '.join([p.get_text(strip=True) for p in ch3_paragraphs])
                result['Ch3 Text'] = ch3_text
            else:
                result['Ch3 Text'] = 'Technologies section not found'

        except Exception as e:
            # Store error for "Text Extract" and "Ch3 Text"
            result['Text Extract'] = f'Error: {str(e)}'
            result['Ch3 Text'] = f'Error: {str(e)}'

        try:
            # Try to fetch the "Ch2 Text" by appending "/chapter/2" to the base URL
            ch2_url = base_url + "/chapter/2"
            response_ch2 = requests.get(ch2_url)
            response_ch2.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the HTML content for "Ch2 Text"
            soup_ch2 = BeautifulSoup(response_ch2.content, 'html.parser')

            # Find the <div> with title="2 The technology"
            ch2_div = soup_ch2.find('div', class_='chapter', title="2 The technology")

            # Extract the text from the <p> tags within the <div>
            if ch2_div:
                ch2_paragraphs = ch2_div.find_all('p')
                ch2_text = ' '.join([p.get_text(strip=True) for p in ch2_paragraphs])
                result['Ch2 Text'] = ch2_text
            else:
                result['Ch2 Text'] = 'Chapter 2 section not found'

        except Exception as e:
            # Store error for "Ch2 Text"
            result['Ch2 Text'] = f'Error: {str(e)}'

    return result

# Use ThreadPoolExecutor to process multiple URLs concurrently
with ThreadPoolExecutor(max_workers=10) as executor:  # You can adjust max_workers depending on your system's capabilities
    futures = [executor.submit(extract_data, index, row) for index, row in df.iterrows()]
    
    # Process completed tasks as they finish
    for future in as_completed(futures):
        result = future.result()
        index = result['index']
        df.at[index, 'Text Extract'] = result.get('Text Extract', 'Error')
        df.at[index, 'Ch3 Text'] = result.get('Ch3 Text', 'Error')
        df.at[index, 'Ch2 Text'] = result.get('Ch2 Text', 'Error')

# Save the updated Excel file with the extracted data
output_file_path = 'output6.xlsx'
df.to_excel(output_file_path, index=False)

print(f"Updated Excel file saved as: {output_file_path}")
