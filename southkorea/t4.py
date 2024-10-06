import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor

def extract_html_links(excel_path, output_path, max_workers=3, batch_size=10):
    # Read the Excel file
    df = pd.read_excel(excel_path)
    
    # Create a new column for extracted text in Korean
    df['Extracted Korean Text'] = ''
    
    def process_url(item_code):
        url = f"https://nedrug.mfds.go.kr/pbp/cmn/html/drb/{item_code}/EE"
        try:
            # Download the HTML
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract all <p> tags
            paragraphs = soup.find_all('p')
            korean_text_content = ' '.join([p.get_text(strip=True) for p in paragraphs])
            
            return item_code, korean_text_content
            
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
            return item_code, f"Error: {str(e)}"
    
    def process_row(row):
        item_code = row['Item standard code']
        return process_url(item_code)
    
    # Use ThreadPoolExecutor to process URLs concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # To track progress and batch writes
        processed_count = 0
        results = []
        
        for result in executor.map(process_row, [row for _, row in df.iterrows()]):
            results.append(result)
            processed_count += 1

            # Write to Excel after every batch_size URLs
            if processed_count % batch_size == 0:
                for item_code, korean_text in results:
                    df.loc[df['Item standard code'] == item_code, 'Extracted Korean Text'] = korean_text
                
                # Save progress to Excel
                df.to_excel(output_path, index=False)
                print(f"Processed {processed_count} URLs. Progress saved to {output_path}.")
                
                # Clear results list for the next batch
                results.clear()
        
        # Write any remaining results if not a full batch
        if results:
            for item_code, korean_text in results:
                df.loc[df['Item standard code'] == item_code, 'Extracted Korean Text'] = korean_text
            
            # Save final progress to Excel
            df.to_excel(output_path, index=False)
            print(f"Processed {processed_count} URLs. Final progress saved to {output_path}.")
        
    print(f"Processing complete. Results saved to {output_path}")

# Example usage
excel_path = 'TA_input.xlsx'
output_path = 'output.xlsx'
extract_html_links(excel_path, output_path, max_workers=4, batch_size=10)
