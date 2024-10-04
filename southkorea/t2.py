import pandas as pd
import requests
from bs4 import BeautifulSoup
from googletrans import Translator
import time
from concurrent.futures import ThreadPoolExecutor

def translate_and_extract_html_links(excel_path, output_path, max_workers=3, batch_size=10):
    # Read the Excel file
    df = pd.read_excel(excel_path)
    
    # Initialize translator
    translator = Translator()
    
    # Create new columns for extracted text in Korean and the translated text in English
    df['Extracted Korean Text'] = ''
    df['Translated English Text'] = ''
    
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
            
            # Translate text if it's not empty
            english_text_content = ''
            if korean_text_content:
                # Split text into chunks to avoid translation limits
                chunks = [korean_text_content[i:i+5000] for i in range(0, len(korean_text_content), 5000)]
                translated_chunks = []
                
                for chunk in chunks:
                    try:
                        translated = translator.translate(chunk, src='ko', dest='en').text
                        translated_chunks.append(translated)
                        # Reduce delay to 0.5 seconds
                        time.sleep(0.5)  
                    except Exception as e:
                        print(f"Translation error: {e}")
                
                english_text_content = ' '.join(translated_chunks)
            
            return item_code, korean_text_content, english_text_content
            
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
            return item_code, f"Error: {str(e)}", f"Error: {str(e)}"
    
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
                for item_code, korean_text, english_text in results:
                    df.loc[df['Item standard code'] == item_code, 'Extracted Korean Text'] = korean_text
                    df.loc[df['Item standard code'] == item_code, 'Translated English Text'] = english_text
                
                # Save progress to Excel
                df.to_excel(output_path, index=False)
                print(f"Processed {processed_count} URLs. Progress saved to {output_path}.")
                
                # Clear results list for the next batch
                results.clear()
        
        # Write any remaining results if not a full batch
        if results:
            for item_code, korean_text, english_text in results:
                df.loc[df['Item standard code'] == item_code, 'Extracted Korean Text'] = korean_text
                df.loc[df['Item standard code'] == item_code, 'Translated English Text'] = english_text
            
            # Save final progress to Excel
            df.to_excel(output_path, index=False)
            print(f"Processed {processed_count} URLs. Final progress saved to {output_path}.")
        
    print(f"Processing complete. Results saved to {output_path}")

# Example usage
excel_path = 'input.xlsx'
output_path = 'output.xlsx'
translate_and_extract_html_links(excel_path, output_path, max_workers=3, batch_size=10)
