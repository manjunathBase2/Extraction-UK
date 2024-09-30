import pandas as pd
import requests
import pdfplumber
import io

def extract_lay_summary_paragraph_from_pdf(doc_link):
    try:
        if pd.isna(doc_link):
            return ""
        
        response = requests.get(doc_link)
        response.raise_for_status()  # Raise an error for bad responses
        
        # Open the PDF from the response content
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            if len(pdf.pages) < 2:
                return ""  # Return empty if the PDF has less than 2 pages

            # Extract the entire content of page 2
            page_2_text = pdf.pages[1].extract_text()  # Page 2 is index 1 (0-based indexing)

            # Print the extracted content for debugging
            if page_2_text:
                print(f"Extracted content from page 2:\n{page_2_text}")
            else:
                print("No text found on page 2.")

            return page_2_text.strip() if page_2_text else ""  # Return text of page 2

    except Exception as e:
        print(f"Error: {e}")
        return ""  # Return empty string for any exceptions

def main():
    # Load the Excel file (change 'your_file.xlsx' to your actual file name)
    df = pd.read_excel(r"data/UK_output.xlsx")
    
    # Ensure there is a column named 'PAR links'
    if 'PAR Link' not in df.columns:
        print("The Excel file does not contain a 'PAR links' column.")
        return
    
    # Create a new column for the extracted paragraphs from page 2
    df['Page 2 Content'] = df['PAR Link'].apply(extract_lay_summary_paragraph_from_pdf)

    # Save the updated DataFrame back to Excel
    df.to_excel(r"data/UK_Final.xlsx", index=False)  # Change 'updated_file.xlsx' as needed

if __name__ == "__main__":
    main()
