import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Initialize Selenium WebDriver
def init_driver():
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Function to extract PAR link from a source URL
def get_par_link(driver, source_url):
    try:
        driver.get(source_url)
        print(f"Processing source URL: {source_url}")

        # Wait for the checkbox to appear and be clickable
        disclaimer_checkbox = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "agree-checkbox"))
        )
        disclaimer_checkbox.click()
        print("Checkbox clicked")

        # Wait for the "Agree" button to become enabled and click it
        agree_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Agree')]"))
        )
        agree_button.click()
        print("Agree button clicked")

        # Wait for the page to load and locate the PAR link
        search_results = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.search-result"))
        )

        # Find the first PAR link
        for result in search_results:
            icon_text = result.find_element(By.CSS_SELECTOR, "p.icon").text
            if icon_text == "PAR":
                par_link = result.find_element(By.CSS_SELECTOR, "a.doc-type-par").get_attribute("href")
                print(f"PAR Link: {par_link}")
                return par_link
        print("PAR Link not found")
        return None
    except Exception as e:
        print(f"Error processing URL {source_url}: {e}")
        return None

# Main function to read Excel, process URLs, and save back
def process_excel_and_store_par_links(input_excel_path, output_excel_path):
    # Read Excel file
    df = pd.read_excel(input_excel_path)

    # Check if 'Source of truth' column exists
    if 'Source of truth' not in df.columns:
        print("'Source of truth' column not found in the Excel file.")
        return

    # Initialize WebDriver
    driver = init_driver()

    # List to store extracted PAR links
    par_links = []

    # Iterate over the rows and process each URL in the 'Source of truth' column
    for index, row in df.iterrows():
        source_url = row['Source of truth']
        if pd.isna(source_url):
            par_links.append(None)  # If URL is empty, append None
        else:
            par_link = get_par_link(driver, source_url)
            par_links.append(par_link)

    # Add the extracted PAR links to a new column in the DataFrame
    df['PAR Link'] = par_links

    # Save the updated DataFrame with PAR links back to the original Excel file
    df.to_excel(output_excel_path, index=False)

    # Close the WebDriver
    driver.quit()
    print("Processing complete and saved to Excel.")

# Usage
input_excel_path = './data/input.xlsx'   # Path to input Excel file
output_excel_path = './data/UK_output.xlsx'  # Path to save updated Excel file with PAR links

process_excel_and_store_par_links(input_excel_path, output_excel_path)