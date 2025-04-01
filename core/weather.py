from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime, timedelta
import multiprocessing as mp
import os
from tqdm import tqdm  # For progress bar
import logging  # For detailed tracking

# Set up logging
logging.basicConfig(
    filename='weather_scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fahrenheit_to_celsius(temp_str):
    try:
        temp_f = float(temp_str.split()[0])
        return round((temp_f - 32) * 5 / 9, 1)
    except:
        return temp_str

def scrape_day(args):
    city_station, date, country, city, output_dir = args
    formatted_date = date.strftime("%Y-%m-%d")
    url = f"https://www.wunderground.com/history/daily/{country}/{city}/{city_station}/date/{formatted_date}"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get(url)
        logging.info(f"Started scraping {formatted_date}")
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'mat-table')]"))
        )

        table = driver.find_element(By.XPATH, "//table[contains(@class, 'mat-table')]")
        columns = ["Time", "Temperature", "Dew Point", "Humidity", "Wind", "Wind Speed", 
                  "Wind Gust", "Pressure", "Precip.", "Condition"]

        tbody = table.find_element(By.TAG_NAME, "tbody")
        rows = tbody.find_elements(By.TAG_NAME, "tr")
        
        weather_data = [
            [col.text.strip() for col in row.find_elements(By.TAG_NAME, "td")][:len(columns)]
            for row in rows if row.find_elements(By.TAG_NAME, "td")
        ]
        
        if not weather_data:
            logging.warning(f"No data found for {formatted_date}")
            return None
        
        df = pd.DataFrame(weather_data, columns=columns[:len(weather_data[0])])
        df["Date"] = formatted_date
        df["Temperature"] = df["Temperature"].apply(fahrenheit_to_celsius)
        df["Dew Point"] = df["Dew Point"].apply(fahrenheit_to_celsius)
        
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/weather_osijek_{formatted_date}.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Saved data to {filename}")
        return True

    except Exception as e:
        logging.error(f"Error on {formatted_date}: {str(e)}")
        return None

    finally:
        driver.quit()

def scrape_date_range(start_date, end_date, city_station, country="hr", city="osijek", output_dir="weather_data_osijek"):
    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    args = [(city_station, date, country, city, output_dir) for date in dates]
    total_days = len(dates)
    
    logging.info(f"Starting scrape for {total_days} days from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Scraping {total_days} days...")

    # Use multiprocessing pool with progress tracking
    with mp.Pool(processes=min(mp.cpu_count(), 8)) as pool:
        # Use tqdm to show progress bar
        results = list(tqdm(
            pool.imap(scrape_day, args),
            total=total_days,
            desc="Scraping Progress",
            unit="day"
        ))
    
    successful = sum(1 for r in results if r is True)
    failed = total_days - successful
    logging.info(f"Scraping completed: {successful} successful, {failed} failed")
    print(f"Completed: {successful} days successful, {failed} days failed")

if __name__ == "__main__":
    start_date = datetime(2002, 5, 20)
    end_date = datetime(2025, 4, 1)
    city_station = "LDOS"
    output_dir = "custom_output_folder"  # Change this to your desired folder path
    
    print(f"Scraping from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    scrape_date_range(start_date, end_date, city_station, output_dir=output_dir)
    print("Scraping completed!")