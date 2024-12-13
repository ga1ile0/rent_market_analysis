import requests
from bs4 import BeautifulSoup
import time
from typing import List, Dict
import pandas as pd
import os  # Add this import
from datetime import datetime  # Add this import
import math
from concurrent.futures import ThreadPoolExecutor
import threading  # Add this import


def get_listings_page(page_url: str) -> List[str]:
    """Get all listing URLs from a page"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(page_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all listing links
        listings = soup.find_all('a', {'data-cy': 'listing-item-link'})
        return ['https://www.otodom.pl/' + link['href'] for link in listings]
    
    except Exception as e:
        print(f"Error fetching listings page: {e}")
        return []

def extract_spans_from_p(p_element) -> List[str]:
    """Extract text from span elements within a paragraph"""
    spans = p_element.find_all('span', class_='css-axw7ok')
    return [span.text.strip() for span in spans]

def extract_label_value_pairs(soup, labels_to_find: List[str], container_class: str) -> Dict[str, str]:
    """Extract data from label-value pair divs"""
    results = {}
    containers = soup.find_all('div', class_=container_class)
    
    for container in containers:
        paragraphs = container.find_all('p')
        if len(paragraphs) == 2:
            label = paragraphs[0].text.strip()
            if label in labels_to_find:
                # Check if second paragraph contains spans
                spans = paragraphs[1].find_all('span', class_='css-axw7ok')
                if spans:
                    # Join multiple span values with semicolon
                    value = '; '.join([span.text.strip() for span in spans])
                else:
                    # Regular text value
                    value = paragraphs[1].text.strip()
                results[label] = value
    
    return results

def get_listing_details(listing_url: str, div_class_mapping: Dict[str, str], label_mapping: Dict[str, str]) -> Dict:
    """
    Extract details from individual listing page using two methods:
    1. Direct div content using class selectors
    2. Label-value pairs from container divs
    
    div_class_mapping: Dict with keys as field names and values as CSS selectors for direct content
    label_mapping: Dict with keys as field names and values as labels to search for in p tags
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(listing_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        details = {'url': listing_url}
        
        # Removed description scraping code
        
        # Extract data from direct div content
        for field, selector in div_class_mapping.items():
            element = soup.find(class_=selector)
            details[field] = element.text.strip() if element else None
        
        # Extract data from label-value pairs
        label_values = extract_label_value_pairs(
            soup, 
            list(label_mapping.values()), 
            'css-t7cajz e15n0fyo1'  # The container class for label-value pairs
        )
        
        # Map the found labels to our desired field names
        for field, label in label_mapping.items():
            details[field] = label_values.get(label)
            
        return details
    
    except Exception as e:
        print(f"Error fetching listing details: {e}")
        return {}

def scrape_apartments(base_url: str, div_class_mapping: Dict[str, str],
                      label_mapping: Dict[str, str], num_pages: int = 1, num_workers: int = 5):
    """Main function to scrape apartment listings"""

    all_urls = []

    # Collect all listing URLs and save them to a file
    print("Collecting all listing URLs...")
    for page in range(1, num_pages + 1):
        print(f"Getting URLs from page {page}/{num_pages}")
        page_url = base_url + f"&page={page}" if page > 1 else base_url
        listing_urls = get_listings_page(page_url)
        all_urls.extend(listing_urls)
        print(f"Found {len(listing_urls)} listings on page {page}")
        time.sleep(2)

    # Remove duplicate URLs
    unique_urls = list(set(all_urls))
    total_urls = len(unique_urls)
    print(f"Total unique listings collected: {total_urls}")

    # Create a new directory with unique name including date and hour
    script_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = os.path.join(script_dir, f'scrape_output_{timestamp}')
    os.makedirs(output_dir, exist_ok=True)

    # Save all URLs to a file
    urls_file = os.path.join(output_dir, 'listing_urls.txt')
    with open(urls_file, 'w', encoding='utf-8') as f:
        for url in unique_urls:
            f.write(url + '\n')
    print(f"Saved all listing URLs to {urls_file}")

    batch_size = num_workers * 1000
    total_batches = math.ceil(total_urls / batch_size)
    urls_processed = 0  # Counter for URLs read from file
    listings_scraped = 0  # Counter for listings scraped
    counter_lock = threading.Lock()  # Lock for thread-safe updates

    for batch_num in range(total_batches):
        # Read the next batch of URLs from the file
        with open(urls_file, 'r', encoding='utf-8') as f:
            # Skip URLs already processed
            for _ in range(urls_processed):
                next(f)
            batch_urls = [next(f).strip() for _ in range(min(batch_size, total_urls - urls_processed))]
        urls_processed += len(batch_urls)
        print(f"Processing batch {batch_num + 1}/{total_batches} with {len(batch_urls)} URLs")

        # Divide batch_urls among workers
        urls_per_worker = math.ceil(len(batch_urls) / num_workers)
        url_chunks = [batch_urls[i:i + urls_per_worker] for i in range(0, len(batch_urls), urls_per_worker)]

        def process_urls(url_chunk):
            nonlocal listings_scraped
            scraped_listings = []
            start_time = time.time()
            last_report_time = start_time

            for url in url_chunk:
                details = get_listing_details(url, div_class_mapping, label_mapping)
                if details:
                    scraped_listings.append(details)
                    with counter_lock:
                        listings_scraped += 1

                current_time = time.time()
                if current_time - last_report_time >= 10:
                    with counter_lock:
                        print(f"Number of listings scraped: {listings_scraped}/{total_urls}")
                    last_report_time = current_time
                time.sleep(1)

            # Save the batch of listings to CSV after processing the chunk
            if scraped_listings:
                df = pd.DataFrame(scraped_listings)
                worker_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = os.path.join(output_dir, f'apartments_batch_{batch_num + 1}_{worker_timestamp}.csv')
                df.to_csv(output_file, index=False, encoding='utf-8')

        # Use ThreadPoolExecutor to process URLs with multiple workers
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(process_urls, url_chunks)

    print(f"Scraping completed. Total listings scraped: {listings_scraped}/{total_urls}")


if __name__ == "__main__":
    BASE_URL = "https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/cala-polska?ownerTypeSingleSelect=ALL&viewType=listing&limit=72"
    
    # Direct div content mapping - remove description from here
    DIV_CLASS_MAPPING = {
        'title': 'css-wqvm7k ef3kcx01',
        'price': 'css-1o51x5a e1k1vyr21',
        'address': 'css-1jjm9oe e42rcgs1',
        'm2': 'css-1ftqasz',
    }
    
    # Label-value pair mapping (field_name: label_to_find)
    LABEL_MAPPING = {
        'heating': 'Ogrzewanie:',
        'floor': 'Piętro:',
        'finished_cond': 'Stan wykończenia:',
        'available_since': 'Dostępne od:',
        'admin_rent': 'Czynsz:',
        'deposit': 'Kaucja',
        'type_of_lister': 'Typ ogłoszeniodawcy:',
        'bd_yr_of_build': 'Rok budowy:',
        'bd_elevator': 'Winda:',
        'bd_type': 'Rodzaj zabudowy:',
        'bd_material': 'Materiał budynku:',
        'bd_windows_type': 'Okna:',
        'bd_energy_cert': 'Certyfikat energetyczny:',
        'bd_security': 'Bezpieczeństwo:',
        'additional_info': 'Informacje dodatkowe:',  # Add this new mapping
    }
    
    # Scrape data into DataFrame
    scrape_apartments(BASE_URL, DIV_CLASS_MAPPING, LABEL_MAPPING, num_pages=10)
