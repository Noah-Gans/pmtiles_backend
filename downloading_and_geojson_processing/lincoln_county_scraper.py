import requests
import json
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import threading

class LincolnCountyScraper:
    def __init__(self, output_dir="geojson_files"):
        """
        Initialize the LincolnCountyScraper with a base URL, output directory, and a requests session.
        """
        self.base_url = "https://propertydetails.lcwy.org/Home/Result"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def _thread_worker(self, thread_idx, start_page, end_page, tax_year, page_size, thread_files, skipped_files, scraped_dir=None):
        """
        Worker function for each thread. Scrapes a range of pages and writes results to thread-specific files.
        Args:
            thread_idx (int): Index of the thread.
            start_page (int): Starting page number (inclusive).
            end_page (int): Ending page number (exclusive for range).
            tax_year (int): Tax year to scrape.
            page_size (int): Number of results per page.
            thread_files (list): Shared list to store output file paths.
            skipped_files (list): Shared list to store skipped file paths.
            scraped_dir (str): Directory to write thread files to.
        """
        thread_props = {}
        thread_skipped = []
        if scraped_dir is None:
            scraped_dir = self.output_dir
        output_path = os.path.join(scraped_dir, f"lincoln_county_properties_{tax_year}_part_{thread_idx}.jsonl")
        skipped_path = os.path.join(scraped_dir, f"lincoln_county_skipped_properties_{tax_year}_part_{thread_idx}.json")
        for page_number in range(start_page, end_page):
            url = f"{self.base_url}?pageNumber={page_number}&taxYear={tax_year}&pageSize={page_size}"
            print(f"[THREAD {thread_idx}] Requesting URL: {url}")
            try:
                response = self.session.get(url)
                response.raise_for_status()
                if page_number == 1 and thread_idx == 0:
                    with open(os.path.join(scraped_dir, "lincoln_county_first_page.html"), "w") as f:
                        f.write(response.text)
                    print("[DEBUG] Saved first page HTML for inspection.")
                properties, _, skipped = self._parse_property_page(response.text)
                for rwacct, prop_dict in properties.items():
                    thread_props[rwacct] = prop_dict
                thread_skipped.extend(skipped)
                time.sleep(1)
            except Exception as e:
                print(f"❌ [THREAD {thread_idx}] Error scraping page {page_number}: {e}")
                break
        # Write thread results
        with open(output_path, 'w') as f:
            for rwacct, prop_dict in thread_props.items():
                prop_dict['Account #'] = rwacct
                f.write(json.dumps(prop_dict) + '\n')
        with open(skipped_path, 'w') as f:
            json.dump(thread_skipped, f, indent=2)
        print(f"[THREAD {thread_idx}] Finished. Wrote {len(thread_props)} properties to {output_path}")
        thread_files.append(output_path)
        skipped_files.append(skipped_path)

    def scrape_all_properties(self, tax_year=2026, page_size=100, num_threads=10):
        """
        Scrape all property records for Lincoln County for a given tax year using multithreading.
        Each thread scrapes a chunk of pages and writes to its own file. At the end, all files are merged.
        Args:
            tax_year (int): The tax year to scrape.
            page_size (int): Number of results per page.
            num_threads (int): Number of threads to use for scraping.
        Returns:
            dict: All scraped properties keyed by Account #.
        """
        print(f"🚀 Starting Lincoln County property scrape for tax year {tax_year} with {num_threads} threads")
        total_pages = 243  # 24,254 results / 100 per page = 242.54, so 243 pages
        pages_per_thread = total_pages // num_threads
        threads = []
        # Create a subdirectory for thread files
        scraped_dir = os.path.join(self.output_dir, "lincoln_county_scraped_files")
        os.makedirs(scraped_dir, exist_ok=True)
        thread_files = []
        skipped_files = []

        # Launch threads
        for i in range(num_threads):
            start_page = i * pages_per_thread + 1
            end_page = (i + 1) * pages_per_thread if i < num_threads - 1 else total_pages
            t = threading.Thread(target=self._thread_worker, args=(i, start_page, end_page + 1, tax_year, page_size, thread_files, skipped_files, scraped_dir))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        # Merge results
        final_output_path = os.path.join(self.output_dir, "lincoln_county_wy_ownership_address.jsonl")
        all_properties = {}
        with open(final_output_path, 'w') as fout:
            for part_file in thread_files:
                with open(part_file, 'r') as fin:
                    for line in fin:
                        prop = json.loads(line)
                        rwacct = prop.get('Account #')
                        if rwacct:
                            all_properties[rwacct] = prop
                            fout.write(json.dumps(prop) + '\n')
        print(f"✅ Merged {len(all_properties)} properties. Saved to {final_output_path}")

        # Merge skipped
        final_skipped_path = os.path.join(scraped_dir, f"lincoln_county_skipped_properties_{tax_year}.json")
        all_skipped = []
        for part_file in skipped_files:
            with open(part_file, 'r') as fin:
                all_skipped.extend(json.load(fin))
        with open(final_skipped_path, 'w') as fout:
            json.dump(all_skipped, fout, indent=2)
        print(f"⚠️ Merged {len(all_skipped)} skipped properties. Saved to {final_skipped_path}")

        return all_properties

    def _parse_property_page(self, html_content):
        """
        Parse a single HTML property page and extract property data.
        Args:
            html_content (str): HTML content of the property page.
        Returns:
            tuple: (properties dict, has_more bool, skipped_properties list)
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        properties = {}
        skipped_properties = []

        # Each property is in a <div class="ibox">
        property_boxes = soup.find_all('div', class_='ibox')
        print(f"[DEBUG] Found {len(property_boxes)} property boxes on page.")

        for i, box in enumerate(property_boxes):
            prop_dict = {}
            rwacct = None

            dts = box.find_all('dt')
            dds = box.find_all('dd')
            if len(dts) != len(dds):
                print(f"[DEBUG] Mismatched dt/dd count in property box {i}, skipping.")
                skipped_properties.append(str(box))
                continue

            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True)
                value = dd.get_text(" ", strip=True)
                if label == "Account #":
                    a = dd.find('a')
                    rwacct = a.get_text(strip=True) if a else value
                    prop_dict["Account #"] = rwacct
                else:
                    prop_dict[label] = value

            if rwacct:
                properties[rwacct] = prop_dict
            else:
                print(f"[DEBUG] Property box {i} missing Account # (RWACCT), skipping.")
                skipped_properties.append(prop_dict)

        has_more = self._check_for_more_pages(soup)
        print(f"[DEBUG] has_more: {has_more}")
        return properties, has_more, skipped_properties

    def _extract_total_count(self, html_content):
        """
        Extract the total number of results from the HTML content.
        Args:
            html_content (str): HTML content of the property page.
        Returns:
            int or None: Total number of results if found, else None.
        """
        import re
        match = re.search(r'(\d{1,3}(?:,\d{3})*) results found', html_content)
        if match:
            return int(match.group(1).replace(',', ''))
        return None

    def _check_for_more_pages(self, soup):
        """
        Check if there are more pages available by looking for a 'Next' button in the soup.
        Args:
            soup (BeautifulSoup): Parsed HTML soup.
        Returns:
            bool: True if there is a next page, False otherwise.
        """
        next_button = soup.find('a', text='Next')
        print(f"[DEBUG] Next button found: {next_button is not None}")
        return next_button is not None