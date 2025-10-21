"""
Kentucky Mugshot Scraper - Production Version
Scrapes public arrest records from BustedNewspaper.com
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import re
from pathlib import Path
import logging
import json
import schedule
from typing import List, Dict, Optional
import argparse
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class KentuckyMugshotScraper:
    """
    Enhanced scraper for BustedNewspaper.com Kentucky mugshot records
    """
    
    def __init__(self, output_dir='mugshot_data'):
        self.base_url = "https://bustednewspaper.com/mugshots/kentucky"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Cache directory
        self.cache_dir = Path('scraper_cache')
        self.cache_dir.mkdir(exist_ok=True)
        
        # Browser headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # County mappings
        self.counties = {
            'nelson': 'nelson-county',
            'jefferson': 'jefferson-county',
            'hardin': 'hardin-county',
            'bullitt': 'bullitt-county',
            'fayette': 'fayette-county',
            'spencer': 'spencer-county',
            'warren': 'warren-county',
            'boone': 'boone-county',
            'franklin': 'franklin-county'
        }
    
    def load_cache(self, county_name: str) -> set:
        """Load previously scraped record IDs"""
        cache_file = self.cache_dir / f"{county_name}_cache.json"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    return set(data.get('record_ids', []))
            except Exception as e:
                logger.warning(f"Error loading cache: {e}")
        return set()
    
    def save_cache(self, county_name: str, record_ids: set):
        """Save scraped record IDs to cache"""
        cache_file = self.cache_dir / f"{county_name}_cache.json"
        
        try:
            with open(cache_file, 'w') as f:
                json.dump({
                    'record_ids': list(record_ids),
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"Error saving cache: {e}")
    
    def generate_record_id(self, record: Dict) -> str:
        """Generate unique ID for a record"""
        return f"{record['name']}_{record['booking_date']}".lower().replace(' ', '_')
    
    def scrape_county(self, county_name: str, max_pages: int = 5, 
                     date_from: Optional[datetime] = None, 
                     date_to: Optional[datetime] = None,
                     charge_keywords: Optional[List[str]] = None,
                     skip_duplicates: bool = True) -> List[Dict]:
        """
        Scrape mugshot data for a specific county
        """
        if county_name.lower() not in self.counties:
            logger.error(f"County '{county_name}' not supported")
            return []
        
        cached_ids = self.load_cache(county_name) if skip_duplicates else set()
        county_slug = self.counties[county_name.lower()]
        url = f"{self.base_url}/{county_slug}/"
        
        logger.info(f"Starting scrape for {county_name.title()} County...")
        if date_from:
            logger.info(f"Filtering from: {date_from.strftime('%Y-%m-%d')}")
        if date_to:
            logger.info(f"Filtering to: {date_to.strftime('%Y-%m-%d')}")
        if charge_keywords:
            logger.info(f"Filtering charges: {', '.join(charge_keywords)}")
        
        all_records = []
        new_record_ids = set()
        
        for page in range(1, max_pages + 1):
            page_url = url if page == 1 else f"{url}page/{page}/"
            
            try:
                logger.info(f"Scraping page {page}")
                response = requests.get(page_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                records = self._parse_page(soup, county_name)
                
                if not records:
                    logger.info(f"No more records on page {page}")
                    break
                
                filtered_records = self._filter_records(
                    records, cached_ids, date_from, date_to, charge_keywords
                )
                
                all_records.extend(filtered_records)
                
                for record in filtered_records:
                    new_record_ids.add(self.generate_record_id(record))
                
                logger.info(f"Found {len(filtered_records)} new records on page {page}")
                time.sleep(2)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"Error parsing page {page}: {e}")
                continue
        
        if skip_duplicates and new_record_ids:
            cached_ids.update(new_record_ids)
            self.save_cache(county_name, cached_ids)
        
        logger.info(f"Total new records for {county_name}: {len(all_records)}")
        return all_records
    
    def _filter_records(self, records: List[Dict], cached_ids: set,
                       date_from: Optional[datetime], date_to: Optional[datetime],
                       charge_keywords: Optional[List[str]]) -> List[Dict]:
        """Apply filters to records"""
        filtered = []
        
        for record in records:
            record_id = self.generate_record_id(record)
            if record_id in cached_ids:
                continue
            
            if date_from or date_to:
                booking_date = self._parse_date(record['booking_date'])
                if booking_date:
                    if date_from and booking_date < date_from:
                        continue
                    if date_to and booking_date > date_to:
                        continue
            
            if charge_keywords and record['charges']:
                charges_lower = record['charges'].lower()
                if not any(kw.lower() in charges_lower for kw in charge_keywords):
                    continue
            
            filtered.append(record)
        
        return filtered
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string"""
        if not date_str:
            return None
        
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
    
    def _parse_page(self, soup, county_name):
        """Parse mugshot records from page"""
        records = []
        
        entries = soup.find_all(['article', 'div'], class_=re.compile(r'(mugshot|booking|arrest)', re.I))
        
        if not entries:
            entries = soup.find_all('div', class_='post')
        
        for entry in entries:
            try:
                record = self._extract_record_data(entry, county_name)
                if record:
                    records.append(record)
            except Exception as e:
                logger.debug(f"Error parsing record: {e}")
                continue
        
        return records
    
    def _extract_record_data(self, entry, county_name):
        """Extract data from single record"""
        record = {
            'county': county_name.title(),
            'name': '',
            'age': '',
            'height': '',
            'weight': '',
            'hair_color': '',
            'eye_color': '',
            'sex': '',
            'race': '',
            'booking_date': '',
            'arrested_by': '',
            'charges': '',
            'bond_amount': '',
            'mugshot_url': '',
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        name_elem = entry.find(['h2', 'h3', 'a'], class_=re.compile(r'name|title', re.I))
        if name_elem:
            record['name'] = name_elem.get_text(strip=True)
        
        img_elem = entry.find('img')
        if img_elem and img_elem.get('src'):
            record['mugshot_url'] = img_elem.get('src')
        
        text_content = entry.get_text()
        
        record['age'] = self._extract_pattern(text_content, r'age\s+(\d+)')
        record['height'] = self._extract_pattern(text_content, r"height\s+([\d'\"]+)")
        record['weight'] = self._extract_pattern(text_content, r'weight\s+(\d+)\s*lbs')
        record['hair_color'] = self._extract_pattern(text_content, r'hair\s+([A-Z]{3})')
        record['eye_color'] = self._extract_pattern(text_content, r'eye\s+([A-Z]{3})')
        record['sex'] = self._extract_pattern(text_content, r'sex\s+(Male|Female)', case_sensitive=True)
        record['race'] = self._extract_pattern(text_content, r'race\s+([A-Z])\s+')
        
        booking_date = self._extract_pattern(text_content, r'booked\s+([\d\-]+)')
        if booking_date:
            record['booking_date'] = booking_date
        
        arrested_by = self._extract_pattern(text_content, r'arrested by\s+([A-Z\s]+)', multiword=True)
        if arrested_by:
            record['arrested_by'] = arrested_by
        
        bond = self._extract_pattern(text_content, r'bond[:\s]+\$?([\d,]+)')
        if bond:
            record['bond_amount'] = bond
        
        charges_match = re.search(r'charges?[:\s]+(.+?)(?:bond|$)', text_content, re.I | re.DOTALL)
        if charges_match:
            record['charges'] = charges_match.group(1).strip()
        
        return record if record['name'] else None
    
    def _extract_pattern(self, text, pattern, case_sensitive=False, multiword=False):
        """Extract data using regex"""
        flags = 0 if case_sensitive else re.I
        match = re.search(pattern, text, flags)
        
        if match:
            result = match.group(1).strip()
            if multiword:
                result = ' '.join(result.split())
            return result
        return ''
    
    def save_to_csv(self, records, county_name, include_timestamp=True):
        """Save records to CSV"""
        if not records:
            logger.warning(f"No records to save for {county_name}")
            return None
        
        if include_timestamp:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{county_name.lower()}_mugshots_{timestamp}.csv"
        else:
            filename = f"{county_name.lower()}_mugshots.csv"
        
        filepath = self.output_dir / filename
        
        df = pd.DataFrame(records)
        
        column_order = ['name', 'booking_date', 'charges', 'age', 'sex', 'race',
                       'height', 'weight', 'hair_color', 'eye_color', 
                       'arrested_by', 'bond_amount', 'county', 'mugshot_url', 'scraped_at']
        
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        
        df.to_csv(filepath, index=False)
        
        logger.info(f"Saved {len(records)} records to {filepath}")
        return filepath
    
    def scrape_all_counties(self, max_pages=3, **filter_kwargs):
        """Scrape all counties"""
        results = {}
        
        for county_name in self.counties.keys():
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {county_name.title()} County")
            logger.info(f"{'='*60}\n")
            
            records = self.scrape_county(county_name, max_pages, **filter_kwargs)
            results[county_name] = records
            
            if records:
                self.save_to_csv(records, county_name)
            
            time.sleep(3)
        
        return results
    
    def generate_summary_report(self, results):
        """Generate summary report"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        report = f"\n{'='*60}\n"
        report += f"SCRAPING SUMMARY - {timestamp}\n"
        report += f"{'='*60}\n\n"
        
        total_records = 0
        
        for county, records in results.items():
            count = len(records)
            total_records += count
            status = "✅" if count > 0 else "❌"
            report += f"{status} {county.title()} County: {count} records\n"
        
        report += f"\n{'='*60}\n"
        report += f"TOTAL: {total_records} records\n"
        report += f"{'='*60}\n"
        
        print(report)
        logger.info(report)
        
        return report
    
    def search_by_name(self, name: str, county: Optional[str] = None) -> List[Dict]:
        """Search saved CSVs for name"""
        results = []
        name_lower = name.lower()
        
        if county:
            csv_files = list(self.output_dir.glob(f"{county.lower()}_mugshots_*.csv"))
        else:
            csv_files = list(self.output_dir.glob("*_mugshots_*.csv"))
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                matches = df[df['name'].str.lower().str.contains(name_lower, na=False)]
                results.extend(matches.to_dict('records'))
            except Exception as e:
                logger.warning(f"Error searching {csv_file}: {e}")
        
        return results
    
    def scheduled_scrape(self, counties: List[str] = None, max_pages: int = 3):
        """Run scheduled scrape"""
        logger.info("Running scheduled scrape...")
        
        if counties:
            results = {}
            for county in counties:
                records = self.scrape_county(county, max_pages)
                results[county] = records
                if records:
                    self.save_to_csv(records, county)
                time.sleep(3)
        else:
            results = self.scrape_all_counties(max_pages)
        
        self.generate_summary_report(results)


def setup_scheduler(scraper: KentuckyMugshotScraper, 
                    times: List[str] = ['09:00', '12:00', '15:00', '18:00'],
                    counties: List[str] = None):
    """Setup scheduled scraping"""
    for run_time in times:
        schedule.every().day.at(run_time).do(
            scraper.scheduled_scrape, 
            counties=counties,
            max_pages=3
        )
        logger.info(f"Scheduled scrape at {run_time}")
    
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Kentucky Mugshot Scraper')
    
    parser.add_argument('--county', type=str, help='Specific county to scrape')
    parser.add_argument('--all', action='store_true', help='Scrape all counties')
    parser.add_argument('--pages', type=int, default=5, help='Max pages per county')
    parser.add_argument('--date-from', type=str, help='Filter from date (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=str, help='Filter to date (YYYY-MM-DD)')
    parser.add_argument('--charges', type=str, nargs='+', help='Filter by charges')
    parser.add_argument('--schedule', action='store_true', help='Run on schedule')
    parser.add_argument('--times', type=str, nargs='+', 
                       default=['09:00', '12:00', '15:00', '18:00'],
                       help='Schedule times')
    parser.add_argument('--search-name', type=str, help='Search for name')
    
    args = parser.parse_args()
    
    scraper = KentuckyMugshotScraper()
    
    date_from = datetime.strptime(args.date_from, '%Y-%m-%d') if args.date_from else None
    date_to = datetime.strptime(args.date_to, '%Y-%m-%d') if args.date_to else None
    
    if args.search_name:
        results = scraper.search_by_name(args.search_name, args.county)
        print(f"\nFound {len(results)} matches:")
        for r in results:
            print(f"  - {r['name']} ({r['county']}) - {r['booking_date']}")
        return
    
    if args.schedule:
        counties = [args.county] if args.county else None
        setup_scheduler(scraper, args.times, counties)
        return
    
    if args.county:
        records = scraper.scrape_county(
            args.county, 
            args.pages,
            date_from=date_from,
            date_to=date_to,
            charge_keywords=args.charges
        )
        if records:
            scraper.save_to_csv(records, args.county)
            print(f"\n✅ Scraped {len(records)} records from {args.county.title()}")
        else:
            print(f"\n❌ No records found for {args.county.title()}")
    
    elif args.all:
        results = scraper.scrape_all_counties(
            args.pages,
            date_from=date_from,
            date_to=date_to,
            charge_keywords=args.charges
        )
        scraper.generate_summary_report(results)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
