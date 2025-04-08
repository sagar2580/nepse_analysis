from asyncio import wait
from kafka import KafkaProducer
import json
import time
from datetime import datetime
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging
from bs4 import BeautifulSoup
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from selenium.webdriver.common.alert import Alert
class FloorsheetStreamer:
    def __init__(self):
        self.driver = None
        self.producer = None
        
    def initialize_webdriver(self):
        """Initialize Selenium WebDriver with proper options"""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        
        chrome_options = Options()
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(5)
        return self.driver
    def _create_headless_driver(self):
        """Configure headless Chrome with optimal settings"""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_script_timeout(30)
        return driver
    def _handle_alert(self):
        """Comprehensive alert handling with multiple strategies"""
        try:
            # Strategy 1: Immediate alert check
            WebDriverWait(self.driver, 3).until(EC.alert_is_present())
            Alert(self.driver).dismiss()
            logger.info("Alert dismissed (immediate check)")
            return True
        except:
            pass
        
        try:
            # Strategy 2: JavaScript alert override
            self.driver.execute_script("window.alert = function(){}; window.confirm = function(){return true;}; window.prompt = function(){return true;};")
            logger.info("Alert handlers overridden via JavaScript")
            return True
        except Exception as e:
            logger.warning(f"JavaScript alert override failed: {str(e)}")
        
        try:
            # Strategy 3: Refresh page and try again
            self.driver.refresh()
            WebDriverWait(self.driver, 3).until(EC.alert_is_present())
            Alert(self.driver).dismiss()
            logger.info("Alert dismissed after refresh")
            return True
        except:
            pass
        
        logger.warning("All alert dismissal strategies failed")
        return False
    def create_kafka_producer(self):
        """Create Kafka producer with simplified connection handling"""
        for attempt in range(3):  # Try 3 times
            try:
                producer = KafkaProducer(
                    bootstrap_servers=['localhost:29092'],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    acks='all',
                    retries=3,
                    request_timeout_ms=30000,
                    api_version=(2, 8, 1),
                    security_protocol='PLAINTEXT'
                )
                # Simple test - try to get cluster metadata
                producer.partitions_for('test-topic')  # This will force a connection attempt
                return producer
            except Exception as e:
               
                if attempt < 2:  # Don't sleep on last attempt
                    time.sleep(5)
        raise ConnectionError("Failed to connect to Kafka after 3 attempts")

    
    def _dismiss_alert(self, max_attempts=3):
        
        """Robust alert dismissal with multiple attempts"""
        for attempt in range(max_attempts):
            try:
                # Wait briefly for alert to appear
                time.sleep(1)
                Alert(self.driver).dismiss()
                logger.info(f"Successfully dismissed alert on attempt {attempt + 1}")
                return True
            except:
                logger.debug(f"Alert not present on attempt {attempt + 1}")
                if attempt == max_attempts - 1:
                    logger.warning("Failed to dismiss alert after multiple attempts")
                continue
        return False
    def search(self, date):
        """Automatically search for specific date without visible browser"""
        try:
            logger.info(f"Attempting headless search for date: {date}") 
            # Load the page
            self.driver.get("https://merolagani.com/Floorsheet.aspx")
            self._handle_alert()
            # Set the date value directly through JavaScript
            self.driver.execute_script(f"""
                document.getElementById("ctl00_ContentPlaceHolder1_ASCompanyFilter_txtAutoSuggest").value = "EBL";
            """)
            self.driver.execute_script(f"""
                document.getElementById("ctl00_ContentPlaceHolder1_txtFloorsheetDateFilter").value = "{date}";
            """) 
            # Submit the form directly instead of clicking button
            self.driver.execute_script("""
                document.getElementById("ctl00_ContentPlaceHolder1_lbtnSearchFloorsheet").click();
            """)
            
            # Wait for results - using JavaScript checks instead of WebDriverWait
            result = self.driver.execute_async_script("""
                const callback = arguments[arguments.length - 1];
                const checkInterval = setInterval(() => {
                    const tables = document.querySelectorAll("table.table");
                    const errorMsg = document.evaluate(
                        "//*[contains(text(), 'Could not find floorsheet')]",
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
                    ).singleNodeValue;
                    
                    if (tables.length > 0 || errorMsg) {
                        clearInterval(checkInterval);
                        callback({
                            has_tables: tables.length > 0,
                            has_error: !!errorMsg
                        });
                    }
                }, 500);
            """) 
            if result['has_error']:
                logger.warning(f"No data found for {date}")
                return False
                
            logger.info("Headless search successful - data found")
            return True
            
        except Exception as e:
            logger.error(f"Headless search failed: {str(e)}", exc_info=True)
            return False
        finally:
            # Consider whether to close driver here or manage at class level
            pass

    def close(self):
        """Clean up the driver"""
        if self.driver:
            self.driver.quit()
    def process_page(self, producer, date, page_num):
        """Process a single page and send to Kafka"""
        try:
            # Wait for table to load
            table = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "table.table.table-bordered.table-striped")
                )
            )
        
            # Get the HTML source and parse with BeautifulSoup
            html = table.get_attribute('outerHTML')
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract headers
            headers = [th.text.strip() for th in soup.find_all('th')]
            # Process rows
            data = []
            for row in soup.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == len(headers):  # Ensure header-cell alignment
                    row_data = {
                        headers[i]: cell.text.strip() 
                        for i, cell in enumerate(cells)
                    }
                    row_data.update({
                        'scraped_at': datetime.now().isoformat(),
                        'trade_date': date,
                        'page_number': page_num
                    })
                    data.append(row_data)
            print(data)
            # Send to Kafka
            if data and producer is not None:
                for record in data:
                    try:
                        # Convert to JSON and send
                        future = producer.send(
                            'test-topic',
                            value=record
                        )
                        future.get(timeout=1)
                        logger.info(f"Sent record for {record.get('Symbol', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Failed to send record: {str(e)}")
                        continue
            
            logger.info(f"Processed page {page_num} with {len(data)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error processing page {page_num}: {str(e)}", exc_info=True)
            return False
        finally:
            # Don't close producer here - let the calling function manage it
            pass
    def get_last_page_number(self):
        try:
            last_page_link = self.driver.find_element(By.CSS_SELECTOR, "a[title='Last Page']")
            onclick_js = last_page_link.get_attribute("onclick")
            last_page = onclick_js.split('"')[1]  # Extract from JS (e.g., "__doPostBack('...','139')")
            return int(last_page)
        except NoSuchElementException:
            return int(1)
        except Exception as e:
            print(f"Error getting last page number: {e}")
            return None
    def paginate(self, date):
        """Handle pagination for a single date"""
        page_num = 1
        
        max_pages = self.get_last_page_number()
        print(max_pages)
        if not self.search( date):
            return False
        while page_num <= max_pages:
            success = self.process_page(self.producer, date, page_num)
            if not success:
                break
            # Pagination logic
            try:
                next_buttons = self.driver.find_elements(By.XPATH, "//a[contains(., 'Next')]")
                if not next_buttons:
                    logger.info("No more pages available")
                    break
                    
                next_button = next_buttons[0]
                if "disabled" in next_button.get_attribute("class"):
                    logger.info("Reached last page")
                    break
                    
                # Scroll and click
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(0.5)
                self._handle_alert()
                
                # Save reference for staleness check
                current_first_row = self.driver.find_element(
                    By.CSS_SELECTOR, 
                    "table.table.table-bordered.table-striped tbody tr:first-child"
                )
                
                self.driver.execute_script("arguments[0].click();", next_button)
                WebDriverWait(self.driver, 15).until(EC.staleness_of(current_first_row))
                
                page_num += 1
                
            except Exception as e:
                logger.error(f"Pagination error: {str(e)}")
                break
                
        return True
    def stream_dates(self, dates):
        """Main method to stream multiple dates (corrected)"""
        try:
            self.driver = self._create_headless_driver()
            self.producer = self.create_kafka_producer()
            
            for date in dates:
                logger.info(f"\nProcessing date: {date}")
                start_time = datetime.now()
                
                # Corrected: Only pass date to search
                if not self.search(date):
                    logger.warning(f"Skipping {date} - search failed")
                    continue
                # Process pages
                self.paginate(date)
                logger.info(f"Completed {date} in {datetime.now() - start_time}")
                
        except Exception as e:
            logger.error(f"Streaming failed: {str(e)}")
        finally:
            if self.producer:
                self.producer.close()
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    streamer = FloorsheetStreamer()
    dates = ["04/07/2025"]
    # dates = ["03/24/2025","03/25/2025","03/26/2025","03/27/2025","03/28/2025","03/29/2025","03/30/2025","04/01/2025","04/02/2025","04/03/2025"]  # Test with one date first
    streamer.stream_dates(dates)