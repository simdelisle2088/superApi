import aiohttp
from pyrate_limiter import Union
import requests
from ftplib import FTP
import pandas as pd
from io import BytesIO
import asyncio
import logging
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv(
    os.path.join(os.path.dirname(__file__), ".env"),
)

# Environment variables
JAVA_EPICOR_API = os.getenv("JAVA_EPICOR_API")
if not JAVA_EPICOR_API: JAVA_EPICOR_API = 'http://localhost:10000/epicor-ws-test'
ESL_SIGN = os.getenv("ESL_SIGN") 
if not ESL_SIGN: ESL_SIGN = '80805d794841f1b4'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # logging.FileHandler(f'prix_etiquette_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

class FTPBusyError(Exception):
    """Custom exception for when FTP server is busy"""
    pass

class SystemBusyError(Exception):
    """Custom exception for when system is busy"""
    pass

class EtiquetteController:
    FTP_HOST = "gamcar.ca"
    FTP_USER = "esl@pasuper.xyz"
    FTP_PASS = "Pasuper7803!"
    BATCH_SIZE = 100
    DELAY_BETWEEN_BATCHES = 2

    FILES_CONFIG = {
        "PRIXETIQUETTECHATEAUGUAY.xlsx": {
            "store_id": "0003"
        },
        "PRIXETIQUETTEST-HUBERT.xlsx": {
            "store_id": "0001"
        },
        "PRIXETIQUETTEST-JEAN.xlsx": {
            "store_id": "0002"
        }
    }

    def __init__(self, include_price: bool = True):
        """Initialize the controller with a flag to determine whether to include price information"""
        self.include_price = include_price
        # Define the key mappings based on whether price is included
        self._key_mapping = {
            'Part Number': 'pi',
            'Part Description': 'pn', 
            'Value': 'kc',
            'UPC Code': 'pc'
        }
        if self.include_price:
            self._key_mapping['Price'] = 'pp'

    def convert_keys(self, data_list: List[Dict]) -> List[Dict]:
        """
        Convert dictionary keys based on the configured mapping
        """
        return [{self._key_mapping[k]: v for k, v in item.items() 
                if k in self._key_mapping} for item in data_list]

    @staticmethod
    def clean_data(array: List[Dict]) -> List[Dict]:
        """Clean the data by handling NaN values"""
        for item in array:
            for key, value in item.items():
                if pd.isna(value):
                    item[key] = 0
        return array

    @staticmethod
    def merge_price_data(prices: List[Dict], data: List[Dict]) -> List[Dict]:
        """Merge price information with the original data"""
        # Convert arrays to dictionaries using Part Number as key
        dict1 = {item['Part Number']: item for item in prices}
        dict2 = {item['Part Number']: item for item in data}
        
        for part_num, item in dict2.items():
            if part_num in dict1:
                dict2[part_num].update(dict1[part_num])
                
        return list(dict2.values())

    @staticmethod
    def get_processed_parts_prices(parts: List[Dict]) -> Tuple[List[Dict], bool]:
        """Query and process parts pricing from API"""
        try:
            parts_list = [{"Code": pn[0], "PartNum": pn[1]} 
                        for part in parts 
                        if len(pn := str(part['Part Number']).strip().split(' ', 1)) >= 2]

            if not parts_list:
                return [], False

            response = requests.post(
                JAVA_EPICOR_API+"/queryParts",
                params={'customerNumber': 100},
                headers={'Content-Type': 'application/json'},
                json={'listParts': parts_list},
                timeout=30
            )
            response.raise_for_status()

            result = response.json().get('result', {})
            return [{
                'Part Number': f"{info['MfgCode']} {info['PartNum']}", 
                'Price': info['Price']['UnitCost']
            } for parts in result.values() for info in parts], True

        except Exception as e:
            logging.error(f"Error processing parts: {e}")
            return [], False

    async def process_csv_with_retry(
        self,
        ftp: FTP, 
        file_name: str, 
        max_retries: int = 3, 
        initial_delay: int = 5
    ) -> Tuple[Optional[pd.DataFrame], bool]:
        """Process a single file with retry logic"""
        delay = initial_delay
        
        for attempt in range(max_retries):
            try:
                with BytesIO() as bio:
                    logging.info(f"Attempt {attempt + 1} processing file: {file_name}")
                    ftp.retrbinary(f"RETR /{file_name}", bio.write)
                    bio.seek(0)
                    df = pd.read_excel(bio, engine='openpyxl')
                    
                    if df.empty:
                        logging.error(f"Empty dataframe for {file_name}")
                        return None, False
                        
                    logging.info(f"Columns in file {file_name}: {df.columns.tolist()}")
                    return df, True
                    
            except Exception as e:
                if "busy" in str(e).lower():
                    logging.warning(f"FTP server busy on attempt {attempt + 1}, waiting {delay} seconds...")
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                    
                logging.error(f"Error processing {file_name} on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                    
                return None, False
        
        return None, False

    @staticmethod
    def batch_parts_list(parts: List[Dict], batch_size: int = 2000) -> List[List[Dict]]:
        """Split parts list into batches"""
        return [parts[i:i + batch_size] for i in range(0, len(parts), batch_size)]

    async def process_batch(self, batch: List[Dict]) -> Tuple[List[Dict], bool]:
        """Process a batch of data, optionally including price information"""
        if not self.include_price:
            return self.clean_data(batch), True

        # If including price, get and merge price information
        prices, success = self.get_processed_parts_prices(batch)
        if not success:
            return [], False

        cleaned_data = self.merge_price_data(prices, batch)
        return self.clean_data(cleaned_data), True

    async def post_data_with_retry(
        self,
        session_data: Union[List[Dict], Dict],
        store_id: str,
        max_retries: int = 3,
        initial_delay: int = 5
    ) -> bool:
        """Post processed data to the API with retry logic"""
        delay = initial_delay
    
        if isinstance(session_data, dict):
            data_to_process = [session_data]
        else:
            data_to_process = session_data
    
        batches = self.batch_parts_list(data_to_process)
        logging.info(f"Processing {len(batches)} batches of up to {self.BATCH_SIZE} items each")
    
        overall_success = True

        for i, batch in enumerate(batches, 1):
            logging.info(f"Processing batch {i} of {len(batches)} ({len(batch)} items)")
            
            processed_data, success = await self.process_batch(batch)
            if not success:
                overall_success = False
                continue

            products = self.convert_keys(processed_data)
            
            if not products:
                logging.error(f"No valid products in batch {i}")
                overall_success = False
                continue

            payload = {
                "store_code": store_id,
                "f1": products,
                "is_base64": "0", 
                "f2": "40:d6:3c:5e:11:63",
                "sign": ESL_SIGN
            }

            success = await self._send_batch_to_api(payload, i, max_retries, delay)
            if not success:
                overall_success = False

            await asyncio.sleep(self.DELAY_BETWEEN_BATCHES)

        return overall_success

    async def _send_batch_to_api(
        self,
        payload: Dict,
        batch_num: int,
        max_retries: int,
        initial_delay: int
    ) -> bool:
        """Send a batch of data to the API with retry logic"""
        delay = initial_delay
        
        async with aiohttp.ClientSession() as session:
            for attempt in range(max_retries):
                try:
                    async with session.post(
                        'https://esl.pasuper.xyz/api/default/product/create_multiple',
                        headers={'Content-Type': 'application/json'},
                        json=payload,
                        timeout=30
                    ) as response:
                        result = await response.json()
                        if response.status == 200 and isinstance(result, dict) and result.get('error_code') == 0:
                            return True
                        
                        logging.error(f"API error in batch {batch_num}: Status {response.status}, Response: {result}")
                
                except Exception as e:
                    logging.error(f"API error on attempt {attempt + 1} for batch {batch_num}: {e}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2
                
        return False

    async def read_and_store_files(self):
        """Read files from FTP and store their contents"""
        ftp = None
        try:
            ftp = FTP(self.FTP_HOST)
            ftp.login(self.FTP_USER, self.FTP_PASS)

            files_on_ftp = ftp.nlst("/")
            logging.info(f"Available files in FTP root: {files_on_ftp}")

            errors = []
            for file_name, config in self.FILES_CONFIG.items():
                logging.info(f"Processing {file_name} for store {config['store_id']}")
                
                df, success = await self.process_csv_with_retry(ftp, file_name)
                if not success:
                    errors.append(f"Failed to process {file_name}")
                    continue

                data_to_store = df.to_dict('records')
                store_success = await self.post_data_with_retry(
                    session_data=data_to_store,
                    store_id=config['store_id']
                )
                
                if not store_success:
                    errors.append(f"Failed to store data for {file_name}")

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            errors.append(str(e))
            
        finally:
            if ftp:
                try:
                    ftp.quit()
                except Exception as e:
                    logging.error(f"Error closing FTP connection: {e}")

        if errors:
            raise Exception(f"Errors occurred: {'; '.join(errors)}")

async def etiquette_scheduled_job(include_price: bool = True):
    """
    Scheduled job that can handle both price and quantity updates
    
    Args:
        include_price: If True, includes price information. If False, only updates quantity and basic info.
    """
    try:
        controller = EtiquetteController(include_price=include_price)
        await controller.read_and_store_files()
        job_type = "price and quantity" if include_price else "quantity"
        logging.info(f"Completed {job_type} update job successfully")
    except Exception as e:
        logging.error(f"Error in scheduled job test: {e}")
        raise


# You can now use these functions like this:
async def price_label_scheduled_job():
    """Job that includes price updates"""
    await etiquette_scheduled_job(include_price=True)

async def qty_label_scheduled_job():
    """Job that only updates quantity information"""
    await etiquette_scheduled_job(include_price=False)