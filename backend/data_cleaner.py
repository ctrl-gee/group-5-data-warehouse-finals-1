import pandas as pd
import re
import json
from datetime import datetime

class DataCleaner:
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.current_passenger_id = 1000
        self.current_transaction_id = 40000
        
        # Column mapping from CSV headers to database column names
        self.column_mappings = {
            'airlines': {
                'AirlineKey': 'airlinekey',
                'AirlineName': 'airlinename', 
                'Alliance': 'alliance'
            },
            'airports': {
                'AirportKey': 'airportkey',
                'AirportName': 'airportname',
                'City': 'city',
                'Country': 'country'
            },
            'flights': {
                'FlightKey': 'flightkey',
                'OriginAirportKey': 'originairportkey',
                'DestinationAirportKey': 'destinationairportkey', 
                'AircraftType': 'aircrafttype'
            },
            'passengers': {
                'PassengerKey': 'passengerkey',
                'FullName': 'fullname',
                'Email': 'email',
                'LoyaltyStatus': 'loyaltystatus'
            },
            'travel_agency_sales_001': {
                'TransactionID': 'transactionid',
                'TransactionDate': 'transactiondate',
                'PassengerID': 'passengerkey',
                'FlightID': 'flightkey',
                'TicketPrice': 'ticketprice',
                'Taxes': 'taxes',
                'BaggageFees': 'baggagefees',
                'TotalAmount': 'totalamount'
            }
        }
    
    def map_columns(self, df, table_name):
        """Map CSV column names to database column names"""
        if table_name not in self.column_mappings:
            return df
            
        mapping = self.column_mappings[table_name]
        df_renamed = df.rename(columns=mapping)
        
        # Only keep columns that exist in the mapping
        valid_columns = [col for col in df_renamed.columns if col in mapping.values()]
        return df_renamed[valid_columns]
    
    def get_existing_keys(self, table_name, key_column):
        """Get existing keys from database to check for duplicates"""
        try:
            response = self.supabase.table(table_name).select(key_column).execute()
            return set([item[key_column] for item in response.data])
        except Exception as e:
            print(f"Error getting existing keys from {table_name}: {e}")
            return set()
    
    def insert_data_with_duplicate_handling(self, table_name, cleaned_data):
        """Insert data while handling duplicates by moving them to dirty table"""
        successful_inserts = 0
        duplicate_errors = []
        
        for record in cleaned_data:
            try:
                # Try to insert each record individually
                response = self.supabase.table(table_name).insert(record).execute()
                if response.data:
                    successful_inserts += 1
            except Exception as e:
                error_str = str(e)
                # Check if it's a duplicate key error
                if '23505' in error_str or 'duplicate' in error_str.lower():
                    duplicate_errors.append({
                        'table_name': table_name,
                        'original_data': record,
                        'error_reason': f'Duplicate key: {error_str}'
                    })
                else:
                    # Other errors also go to dirty data
                    duplicate_errors.append({
                        'table_name': table_name,
                        'original_data': record,
                        'error_reason': error_str
                    })
        
        return successful_inserts, duplicate_errors
    
    def clean_airline_key(self, airline_key):
        """Clean airline key to 2 uppercase letters"""
        if pd.isna(airline_key):
            return None
            
        # Extract only letters and convert to uppercase
        cleaned = re.sub(r'[^A-Za-z]', '', str(airline_key)).upper()
        
        # Take only first 2 characters
        if len(cleaned) >= 2:
            return cleaned[:2]
        elif len(cleaned) == 1:
            return cleaned + 'X'  # Pad if only 1 character
        else:
            return None
    
    def clean_airport_key(self, airport_key):
        """Clean airport key to 3 uppercase letters"""
        if pd.isna(airport_key):
            return None
            
        cleaned = re.sub(r'[^A-Za-z]', '', str(airport_key)).upper()
        
        if len(cleaned) >= 3:
            return cleaned[:3]
        elif len(cleaned) == 2:
            return cleaned + 'X'
        elif len(cleaned) == 1:
            return cleaned + 'XX'
        else:
            return None
    
    def clean_flight_key(self, flight_key):
        """Clean flight key: 1-2 letters + 3-4 numbers"""
        if pd.isna(flight_key):
            return None
            
        # Extract letters and numbers
        match = re.match(r'([A-Za-z]{1,2})(\d{3,4})', str(flight_key).upper())
        if match:
            letters = match.group(1)
            numbers = match.group(2)
            return f"{letters}{numbers}"
        else:
            # Try to extract any pattern
            letters = ''.join(re.findall(r'[A-Za-z]', str(flight_key))).upper()[:2]
            numbers = ''.join(re.findall(r'\d', str(flight_key)))[:4]
            if letters and numbers:
                return f"{letters}{numbers}"
            return None
    
    def clean_passenger_key(self, passenger_key, existing_keys=None):
        """Clean passenger key to P + incrementing number starting from 1001"""
        if pd.isna(passenger_key):
            self.current_passenger_id += 1
            return f"P{self.current_passenger_id}"
            
        # Check if it's already in correct format
        if re.match(r'^P\d{4,}$', str(passenger_key)):
            # Extract the number and ensure proper formatting
            num = int(re.findall(r'\d+', passenger_key)[0])
            return f"P{num}"
            
        # Extract numbers and create new ID
        numbers = re.findall(r'\d+', str(passenger_key))
        if numbers:
            # Use the first number found, but ensure it's at least 1001
            num = int(numbers[0])
            if num < 1000:
                self.current_passenger_id += 1
                return f"P{self.current_passenger_id}"
            else:
                return f"P{num}"
        else:
            self.current_passenger_id += 1
            return f"P{self.current_passenger_id}"
    
    def clean_transaction_id(self, transaction_id, existing_ids=None):
        """Clean transaction ID starting from 40001"""
        if pd.isna(transaction_id):
            self.current_transaction_id += 1
            return self.current_transaction_id
            
        # If it's already a valid number
        if isinstance(transaction_id, (int, float)) and not pd.isna(transaction_id):
            num = int(transaction_id)
            if num >= 40000:
                return num
        
        # Extract numbers only from string
        numbers = re.findall(r'\d+', str(transaction_id))
        if numbers:
            num = int(''.join(numbers))
            if num >= 40000:
                return num
            else:
                self.current_transaction_id += 1
                return self.current_transaction_id
        else:
            self.current_transaction_id += 1
            return self.current_transaction_id
    
    def clean_email(self, email, full_name):
        """Standardize email to firstname.lastname@example.com"""
        if pd.isna(email) or not self.is_valid_email(str(email)):
            # Create email from full name
            if pd.notna(full_name):
                name_parts = str(full_name).lower().split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0].replace(' ', '').replace(',', '')
                    last_name = name_parts[-1].replace(' ', '').replace(',', '')
                    return f"{first_name}.{last_name}@example.com"
            return "unknown@example.com"
        return str(email).lower()
    
    def is_valid_email(self, email):
        """Basic email validation"""
        if pd.isna(email):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, str(email)) is not None
    
    def clean_country(self, country):
        """Standardize country names"""
        if pd.isna(country):
            return "Unknown"
            
        country = str(country).strip()
        country_mapping = {
            "USA": "United States",
            "U.S.A": "United States",
            "U.S.A.": "United States",
            "U.S.": "United States",
            "U.S": "United States",
            "US": "United States",
            "America": "United States",
            "UK": "United Kingdom",
            "U.K.": "United Kingdom",
            "UAE": "United Arab Emirates",
            "United States of America": "United States"
        }
        
        return country_mapping.get(country, country)
    
    def clean_date(self, date_value):
        """Clean and standardize date format"""
        if pd.isna(date_value):
            return None
            
        try:
            # Try different date formats
            if isinstance(date_value, str):
                # Remove any time component
                date_value = date_value.split()[0]
                
            # Parse the date
            if '/' in str(date_value):
                return pd.to_datetime(date_value, format='%Y/%m/%d').strftime('%Y%m%d')
            elif '-' in str(date_value):
                return pd.to_datetime(date_value, format='%Y-%m-%d').strftime('%Y%m%d')
            else:
                # Assume it's already in YYYYMMDD format
                return int(date_value)
        except:
            return None
    
    def clean_amount(self, amount):
        """Clean monetary amounts"""
        if pd.isna(amount):
            return 0.0
            
        try:
            # Remove currency symbols and commas
            cleaned = re.sub(r'[^\d.]', '', str(amount))
            return float(cleaned)
        except:
            return 0.0

    def process_airlines_data(self, df):
        """Process and clean airlines data"""
        cleaned_data = []
        dirty_data = []
        
        # Map column names first
        df_mapped = self.map_columns(df, 'airlines')
        
        # Get existing airline keys to identify duplicates during cleaning
        existing_airlines = self.get_existing_keys('airlines', 'airlinekey')
        
        for _, row in df_mapped.iterrows():
            try:
                airline_key = self.clean_airline_key(row.get('airlinekey'))
                
                if not airline_key:
                    raise ValueError("Invalid AirlineKey")
                
                cleaned_row = {
                    'airlinekey': airline_key,
                    'airlinename': str(row.get('airlinename', '')).strip(),
                    'alliance': str(row.get('alliance', '')).strip()
                }
                
                # Validate required fields
                if not cleaned_row['airlinename']:
                    raise ValueError("Missing AirlineName")
                    
                cleaned_data.append(cleaned_row)
                
            except Exception as e:
                dirty_data.append({
                    'table_name': 'airlines',
                    'original_data': row.to_dict(),
                    'error_reason': str(e)
                })
        
        return pd.DataFrame(cleaned_data), dirty_data
    
    def process_airports_data(self, df):
        """Process and clean airports data"""
        cleaned_data = []
        dirty_data = []
        
        # Map column names first
        df_mapped = self.map_columns(df, 'airports')
        
        for _, row in df_mapped.iterrows():
            try:
                airport_key = self.clean_airport_key(row.get('airportkey'))
                
                if not airport_key:
                    raise ValueError("Invalid AirportKey")
                
                cleaned_row = {
                    'airportkey': airport_key,
                    'airportname': str(row.get('airportname', '')).strip(),
                    'city': str(row.get('city', '')).strip(),
                    'country': self.clean_country(row.get('country', ''))
                }
                
                # Validate required fields
                if not all([cleaned_row['airportname'], cleaned_row['city'], cleaned_row['country']]):
                    raise ValueError("Missing required fields")
                    
                cleaned_data.append(cleaned_row)
                
            except Exception as e:
                dirty_data.append({
                    'table_name': 'airports',
                    'original_data': row.to_dict(),
                    'error_reason': str(e)
                })
        
        return pd.DataFrame(cleaned_data), dirty_data
    
    def process_passengers_data(self, df):
        """Process and clean passengers data"""
        cleaned_data = []
        dirty_data = []
        
        # Map column names first
        df_mapped = self.map_columns(df, 'passengers')
        
        for _, row in df_mapped.iterrows():
            try:
                passenger_key = self.clean_passenger_key(row.get('passengerkey'))
                
                full_name = str(row.get('fullname', '')).strip()
                if not full_name:
                    raise ValueError("Missing FullName")
                
                cleaned_row = {
                    'passengerkey': passenger_key,
                    'fullname': full_name,
                    'email': self.clean_email(row.get('email'), full_name),
                    'loyaltystatus': str(row.get('loyaltystatus', 'Bronze')).strip()
                }
                
                cleaned_data.append(cleaned_row)
                
            except Exception as e:
                dirty_data.append({
                    'table_name': 'passengers',
                    'original_data': row.to_dict(),
                    'error_reason': str(e)
                })
        
        return pd.DataFrame(cleaned_data), dirty_data
    
    def process_flights_data(self, df):
        """Process and clean flights data"""
        cleaned_data = []
        dirty_data = []
        
        # Map column names first
        df_mapped = self.map_columns(df, 'flights')
        
        for _, row in df_mapped.iterrows():
            try:
                flight_key = self.clean_flight_key(row.get('flightkey'))
                
                if not flight_key:
                    raise ValueError("Invalid FlightKey")
                
                cleaned_row = {
                    'flightkey': flight_key,
                    'originairportkey': self.clean_airport_key(row.get('originairportkey')),
                    'destinationairportkey': self.clean_airport_key(row.get('destinationairportkey')),
                    'aircrafttype': str(row.get('aircrafttype', '')).strip()
                }
                
                # Validate required fields
                if not all([cleaned_row['originairportkey'], cleaned_row['destinationairportkey']]):
                    raise ValueError("Missing airport codes")
                    
                cleaned_data.append(cleaned_row)
                
            except Exception as e:
                dirty_data.append({
                    'table_name': 'flights',
                    'original_data': row.to_dict(),
                    'error_reason': str(e)
                })
        
        return pd.DataFrame(cleaned_data), dirty_data
    
    def process_sales_data(self, df):
        """Process and clean sales data"""
        cleaned_data = []
        dirty_data = []
        
        # Map column names first - note the CSV is travel_agency_sales_001
        df_mapped = self.map_columns(df, 'travel_agency_sales_001')
        
        for _, row in df_mapped.iterrows():
            try:
                transaction_id = self.clean_transaction_id(row.get('transactionid'))
                
                # Clean passenger key (note: CSV has PassengerID, but we map to passengerkey)
                passenger_key = self.clean_passenger_key(row.get('passengerkey'))
                
                # Clean flight key
                flight_key = self.clean_flight_key(row.get('flightkey'))
                
                # Clean date
                date_key = self.clean_date(row.get('transactiondate'))
                
                if not all([transaction_id, passenger_key, flight_key, date_key]):
                    raise ValueError("Missing required fields")
                
                cleaned_row = {
                    'transactionid': transaction_id,
                    'datekey': date_key,
                    'passengerkey': passenger_key,
                    'flightkey': flight_key,
                    'ticketprice': self.clean_amount(row.get('ticketprice')),
                    'taxes': self.clean_amount(row.get('taxes')),
                    'baggagefees': self.clean_amount(row.get('baggagefees')),
                    'totalamount': self.clean_amount(row.get('totalamount'))
                }
                
                cleaned_data.append(cleaned_row)
                
            except Exception as e:
                dirty_data.append({
                    'table_name': 'factairlinesales',
                    'original_data': row.to_dict(),
                    'error_reason': str(e)
                })
        
        return pd.DataFrame(cleaned_data), dirty_data
