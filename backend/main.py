import os
from supabase import create_client
from data_cleaner import DataCleaner
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataWarehouseManager:
    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.supabase = create_client(self.supabase_url, self.supabase_key)
        self.cleaner = DataCleaner(self.supabase)
    
    def detect_table_type(self, file_path):
        """Detect what type of table the CSV file contains"""
        try:
            df = pd.read_csv(file_path, nrows=1)  # Read just the header
            
            # Check columns to determine table type
            columns = [col.lower() for col in df.columns]
            
            if 'airlinekey' in columns or 'airlinename' in columns:
                return 'airlines'
            elif 'airportkey' in columns or 'airportname' in columns:
                return 'airports' 
            elif 'flightkey' in columns and ('originairportkey' in columns or 'destinationairportkey' in columns):
                return 'flights'
            elif 'passengerkey' in columns or 'fullname' in columns:
                return 'passengers'
            elif 'transactionid' in columns and ('passengerid' in columns or 'flightid' in columns):
                return 'travel_agency_sales_001'
            else:
                return 'unknown'
                
        except Exception as e:
            print(f"Error detecting table type: {e}")
            return 'unknown'
    
    def upload_file(self, file_path, table_name=None):
        """Upload and process a CSV file with proper duplicate handling"""
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            print(f"📊 Loaded {len(df)} records from {file_path}")
            
            # Auto-detect table type if not specified
            if not table_name or table_name == 'auto':
                table_name = self.detect_table_type(file_path)
                print(f"🔍 Auto-detected table type: {table_name}")
            
            if table_name == 'unknown':
                return {'error': 'Could not determine table type from CSV columns'}
            
            # Process the data based on table type
            if table_name == 'airlines':
                cleaned_df, dirty_data = self.cleaner.process_airlines_data(df)
                table_to_insert = 'airlines'
                key_column = 'airlinekey'
            elif table_name == 'airports':
                cleaned_df, dirty_data = self.cleaner.process_airports_data(df)
                table_to_insert = 'airports'
                key_column = 'airportkey'
            elif table_name == 'flights':
                cleaned_df, dirty_data = self.cleaner.process_flights_data(df)
                table_to_insert = 'flights'
                key_column = 'flightkey'
            elif table_name == 'passengers':
                cleaned_df, dirty_data = self.cleaner.process_passengers_data(df)
                table_to_insert = 'passengers'
                key_column = 'passengerkey'
            elif table_name == 'travel_agency_sales_001':
                cleaned_df, dirty_data = self.cleaner.process_sales_data(df)
                table_to_insert = 'factairlinesales'
                key_column = 'transactionid'
            else:
                return {'error': f'Unsupported table type: {table_name}'}
            
            print(f"✅ Cleaned data: {len(cleaned_df)} records, Dirty data: {len(dirty_data)} records")
            
            # Insert cleaned data with duplicate handling
            successful_inserts = 0
            duplicate_errors = []
            
            if not cleaned_df.empty:
                # Convert DataFrame to list of dictionaries
                cleaned_data = cleaned_df.to_dict('records')
                
                # Insert each record individually to catch duplicates
                for record in cleaned_data:
                    try:
                        response = self.supabase.table(table_to_insert).insert(record).execute()
                        if response.data:
                            successful_inserts += 1
                    except Exception as e:
                        error_str = str(e)
                        # Check if it's a duplicate key error
                        if '23505' in error_str or 'duplicate' in error_str.lower():
                            duplicate_errors.append({
                                'table_name': table_to_insert,
                                'original_data': record,
                                'error_reason': f'Duplicate key: {error_str}'
                            })
                        else:
                            # Other errors also go to dirty data
                            duplicate_errors.append({
                                'table_name': table_to_insert,
                                'original_data': record,
                                'error_reason': error_str
                            })
                
                print(f"📥 Successfully inserted: {successful_inserts} records")
                print(f"🚫 Duplicates/errors: {len(duplicate_errors)} records")
            
            # Combine all dirty data (cleaning errors + duplicate errors)
            all_dirty_data = dirty_data + duplicate_errors
            
            # Insert dirty data
            if all_dirty_data:
                try:
                    self.supabase.table('dirty_data').insert(all_dirty_data).execute()
                    print(f"🗑️ Stored {len(all_dirty_data)} dirty records")
                except Exception as e:
                    print(f"❌ Error storing dirty data: {e}")
            
            return {
                'processed': successful_inserts,
                'dirty_data': len(all_dirty_data),
                'cleaned_but_duplicate': len(duplicate_errors),
                'cleaning_errors': len(dirty_data),
                'table_name': table_to_insert,
                'message': f'Successfully processed {successful_inserts} records, {len(all_dirty_data)} moved to dirty table'
            }
            
        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            return {'error': str(e)}
    
    def check_insurance_eligibility(self, passenger_name=None, flight_id=None):
        """Check if customer is eligible for insurance"""
        try:
            query = self.supabase.table('factairlinesales').select('*')
            
            if passenger_name:
                # Get passenger key from name
                passenger_response = self.supabase.table('passengers')\
                    .select('passengerkey')\
                    .ilike('fullname', f'%{passenger_name}%')\
                    .execute()
                
                if passenger_response.data:
                    passenger_keys = [p['passengerkey'] for p in passenger_response.data]
                    query = query.in_('passengerkey', passenger_keys)
            
            if flight_id:
                query = query.eq('flightkey', flight_id)
            
            response = query.execute()
            
            eligible_records = []
            for record in response.data:
                # Check eligibility conditions 
                is_eligible = self.determine_eligibility(record)
                eligible_records.append({
                    **record,
                    'iseligible': is_eligible
                })
            
            return eligible_records
            
        except Exception as e:
            print(f"Error checking eligibility: {e}")
            return []
    
    def determine_eligibility(self, record):
        """Determine insurance eligibility based on business rules"""
        # Mock implementation - you'll replace this with real logic
        # For now, randomly determine eligibility
        import random
        conditions = [
            random.choice([True, False]),  # Flight delayed > 4 hours
            random.choice([True, False]),  # Baggage lost
            random.choice([True, False])   # Baggage damaged
        ]
        
        # Eligible if any condition is true
        return any(conditions)

# Example usage
if __name__ == "__main__":
    manager = DataWarehouseManager()
    
    # Example: Upload different file types
    # manager.upload_file('datasets/airlines.csv')
    # manager.upload_file('datasets/travel_agency_sales_001.csv')
