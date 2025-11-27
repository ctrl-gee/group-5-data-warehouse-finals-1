import pandas as pd
import os
import json
from datetime import datetime

class FallbackDataManager:
    def __init__(self):
        self.data_folder = "local_data"
        os.makedirs(self.data_folder, exist_ok=True)
    
    def save_to_local(self, table_name, data):
        """Save data to local CSV files"""
        file_path = f"{self.data_folder}/{table_name}.csv"
        
        try:
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data)
            
            # Append to existing file or create new
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_csv(file_path, index=False)
            else:
                df.to_csv(file_path, index=False)
                
            print(f"✅ Saved {len(df)} records to {file_path}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving locally: {e}")
            return False
    
    def save_dirty_data(self, dirty_data):
        """Save dirty data locally"""
        file_path = f"{self.data_folder}/dirty_data.json"
        
        try:
            # Load existing dirty data
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []
            
            # Add timestamp to new records
            for record in dirty_data:
                record['local_timestamp'] = datetime.now().isoformat()
            
            existing_data.extend(dirty_data)
            
            # Save back to file
            with open(file_path, 'w') as f:
                json.dump(existing_data, f, indent=2)
                
            print(f"✅ Saved {len(dirty_data)} dirty records locally")
            return True
            
        except Exception as e:
            print(f"❌ Error saving dirty data: {e}")
            return False
