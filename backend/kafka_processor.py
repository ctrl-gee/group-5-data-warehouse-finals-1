import json
import pandas as pd
from confluent_kafka import Producer, Consumer
from data_cleaner import DataCleaner
import os

class KafkaDataProcessor:
    def __init__(self, supabase_client, bootstrap_servers='localhost:9092'):
        self.supabase = supabase_client
        self.cleaner = DataCleaner(supabase_client)
        
        # Kafka configuration
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'airline-data-processor'
        }
        
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'airline-data-group',
            'auto.offset.reset': 'earliest'
        }
        
        self.producer = Producer(self.producer_config)
        self.consumer = Consumer(self.consumer_config)
        
        # Subscribe to topics
        self.consumer.subscribe(['raw-data', 'cleaned-data'])
    
    def produce_raw_data(self, table_name, data):
        """Send raw data to Kafka topic"""
        message = {
            'table_name': table_name,
            'data': data.to_dict('records')
        }
        
        self.producer.produce(
            'raw-data',
            key=table_name,
            value=json.dumps(message)
        )
        self.producer.flush()
    
    def process_raw_data(self):
        """Consume and process raw data from Kafka"""
        while True:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value())
                table_name = data['table_name']
                raw_df = pd.DataFrame(data['data'])
                
                print(f"Processing {len(raw_df)} records for {table_name}")
                
                # Clean the data based on table type
                if table_name == 'airlines':
                    cleaned_df, dirty_data = self.cleaner.process_airlines_data(raw_df)
                elif table_name == 'airports':
                    cleaned_df, dirty_data = self.cleaner.process_airports_data(raw_df)
                elif table_name == 'passengers':
                    cleaned_df, dirty_data = self.cleaner.process_passengers_data(raw_df)
                elif table_name == 'flights':
                    cleaned_df, dirty_data = self.cleaner.process_flights_data(raw_df)
                elif table_name == 'sales':
                    cleaned_df, dirty_data = self.cleaner.process_sales_data(raw_df)
                else:
                    print(f"Unknown table: {table_name}")
                    continue
                
                # Send cleaned data to next topic
                if not cleaned_df.empty:
                    self.produce_cleaned_data(table_name, cleaned_df)
                
                # Store dirty data
                if dirty_data:
                    self.store_dirty_data(dirty_data)
                    
            except Exception as e:
                print(f"Error processing message: {e}")
    
    def produce_cleaned_data(self, table_name, cleaned_df):
        """Send cleaned data to Kafka topic"""
        message = {
            'table_name': table_name,
            'data': cleaned_df.to_dict('records')
        }
        
        self.producer.produce(
            'cleaned-data',
            key=table_name,
            value=json.dumps(message)
        )
        self.producer.flush()
    
    def store_dirty_data(self, dirty_data):
        """Store dirty data in Supabase"""
        for dirty_row in dirty_data:
            try:
                self.supabase.table('dirty_data').insert(dirty_row).execute()
            except Exception as e:
                print(f"Error storing dirty data: {e}")
