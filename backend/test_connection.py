import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

def test_supabase_connection():
    try:
        url = os.getenv('SUPABASE_URL')
        key = os.getenv('SUPABASE_KEY')
        
        print(f"URL: {url}")
        print(f"Key starts with: {key[:20]}..." if key else "No key found")
        
        if not url or not key:
            print("❌ Missing Supabase credentials in .env file")
            return False
            
        # Test connection
        supabase = create_client(url, key)
        
        # Simple query to test connection
        response = supabase.table('dirty_data').select('*').limit(1).execute()
        
        print("✅ Successfully connected to Supabase!")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_supabase_connection()
