from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from main import DataWarehouseManager
import os

app = Flask(__name__)
CORS(app)

manager = DataWarehouseManager()

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        table_name = request.form.get('tableName', 'airlines')
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Save uploaded file temporarily
        file_path = f"temp_{file.filename}"
        file.save(file_path)
        
        # Process the file
        manager.upload_file(file_path, table_name)
        
        # Clean up
        os.remove(file_path)
        
        return jsonify({
            'message': 'File uploaded successfully',
            'table_name': table_name,
            'processed': 'Data sent to processing queue'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process', methods=['POST'])
def process_data():
    try:
        # This would trigger the data processing pipeline
        # For now, we'll just return a success message
        return jsonify({
            'message': 'Data processing initiated',
            'status': 'Processing in background'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/check-eligibility', methods=['GET'])
def check_eligibility():
    try:
        passenger_name = request.args.get('name')
        flight_id = request.args.get('flightID')
        
        results = manager.check_insurance_eligibility(
            passenger_name=passenger_name,
            flight_id=flight_id
        )
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
    
