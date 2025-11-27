import React, { useState } from 'react';
import axios from 'axios';

const FileUpload = () => {
    const [selectedFile, setSelectedFile] = useState(null);
    const [tableName, setTableName] = useState('airlines');
    const [uploadStatus, setUploadStatus] = useState('');

    const handleFileSelect = (event) => {
        setSelectedFile(event.target.files[0]);
    };

    const handleUpload = async () => {
        if (!selectedFile) {
            alert('Please select a file first');
            return;
        }

        const formData = new FormData();
        formData.append('file', selectedFile);
        formData.append('tableName', tableName);

        try {
            setUploadStatus('Uploading...');
            const response = await axios.post('http://localhost:5000/upload', formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                },
            });
            setUploadStatus(`Upload successful! Processed ${response.data.processed} records`);
        } catch (error) {
            setUploadStatus('Upload failed: ' + error.message);
        }
    };

    const handleProcess = async () => {
        try {
            setUploadStatus('Processing data...');
            const response = await axios.post('http://localhost:5000/process');
            setUploadStatus(`Processing completed! ${response.data.message}`);
        } catch (error) {
            setUploadStatus('Processing failed: ' + error.message);
        }
    };

    return (
        <div style={{ padding: '20px', border: '1px solid #ccc', margin: '10px', borderRadius: '5px' }}>
            <h2>Upload Data File</h2>
            <div>
                <label>Select Table: </label>
                <select value={tableName} onChange={(e) => setTableName(e.target.value)}>
                    <option value="airlines">Airlines</option>
                    <option value="airports">Airports</option>
                    <option value="flights">Flights</option>
                    <option value="passengers">Passengers</option>
                    <option value="sales">Sales</option>
                </select>
            </div>
            <div style={{ margin: '10px 0' }}>
                <input type="file" onChange={handleFileSelect} />
            </div>
            <div>
                <button onClick={handleUpload} style={{ marginRight: '10px', padding: '10px 20px' }}>
                    Upload
                </button>
                <button onClick={handleProcess} style={{ padding: '10px 20px' }}>
                    Process Data
                </button>
            </div>
            {uploadStatus && (
                <div style={{ marginTop: '10px', padding: '10px', backgroundColor: '#f0f0f0' }}>
                    {uploadStatus}
                </div>
            )}
        </div>
    );
};

export default FileUpload;
