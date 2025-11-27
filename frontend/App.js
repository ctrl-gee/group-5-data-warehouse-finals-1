import React from 'react';
import './App.css';
import FileUpload from './components/FileUpload';
import InsuranceEligibility from './components/InsuranceEligibility';

function App() {
    return (
        <div className="App">
            <header className="App-header" style={{ backgroundColor: '#282c34', padding: '20px', color: 'white' }}>
                <h1>Airline Data Warehouse System</h1>
            </header>
            
            <main style={{ padding: '20px' }}>
                <FileUpload />
                <InsuranceEligibility />
            </main>
        </div>
    );
}

export default App;
