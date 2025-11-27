import React, { useState } from 'react';
import axios from 'axios';

const InsuranceEligibility = () => {
    const [searchParams, setSearchParams] = useState({
        name: '',
        flightID: '',
        baggage: '',
        date: ''
    });
    const [searchResult, setSearchResult] = useState(null);
    const [isLoading, setIsLoading] = useState(false);

    const handleInputChange = (event) => {
        const { name, value } = event.target;
        setSearchParams(prev => ({
            ...prev,
            [name]: value
        }));
    };

    const handleSearch = async () => {
        if (!searchParams.name && !searchParams.flightID) {
            alert('Please enter either name or flight ID');
            return;
        }

        setIsLoading(true);
        try {
            const response = await axios.get('http://localhost:5000/check-eligibility', {
                params: searchParams
            });
            
            if (response.data.length > 0) {
                setSearchResult(response.data[0]); // Take first result
            } else {
                setSearchResult({ IsEligible: false, message: 'No records found' });
            }
        } catch (error) {
            console.error('Search error:', error);
            setSearchResult({ IsEligible: false, message: 'Error searching records' });
        }
        setIsLoading(false);
    };

    return (
        <div style={{ padding: '20px', border: '1px solid #ccc', margin: '10px', borderRadius: '5px' }}>
            <h2>Insurance Eligibility Check</h2>
            
            <div style={{ marginBottom: '10px' }}>
                <input
                    type="text"
                    name="name"
                    placeholder="Passenger Name"
                    value={searchParams.name}
                    onChange={handleInputChange}
                    style={{ marginRight: '10px', padding: '5px' }}
                />
                <input
                    type="text"
                    name="flightID"
                    placeholder="Flight ID"
                    value={searchParams.flightID}
                    onChange={handleInputChange}
                    style={{ marginRight: '10px', padding: '5px' }}
                />
            </div>
            
            <div style={{ marginBottom: '10px' }}>
                <input
                    type="text"
                    name="baggage"
                    placeholder="Baggage Status"
                    value={searchParams.baggage}
                    onChange={handleInputChange}
                    style={{ marginRight: '10px', padding: '5px' }}
                />
                <input
                    type="date"
                    name="date"
                    value={searchParams.date}
                    onChange={handleInputChange}
                    style={{ marginRight: '10px', padding: '5px' }}
                />
            </div>
            
            <button 
                onClick={handleSearch} 
                disabled={isLoading}
                style={{ padding: '10px 20px', backgroundColor: '#007bff', color: 'white', border: 'none' }}
            >
                {isLoading ? 'Searching...' : 'Search'}
            </button>

            {searchResult && (
                <div style={{
                    marginTop: '20px',
                    padding: '20px',
                    backgroundColor: searchResult.IsEligible ? '#d4edda' : '#f8d7da',
                    border: `2px solid ${searchResult.IsEligible ? '#28a745' : '#dc3545'}`,
                    borderRadius: '5px'
                }}>
                    <h3>Insurance Eligibility Result</h3>
                    {searchResult.IsEligible ? (
                        <div>
                            <p style={{ color: '#155724', fontWeight: 'bold', fontSize: '18px' }}>
                                ✅ Customer is ELIGIBLE for Insurance
                            </p>
                            {searchResult.FlightKey && <p>Flight: {searchResult.FlightKey}</p>}
                            {searchResult.PassengerKey && <p>Passenger ID: {searchResult.PassengerKey}</p>}
                        </div>
                    ) : (
                        <p style={{ color: '#721c24', fontWeight: 'bold', fontSize: '18px' }}>
                            ❌ Customer is NOT ELIGIBLE for Insurance
                        </p>
                    )}
                </div>
            )}
        </div>
    );
};

export default InsuranceEligibility;
