# tests/test_api_client.py
import pytest
from unittest.mock import patch, Mock
from requests.exceptions import RequestException, Timeout, HTTPError
from scripts.api_client import BreweryApiClient
import logging

# Fixture for the API client
@pytest.fixture
def api_client():
    return BreweryApiClient(max_retries=2, timeout=5)

# Test successful API response
def test_get_breweries_success(api_client):
    mock_response = Mock()
    mock_response.json.return_value = [
        {"id": "1", "name": "Test Brewery 1"},
        {"id": "2", "name": "Test Brewery 2"}
    ]
    mock_response.raise_for_status.return_value = None

    with patch('requests.get', return_value=mock_response) as mock_get:
        result = api_client.get_breweries(page=1, per_page=2)
        
        assert len(result) == 2
        assert result[0]['name'] == "Test Brewery 1"
        mock_get.assert_called_once_with(
            "https://api.openbrewerydb.org/v1/breweries",
            params={"page": 1, "per_page": 2},
            timeout=5
        )

# Test retry mechanism on failure
def test_get_breweries_retry_on_failure(api_client, caplog):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Timeout("Connection timeout")
    
    with patch('requests.get', return_value=mock_response) as mock_get:
        with pytest.raises(RequestException):
            api_client.get_breweries()
            
        assert mock_get.call_count == 2  # Original + 1 retry
        assert "Attempt 1 failed" in caplog.text
        assert "Attempt 2 failed" in caplog.text

# Test HTTP error handling
def test_get_breweries_http_error(api_client, caplog):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(HTTPError):
            api_client.get_breweries()
            
    assert "HTTP error occurred" in caplog.text

# Test pagination in get_all_breweries
def test_get_all_breweries_pagination(api_client):
    mock_responses = [
        Mock(json=Mock(return_value=[{"id": str(i), "name": f"Brewery {i}"} for i in range(1, 201)]),
        Mock(json=Mock(return_value=[{"id": str(i), "name": f"Brewery {i}"} for i in range(201, 401)])),
        Mock(json=Mock(return_value=[]))  # Empty response to stop pagination
    ]
    
    for mock in mock_responses:
        mock.raise_for_status.return_value = None

    with patch('requests.get', side_effect=mock_responses) as mock_get:
        result = api_client.get_all_breweries()
        
        assert len(result) == 400
        assert mock_get.call_count == 3
        assert result[0]['name'] == "Brewery 1"
        assert result[399]['name'] == "Brewery 400"

# Test empty response handling
def test_get_all_breweries_empty_response(api_client):
    mock_response = Mock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None
    
    with patch('requests.get', return_value=mock_response):
        result = api_client.get_all_breweries()
        assert len(result) == 0

# Test logging configuration
def test_api_client_logging(caplog):
    caplog.set_level(logging.WARNING)
    client = BreweryApiClient()
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Timeout("Timeout")
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(RequestException):
            client.get_breweries()
            
    assert "Timeout" in caplog.text

# Test configuration parameters
def test_api_client_configuration():
    custom_client = BreweryApiClient(max_retries=5, timeout=10)
    assert custom_client.max_retries == 5
    assert custom_client.timeout == 10

# Test invalid page parameters
def test_invalid_page_parameters(api_client):
    with pytest.raises(ValueError):
        api_client.get_breweries(page=0, per_page=10)
    
    with pytest.raises(ValueError):
        api_client.get_breweries(page=1, per_page=0)

# Test connection error handling
def test_connection_error(api_client, caplog):
    with patch('requests.get', side_effect=ConnectionError("Connection failed")):
        with pytest.raises(ConnectionError):
            api_client.get_breweries()
            
    assert "Connection failed" in caplog.text