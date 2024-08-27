import pytest
import requests
from unittest import mock
from data_pipeline.data_ingestion import get_pokemon  # Substitua pelo caminho correto do seu módulo

@pytest.fixture
def mock_requests_get_success(mocker):
    mock_response = mock.Mock()
    mock_response.json.return_value = {'name': 'ditto'}
    mock_response.status_code = 200
    mock_get = mocker.patch('requests.get', return_value=mock_response)
    return mock_get

@pytest.fixture
def mock_requests_get_http_error(mocker):
    mock_response = mock.Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
    mock_get = mocker.patch('requests.get', return_value=mock_response)
    return mock_get

@pytest.fixture
def mock_requests_get_request_exception(mocker):
    mock_get = mocker.patch('requests.get', side_effect=requests.exceptions.RequestException("Request Error"))
    return mock_get

def test_get_pokemon_success(mock_requests_get_success):
    result = get_pokemon('ditto')
    
    assert result == {'name': 'ditto'}
    mock_requests_get_success.assert_called_once_with('https://pokeapi.co/api/v2/pokemon/ditto')

def test_get_pokemon_http_error(mock_requests_get_http_error):
    with pytest.raises(RuntimeError, match="Erro HTTP ao buscar Pokémon unknown"):
        get_pokemon('unknown')
    
    mock_requests_get_http_error.assert_called_once_with('https://pokeapi.co/api/v2/pokemon/unknown')

def test_get_pokemon_request_exception(mock_requests_get_request_exception):
    with pytest.raises(RuntimeError, match="Erro ao buscar Pokémon unknown"):
        get_pokemon('unknown')
    
    mock_requests_get_request_exception.assert_called_once_with('https://pokeapi.co/api/v2/pokemon/unknown')
