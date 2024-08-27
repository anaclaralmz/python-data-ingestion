import pytest
import requests
from unittest import mock
from data_pipeline.data_ingestion import get_pokemon  

def test_get_pokemon(mocker):
    mock_response = mock.Mock()
    mock_response.json.return_value = {'name': 'ditto'}
    mock_response.status_code = 200
    mock_get = mocker.patch('requests.get', return_value=mock_response)

    result = get_pokemon('ditto')
    
    assert result == {'name': 'ditto'}
    mock_get.assert_called_once_with('https://pokeapi.co/api/v2/pokemon/ditto')
