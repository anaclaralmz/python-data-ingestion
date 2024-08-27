from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
import pytest
from unittest import mock
import clickhouse_connect
import pandas as pd

@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv('CLICKHOUSE_HOST', 'localhost')
    monkeypatch.setenv('CLICKHOUSE_PORT', '8123')

@pytest.fixture
def mock_clickhouse_client(mocker):
    mock_client = mock.Mock()
    mocker.patch('clickhouse_connect.get_client', return_value=mock_client)
    return mock_client

def test_get_client(mock_env_vars, mock_clickhouse_client):
    client = get_client()
    assert client is not None
    clickhouse_connect.get_client.assert_called_once_with(host='localhost', port='8123')

def test_execute_sql_script(mock_env_vars, mock_clickhouse_client, mocker):
    mock_open = mocker.patch('builtins.open', mock.mock_open(read_data='SELECT 1'))
    script_path = '/path/to/create_table.sql'
    
    client = execute_sql_script(script_path)
    
    mock_open.assert_called_once_with(script_path, 'r')
    mock_clickhouse_client.command.assert_called_once_with('SELECT 1')
    assert client is mock_clickhouse_client

def test_insert_dataframe(mock_clickhouse_client):
    df = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
    table_name = 'test_table'
    
    insert_dataframe(mock_clickhouse_client, table_name, df)
    
    mock_clickhouse_client.insert_df.assert_called_once_with(table_name, df)
