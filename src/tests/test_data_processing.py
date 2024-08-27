from datetime import datetime
from data_pipeline.data_processing import prepare_dataframe_for_insert, process_data
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unittest import mock

@pytest.fixture
def mock_datetime_now(mocker):
    mock_datetime = mocker.patch('data_pipeline.data_processing.datetime')
    mock_datetime.now.return_value = datetime(2023, 8, 27, 12, 0, 0)
    return mock_datetime

@pytest.fixture
def mock_pq_write_table(mocker):
    return mocker.patch('data_pipeline.data_processing.pq.write_table')

def test_process_data_success(mock_datetime_now, mock_pq_write_table, mocker):
    mock_data = {
        'base_experience': 64,
        'height': 7,
        'id': 1,
        'name': 'bulbasaur'
    }

    expected_filename = "raw_data_20230827120000.parquet"
    filename = process_data(mock_data)
    
    assert filename == expected_filename
    mock_pq_write_table.assert_called_once()
    assert filename == expected_filename

def test_process_data_missing_key(mock_datetime_now, mock_pq_write_table):
    mock_data = {
        'base_experience': 64,
        'height': 7,
        'id': 1
        # tirei o 'name' 
    }

    with pytest.raises(ValueError, match=r"Erro ao processar os dados do Pokémon. Chave ausente: 'name'"):
        process_data(mock_data)

def test_process_data_general_exception(mock_datetime_now, mocker):
    mock_data = {
        'base_experience': 64,
        'height': 7,
        'id': 1,
        'name': 'bulbasaur'
    }

    mock_pq_write_table = mocker.patch('data_pipeline.data_processing.pq.write_table', side_effect=Exception("Erro inesperado"))

    with pytest.raises(RuntimeError, match=r"Erro ao processar os dados do Pokémon."):
        process_data(mock_data)

def test_prepare_dataframe_for_insert(mock_datetime_now):
    df = pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })

    result_df = prepare_dataframe_for_insert(df)
    
    assert 'data_ingestao' in result_df.columns
    assert 'dado_linha' in result_df.columns
    assert 'tag' in result_df.columns
    assert (result_df['data_ingestao'] == datetime(2023, 8, 27, 12, 0, 0)).all()
    assert (result_df['tag'] == 'example_tag').all()
    assert result_df['dado_linha'].iloc[0] == '{"column1":1,"column2":"a","data_ingestao":1693137600000}'
