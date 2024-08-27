import pytest
from unittest.mock import patch
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file

class MockS3Error(Exception):
    pass

@patch('data_pipeline.minio_client.minio_client')
def test_create_bucket_if_not_exists_success(mock_minio_client):
    mock_minio_client.bucket_exists.return_value = False
    create_bucket_if_not_exists('test-bucket')
    mock_minio_client.make_bucket.assert_called_once_with('test-bucket')

@patch('data_pipeline.minio_client.minio_client')
def test_create_bucket_if_not_exists_already_exists(mock_minio_client):
    mock_minio_client.bucket_exists.return_value = True
    create_bucket_if_not_exists('test-bucket')
    mock_minio_client.make_bucket.assert_not_called()

@patch('data_pipeline.minio_client.minio_client')
def test_upload_file_success(mock_minio_client):
    upload_file('test-bucket', '/path/to/file.txt')
    mock_minio_client.fput_object.assert_called_once_with('test-bucket', 'file.txt', '/path/to/file.txt')

@patch('data_pipeline.minio_client.minio_client')
def test_upload_file_error(mock_minio_client):
    mock_minio_client.fput_object.side_effect = MockS3Error("Mock error")
    with pytest.raises(MockS3Error):
        upload_file('test-bucket', '/path/to/file.txt')

@patch('data_pipeline.minio_client.minio_client')
def test_download_file_success(mock_minio_client):
    download_file('test-bucket', 'file.txt', '/path/to/local/file.txt')
    mock_minio_client.fget_object.assert_called_once_with('test-bucket', 'file.txt', '/path/to/local/file.txt')

@patch('data_pipeline.minio_client.minio_client')
def test_download_file_error(mock_minio_client):
    mock_minio_client.fget_object.side_effect = MockS3Error("Mock error")
    with pytest.raises(MockS3Error):
        download_file('test-bucket', 'file.txt', '/path/to/local/file.txt')