from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.data_ingestion import get_pokemon
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd

app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("raw-data")

# Executar o script SQL para criar a tabela e a view
execute_sql_script('sql/create_table.sql')
execute_sql_script('sql/create_view.sql')

@app.route('/data/<name>', methods=['GET'])
def receive_data(name):
    data = get_pokemon(name)
    
    filename = process_data(data)

    upload_file("raw-data", filename)

    # Ler arquivo Parquet do MinIO
    download_file("raw-data", filename, f"downloaded_{filename}")
    df_parquet = pd.read_parquet(f"downloaded_{filename}")

    # Preparar e inserir dados no ClickHouse
    df_prepared = prepare_dataframe_for_insert(df_parquet)
    client = get_client()  # Obter o cliente ClickHouse
    insert_dataframe(client, 'working_data', df_prepared)

    return jsonify({"message": "Pokemon: {name} recebido, armazenado e processado com sucesso."}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)