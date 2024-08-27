import logging
from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.data_ingestion import get_pokemon
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd

app = Flask(__name__)

# Configurar logging
logging.basicConfig(level=logging.INFO)

def safe_create_bucket_if_not_exists(bucket_name):
    try:
        create_bucket_if_not_exists(bucket_name)
    except Exception as e:
        app.logger.error(f"Erro ao criar bucket: {e}")
        raise

def safe_execute_sql_script(script_path):
    try:
        execute_sql_script(script_path)
    except Exception as e:
        app.logger.error(f"Erro ao executar script SQL {script_path}: {e}")
        raise

try:
    # Criar bucket se não existir
    safe_create_bucket_if_not_exists("raw-data")

    # Executar o script SQL para criar a tabela e a view
    safe_execute_sql_script('sql/create_table.sql')
    safe_execute_sql_script('sql/create_view.sql')
except Exception as e:
    app.logger.error(f"Erro ao inicializar o aplicativo: {e}")
    exit(1)

@app.route('/data/<name>', methods=['GET'])
def receive_data(name):
    try:
        data = get_pokemon(name)
    except Exception as e:
        app.logger.error(f"Erro ao obter dados do Pokémon {name}: {e}")
        return jsonify({"error": "Erro ao obter dados do Pokémon."}), 500

    try:
        filename = process_data(data)
        upload_file("raw-data", filename)
    except Exception as e:
        app.logger.error(f"Erro ao processar ou carregar arquivo {filename}: {e}")
        return jsonify({"error": "Erro ao processar ou carregar o arquivo."}), 500

    try:
        # Ler arquivo Parquet do MinIO
        download_file("raw-data", filename, f"downloaded_{filename}")
        df_parquet = pd.read_parquet(f"downloaded_{filename}")
        
        # Preparar e inserir dados no ClickHouse
        df_prepared = prepare_dataframe_for_insert(df_parquet)
        client = get_client()  # Obter o cliente ClickHouse
        insert_dataframe(client, 'working_data', df_prepared)
    except Exception as e:
        app.logger.error(f"Erro ao processar ou inserir dados: {e}")
        return jsonify({"error": "Erro ao processar ou inserir dados."}), 500

    return jsonify({"message": "Pokemon recebido, armazenado e processado com sucesso."}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)