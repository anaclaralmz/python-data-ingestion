import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def process_data(data):
    try:
        # Criar DataFrame e salvar como Parquet
        df = pd.DataFrame([{
            'base_experience': data['base_experience'],
            'height': data['height'],
            'id': data['id'],
            'name': data['name']
        }])

        filename = f"raw_data_{data.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
        
        return filename
    
    except KeyError as message:
        print(f"Chave ausente nos dados do Pokémon. KeyError: {message}")
        raise ValueError(f"Erro ao processar os dados do Pokémon. Chave ausente: {message}") from message
    except Exception as message:
        print(f"Erro ao processar os dados do Pokémon. Exception: {message}")
        raise RuntimeError(f"Erro ao processar os dados do Pokémon.") from message


def prepare_dataframe_for_insert(df):
    df['data_ingestao'] = datetime.now()
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    df['tag'] = 'example_tag'
    return df[['data_ingestao', 'dado_linha', 'tag']]