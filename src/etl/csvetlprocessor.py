import pandas as pd
import awswrangler as awsr
from datetime import datetime

class CSVETLProcessor:

    @classmethod
    def extract_data(cls, data_source_path: str, delimiter: str = ",") -> pd.DataFrame:
        """
        Lê dados csv da origem.
        :params: data_source_path: caminho da origem dos dados
        :return: DataFrame com a data de leitura do arquivo <load_at_dt>
        """
        try:
            load_at_dt = datetime.today().strftime('%Y-%m-%d')
            df = awsr.s3.read_csv(data_source_path, delimiter=delimiter)
            df['load_at_dt'] = load_at_dt
            return df
        except Exception as err:
            print(f'Erro ao ler o arquivo CSV: {err}')

    
    @classmethod
    def transform_data(cls, df: pd.DataFrame) -> dict:
        """
        Converte os tipos de dados de um DataFrame para tipos de dados compatíveis com o Amazon Athena
        :params: DataFrame
        :return: retorna um dicionário  com os tipos de dados convertidos
        """
        athena_types = {
            'object': 'string',
            'int64': 'bigint',
            'float64': 'double',
            'bool': 'boolean',
            'datetime64[ns]': 'timestamp'
        }
        try:
            types_dict = {}
            for column, dtype in df.dtypes.items():
                types_dict[column] = athena_types[str(dtype)]
            return types_dict
        except Exception as err:
            print(f'Erro ao converter tipos do pandas para tipos de dados compatíveis com o Amazon Athena: {err}')


    
    @classmethod
    def load_data(cls, df: pd.DataFrame, data_dest_path: str,  athena_types: dict, scd_type: int, description: str, columns_comments: dict, partition_cols: str) -> dict:

        scd_types = {
            1: 'append',
            2: 'overwrite',
            3: 'overwrite_partitions'
        }

        mode = scd_types.get(scd_type)

        try:

            status_load = awsr.s3.to_parquet(
                df=df,
                mode=mode,
                path=data_dest_path,
                schema_evolution=True,
                partition_cols=[f'{partition_cols}'],
                dataset=True,
                database='default',
                table='my_table',
                dtype=athena_types,
                description=description,
                columns_comments=columns_comments
            )

            return status_load
        except Exception as err:
            print(f'Erro ao tentar inserir os dados: {err}')