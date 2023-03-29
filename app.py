from configparser import ConfigParser
from src.etl.csvetlprocessor import CSVETLProcessor

config = ConfigParser()
config.read('./config/config.ini')

data_source_bucket = config['ORIGEM']['BUCKET']
data_source_key = config['ORIGEM']['KEY']
data_source_path = f's3://{data_source_bucket}/{data_source_key}/'

data_dest_bucket = config['DESTINO']['BUCKET']
data_dest_key = config['DESTINO']['KEY']
data_dest_path = f's3://{data_dest_bucket}/{data_dest_key}/'


data_source_df = CSVETLProcessor.extract_data(data_source_path=data_source_path)

athena_types = CSVETLProcessor.transform_data(df=data_source_df)

description = 'Tabela que contem todas as informações com os dados dos sobreviventes do titanic'
columns_comments = {'PassengerId': 'PassengerId', 'Survived': 'Survived', 'Pclass': 'Pclass', 'Name': 'Name', 'Sex': 'Sex'}
partition_cols = 'pclass'

process_status = CSVETLProcessor.load_data(
    df=data_source_df, 
    data_dest_path=data_dest_path, 
    athena_types=athena_types, 
    scd_type=2, 
    description=description, 
    columns_comments=columns_comments, 
    partition_cols=partition_cols
)

