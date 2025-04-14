from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
import json
import pymssql
import pandas as pd
import requests
import time
import logging
import streamlit as st

@st.cache_data(ttl=87000,show_spinner=False)
def get_data(query, server, database, uid, pwd):
    logging.info("Iniciando a consulta ao banco de dados.")   
    connection_string = f"mssql+pymssql://{uid}:{pwd}@{server}/{database}"     
    for attempt in range(1, 7):
        try:
            logging.info(f"Tentando conexão (tentativa {attempt}/7)...")
            engine = create_engine(connection_string) 
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
            logging.info("Consulta bem-sucedida. Dados carregados.")                        
            return df  

        except OperationalError as e:
            logging.error(f"Tentativa {attempt}/7 falhou. Erro: {e}")            
            
            if attempt < 6:                
                time.sleep(10)
            else:
                logging.error("Máximo de tentativas atingido. Falha ao conectar ao banco de dados.")               
                raise



          


    

    