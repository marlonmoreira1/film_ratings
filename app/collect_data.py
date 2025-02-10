from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
import json
import pymssql
import pandas as pd
import requests
import time
import streamlit as st

@st.cache_data(ttl=86400,show_spinner=False)
def get_data(query, server, database, uid, pwd):    
    connection_string = f"mssql+pymssql://{uid}:{pwd}@{server}/{database}"     
    for attempt in range(1, 7):
        try:
            engine = create_engine(connection_string) 
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)                        
            return df  

        except OperationalError as e:
            print(f"Tentativa {attempt}/7 falhou. Erro: {e}")
            
            if attempt < 6:
                print(f"Aguardando 10 segundos antes de tentar novamente...")
                time.sleep(10)
            else:
                print("Máximo de tentativas atingido. Falha ao conectar ao banco de dados.")
                raise



          


    

    