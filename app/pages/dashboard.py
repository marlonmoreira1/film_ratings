import pandas as pd
import streamlit as st
import plotly.express as px
from carrossel import get_carousel
import pymssql
import os
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
from collect_data import get_data
from queries import QUERY_FILMES, QUERY_SERIES
from dotenv import load_dotenv

st.set_page_config(page_title='Filmes',layout='wide')

st.markdown("""
        <style>
               .block-container {
                    padding-top: 3rem;
                    padding-bottom: 5rem;
                    padding-left: 2rem;
                    padding-right: 2rem;                    
                }
        </style>
        """, unsafe_allow_html=True)

load_dotenv()

SERVER = os.environ["SERVER"]
DATABASE = os.environ["DATABASE"]
UID = os.environ["UID"]
PWD = os.environ["PWD"]

filmes = get_data(
                QUERY_FILMES,
                SERVER,
                DATABASE,
                UID,
                PWD
                )


series = get_data(QUERY_SERIES,
                SERVER,
                DATABASE,
                UID,
                PWD
                )

st.write(f"##### Fontes dos Dados")
st.markdown(get_carousel(), unsafe_allow_html=True)


colunas_correspondentes = {
    "serie_id": "movie_id",
    "nome_serie": "nome_filme",
    "serie_original": "movie_original",
    "nome_series_en": "nome_filmes_en",
    "serie_type": "film_type"
}

series = series.rename(columns=colunas_correspondentes)

dados = pd.concat([filmes,series])

dados = dados.sort_values(by="nota_score", ascending=False)

dados["nota_score"] = dados["nota_score"].astype(str).str.replace(',', '.').astype(float)

dados = dados[dados['film_type']=='Streaming']

dados = dados[dados['streaming'].isin(['Amazon Prime Video',
                                        'Apple TV Plus','Max','Netflix',
                                        'Paramount Plus','Disney Plus'])]

dados = dados.groupby(["data", "streaming"], as_index=False)["nota_score"].mean().round(1)

fig = px.line(
    dados, 
    x="data", 
    y="nota_score", 
    color="streaming", 
    markers=True, 
    title='Percepção das Plataformas ao longo do tempo.'      
)

fig.update_traces(textposition="top right")  


for trace in fig.data:
    trace.showlegend = False  
    x_final = trace.x[-1]  
    y_final = trace.y[-1]  
    fig.add_annotation(
        x=x_final, 
        y=y_final, 
        text=trace.name, 
        showarrow=False, 
        font=dict(size=12, color=trace.line.color),  
        xshift=25  
    )

st.plotly_chart(fig, use_container_width=True)