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


SERVER = st.secrets["SERVER"]
DATABASE = st.secrets["DATABASE"]
UID = st.secrets["UID"]
PWD = st.secrets["PWD"]

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


dados["data"] = pd.to_datetime(dados["data"])

dados['media_movel'] = dados.groupby('streaming')['nota_score'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

dados = dados.groupby(["data", "streaming"], as_index=False)["media_movel"].mean().round(1)

fig = px.line(
    dados, 
    x="data", 
    y="media_movel", 
    color="streaming", 
    markers=True         
)

fig.update_layout(
    legend=dict(
        orientation="h",  
        yanchor="bottom",
        y=1.02,  
        xanchor="right",
        x=0.8
    )
)

st.markdown("<h5 style='text-align: center;'>Percepção das Plataformas ao Longo do Tempo</h5>", unsafe_allow_html=True)
st.plotly_chart(fig, use_container_width=True)


st.markdown("<h5 style='text-align: center;'>Fontes dos Dados</h5>", unsafe_allow_html=True)
st.markdown(get_carousel("150%","125px","-3%","2.5%"), unsafe_allow_html=True)