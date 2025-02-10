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

st.markdown(get_carousel(), unsafe_allow_html=True)


colunas_correspondentes = {
    "serie_id": "movie_id",
    "nome_serie": "nome_filme",
    "serie_original": "movie_original",
    "nome_series_en": "nome_filmes_en",
    "serie_type": "film_type"
}

series = series.rename(columns=colunas_correspondentes)

json_filtro = {
            "Filmes": filmes,
            "Series": series
            }

filtro = st.segmented_control(
    "Série ou Filme",
    options=json_filtro.keys(),
    selection_mode="single",
    default="Filmes"
)


dados = json_filtro[filtro]

dados = dados.sort_values(by="nota_score", ascending=False)

dados["nota_score"] = dados["nota_score"].astype(str).str.replace(',', '.').astype(float).round(1)

dados = dados[dados['film_type']=='Streaming']

dados = dados.groupby(["data", "streaming"], as_index=False)["nota_score"].mean()

fig = px.line(dados, x="data", y="nota_score", color="streaming", markers=True, title='Percepção das Plataformas ao longo do tempo.')

st.plotly_chart(fig, use_container_width=True)