import pandas as pd
import numpy as np
import streamlit as st


filmes = pd.read_csv('C:/filmes_ratings/dados/filmes.csv')
series = pd.read_csv('C:/filmes_ratings/dados/series.csv')

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

st.dataframe(dados)

