import pandas as pd
import numpy as np
import time
import os
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from carrossel import get_carousel
import pymssql
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

series['film_type'] = series['film_type'].replace('Cinema','TV')

json_filtro = {
            "Filmes": filmes,
            "Séries": series
            }

colunas = st.columns([1, 1, 1]) 

with colunas[1]:
    filtro = st.segmented_control(
        "",
        options=json_filtro.keys(),
        selection_mode="single",
        default="Filmes"
    )

st.write('')
if filtro:
    dados = json_filtro[filtro]
else:
    dados=filmes
    filtro="Filmes"

dados = dados.sort_values(by="nota_score", ascending=False)

dados["nota_score"] = dados["nota_score"].astype(str).str.replace(',', '.').astype(float).round(1)

tipo_colunas = st.columns([1, 1, 1])

with colunas[1]:
    tipo_filtro = st.segmented_control(
        "",
        options=dados['film_type'].unique(),
        selection_mode="single",
        default=None
    )


if tipo_filtro:
    dados = dados[dados['film_type']==tipo_filtro]

if tipo_filtro == 'Streaming':
    plataformas = st.radio(
                    "",
                    dados[~dados['streaming'].isna()]['streaming'].unique(),
                    index=None,
                    horizontal=True
                        )
    if plataformas:
        dados = dados[dados['streaming']==plataformas]
        

filmes_por_pagina = 10  
total_produtos = dados.shape[0]
total_paginas = (total_produtos // filmes_por_pagina) + 1

if 'pagina_atual' not in st.session_state:
    st.session_state.pagina_atual = 1

inicio = (st.session_state.pagina_atual - 1) * filmes_por_pagina
fim = inicio + filmes_por_pagina
filmes_pagina = dados["nome_filme"].unique()[inicio:fim]

cols = st.columns([2, 1])  

with cols[0]:  
    st.write(f"### {filtro} mais bem Avaliados")  

with cols[1]:  
    st.write(f"### Página {st.session_state.pagina_atual} de {total_paginas}") 

st.write('')
st.write('')

for i, filme in enumerate(filmes_pagina):
    
    filme = dados[dados["nome_filme"] == filme].iloc[0]
    nome = filme["nome_filme"]
    poster = filme["poster"]
    nota = filme["nota_score"]
    ranking = filme["posicao"]
    if filme["film_type"] == 'Streaming':
        streaming = filme["streaming"]
        extra_info = f"**Streaming:** {streaming}"
    else:        
        extra_info = f"**Cinema**" if filtro == "Filmes" else f"**TV**"

    borda = st.columns([0.1, 1, 0.2])

    with borda[1].container(border=True):        #TO DO
        st.markdown(f"#### {i+inicio+1}° - {nome}") #Tirar o ranking pelo indice do loop e trazer da consulta do banco.    
        st.write(f"##### - **Nota:** {nota}")
        st.write(f"##### - {extra_info}")
        st.image(poster, caption=nome)

    st.write('')

pagina_atual = st.number_input(
    "Página",
    min_value=1,
    max_value=total_paginas,
    step=1,
    value=st.session_state.pagina_atual
)


if pagina_atual != st.session_state.pagina_atual:
    st.session_state.pagina_atual = pagina_atual     
    st.rerun()
        
    