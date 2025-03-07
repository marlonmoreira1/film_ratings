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

st.set_page_config(page_title='Filmes',layout='centered')

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


with st.expander("Sobre o App"):
    st.write("""
    **Bem-vindo ao seu novo aliado para encontrar aquele filme ou série legal!** 🎬🍿

    A ideia é simples:

    Através de dados coletados de várias plataformas de avaliação de filmes e séries como IMDb, Rotten Tomatoes, The Movie Database (TMDb), AdoroCinema, Filmow, Trakt e Letterboxd — considerando somente a avaliação de usuários (e não de críticos) — o app oferece sugestões do que assistir com base na média dessas avaliações. Apenas filmes e séries atuais que estão em exibição dentro de uma janela de 15 dias são considerados.

    A média dessas notas ajuda a descobrir o que está em alta, com a perspectiva de muitas pessoas de diferentes perfis. Aqui, você pode encontrar as melhores opções de filmes e séries, com base em dados reais de usuários, para acertar na escolha e não perder tempo navegando sem fim em várias plataformas diferentes buscando o que assistir.

    Além de destacar o que está em alta, o app também ajuda a descobrir **filmes e séries que talvez não estivessem no seu radar**, ampliando suas opções e trazendo boas surpresas.

    **Em resumo:** O objetivo é usar dados para encontrar boas opções de entretenimento, evitar a frustração de indecisão e dar sugestões valiosas sobre o que está bombando no momento! 🎥

    🎬 **Boa escolha e boa sessão! Obrigado por visitar o Watch Finder!** 🍿
    """)


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

st.markdown("<h5 style='text-align: center;'>Fontes dos Dados</h5>", unsafe_allow_html=True)
st.markdown(get_carousel("110%","80px","-17%","2.5%"), unsafe_allow_html=True)

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


if filtro:
    dados = json_filtro[filtro]
else:
    dados=filmes
    filtro="Filmes"

dados = dados.sort_values(by="nota_score", ascending=False)

dados["nota_score"] = dados["nota_score"].astype(str).str.replace(',', '.').astype(float).round(1)

lista_streamings = [
    "Disney Plus",
    "Amazon Prime Video",
    "Apple TV Plus",
    "Claro tv+",
    "Max",
    "Netflix",
    "Univer Video",
    "Paramount Plus",
    "Paramount+ Amazon Channel",
    "Globoplay",
    "Crunchyroll",
    "YouTube",
    "Looke"
]

def changetype(row):
    if row['streaming'] in lista_streamings:
        return "Streaming"
    return row['film_type']

def update_streaming(row):
    if row['streaming'] == 'Apple TV+':
        return 'Apple TV Plus'
    elif row['streaming'] == 'Disney+':
        return 'Disney Plus'
    return row['streaming']

dados['streaming'] = dados.apply(update_streaming,axis=1)
dados['film_type'] = dados.apply(changetype,axis=1)

tipo_colunas = st.columns([0.9,1,1])

with tipo_colunas[1]:
    tipo_filtro = st.segmented_control(
        "",
        options=dados['film_type'].unique(),
        selection_mode="single",
        default=None
    )

st.write('')

if tipo_filtro:
    dados = dados[dados['film_type']==tipo_filtro]

if tipo_filtro == 'Streaming':
    plataformas = st.radio(
                    "",
                    dados[(~dados['streaming'].isna())&\
                    (dados['streaming'].isin(lista_streamings))]\
                    ['streaming'].unique(),
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

    with borda[1].container(border=True):        
        st.markdown(f"#### {i+inicio+1}° - {nome}") 
        st.write(f"##### - **Nota:** {nota}")
        st.write(f"##### - {extra_info}")
        st.image(poster, caption=nome,use_container_width=True)

    st.write('')

try:
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
        
except st.errors.StreamlitValueAboveMaxError:
    st.session_state.pagina_atual = 1
    st.rerun()
