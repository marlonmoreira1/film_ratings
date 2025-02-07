import pandas as pd
import numpy as np
import streamlit as st
from streamlit_autorefresh import st_autorefresh


filmes = pd.read_csv('C:/filmes_ratings/dados/filmes.csv')
series = pd.read_csv('C:/filmes_ratings/dados/series.csv')

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

dados["nota_score"] = dados["nota_score"].str.replace(',', '.').astype(float).round(1)

st.title(f"{filtro} mais bem Avaliados")

filmes_por_pagina = 10  
total_produtos = dados.shape[0]
total_paginas = (total_produtos // filmes_por_pagina) + 1

if 'pagina_atual' not in st.session_state:
    st.session_state.pagina_atual = 1

inicio = (st.session_state.pagina_atual - 1) * filmes_por_pagina
fim = inicio + filmes_por_pagina
filmes_pagina = dados["nome_filme"].unique()[inicio:fim]


st.write(f"### Página {st.session_state.pagina_atual} de {total_paginas}")
for filme in filmes_pagina:
    
    filme = dados[dados["nome_filme"] == filme].iloc[0]
    nome = filme["nome_filme"]
    poster = filme["poster"]
    nota = filme["nota_score"]
    if filme["film_type"] == 'Streaming':
        streaming = filme["streaming"]
        extra_info = f"**Streaming:** {streaming}"
    else:
        studio = filme["studio"]
        extra_info = f"**Studio:** {studio}"

    
    st.markdown(f"### {nome}")  
    st.image(poster, caption=nome, use_container_width=True)  
    st.write(f"- **Nota:** {nota}")
    st.write(f"- {extra_info}")


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
        
    