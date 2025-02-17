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

dados = get_data(
                QUERY_DASH,
                SERVER,
                DATABASE,
                UID,
                PWD
                )



dados = dados[dados['streaming'].isin(['Amazon Prime Video',
                                        'Apple TV Plus','Max','Netflix',
                                        'Paramount Plus','Disney Plus'])]



fig = px.line(
    dados, 
    x="data", 
    y="Media", 
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