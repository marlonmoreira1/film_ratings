from data_extraction import fetch_imdb_rating, extract_data_rottentomatoes, extrair_dados_filmow, extrair_dados_adorocinema, extrair_filmes_letterboxd, extrair_dados_trakt, discover_movies, now_playing_movies, fetch_movie_details_by_name,load_data, now_playing_series 
from processor import get_imdb_name, concat_rt, concat_filmow, join_omdbdfs, merge_dfs, printar_filmes, filter_processing_final_df, merge_new_movies, print_film, join_omdbdfs_series, weekly_filter, replace_rt, get_streamings
import pandas as pd
import re
import random
import numpy as np
import unicodedata
from datetime import datetime, timedelta
import time
from unidecode import unidecode
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
import json
import pymssql
from prefect import flow, task, get_run_logger
import os
from prefect.blocks.system import Secret
import warnings
warnings.simplefilter(action='ignore')

pt = "pt-BR"
en = "en-US"

API_KEY = Secret.load("api-key").get()
API = Secret.load("api").get()
CLIENT_ID = Secret.load("client-id").get()
SERVER = Secret.load("server").get()
DATABASE = Secret.load("database").get()
UID = Secret.load("uid").get()
PWD = Secret.load("pwd").get()

base_url = "https://api.themoviedb.org/3"

data = datetime.today() - timedelta(days=1)
data_hoje = data.strftime('%Y-%m-%d')
data_inicio = datetime.today() - timedelta(days=17)
data_ontem = data_inicio.strftime('%Y-%m-%d')


@flow(name="WorkFlow das Series.")
def series_flow(timeout_seconds=1800):

    logger = get_run_logger()       
    
    end_point_streaming = "discover/tv"
    tipo = "tv"

    filter_columns = ['movie_id','nome_filme', 'movie_original', 'nome_filmes_en', 'data_lancamento_omdb',
       'poster', 'streaming_trakt', 'nota_omdb', 'nota_imdb_omdb_en0', 'nota_imdb_en0', 'nota_adorocinema',       
       'nota_filmow', 'nota_rottentomatoes','nota_trakt']

    columns_to_convert = ['nota_imdb_omdb_en0', 'nota_imdb_en0', 'nota_adorocinema',
        'nota_filmow','nota_trakt']


    columns_to_multiply = ['nota_filmow','nota_adorocinema']
    columns_to_divide = ['nota_rottentomatoes']

    columns_to_score = ['nota_omdb', 'nota_imdb_en0', 'nota_imdb_en0','nota_filmow',
        'nota_adorocinema', 'nota_rottentomatoes', 'nota_trakt']

    data = datetime.today() - timedelta(days=1)
    data_hoje = data.strftime('%Y-%m-%d')
    data_inicio = datetime.today() - timedelta(days=17)
    data_ontem = data_inicio.strftime('%Y-%m-%d')      


    url_rt = "https://www.rottentomatoes.com/browse/tv_series_browse/sort:newest?page=10"    

    df_rt = extract_data_rottentomatoes.submit(url_rt)
    df_rt_clean = replace_rt(df_rt.result())  


    url_filmow = "https://filmow.com/series/?order=best"    
    df_filmow = extrair_dados_filmow.submit(url_filmow,16)
    

    url_adorocinema = "https://www.adorocinema.com/series-tv/"
    df_adorocinema = extrair_dados_adorocinema.submit(url_adorocinema, num_paginas=16)    

    pt_movies_cinema = now_playing_series.submit(pt,data_ontem,data_hoje,API_KEY,base_url,end_point_streaming,'name','original_name')
    en_movies_cinema = now_playing_series.submit(en,data_ontem,data_hoje,API_KEY,base_url,end_point_streaming,'name','original_name')    

    omdb_df = join_omdbdfs_series.submit(        
        pt_movies_cinema.result(),
        en_movies_cinema.result()
        )

    omdb_df_result = omdb_df.result()
    
    df_imdb = fetch_imdb_rating.submit(omdb_df_result['movie_original'],API)

    url_base = "https://api.trakt.tv/shows/"
    trakt_df = extrair_dados_trakt.submit(url_base,omdb_df_result,CLIENT_ID)    

    pt_dfs = [df_imdb.result(),df_filmow.result(),df_adorocinema.result()]
    en_dfs = [df_imdb.result(),df_rt_clean,trakt_df.result()]
    for df in en_dfs:
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Sample:\n{df.head(3)}") 

    df_final = merge_dfs(omdb_df_result,pt_dfs,en_dfs)    
    

    filmes = filter_processing_final_df(df_final,
                                        3,
                                        filter_columns,
                                        columns_to_convert,
                                        columns_to_multiply,
                                        columns_to_divide,
                                        columns_to_score)    
    

    movies_df = fetch_movie_details_by_name(filmes,API_KEY,base_url,tipo)    

    new_filmes = merge_new_movies(filmes,movies_df)

    new_filmes['streaming'] = new_filmes.apply(get_streamings,axis=1)         

    load_data(
            new_filmes,
            'Notas_Series',
            SERVER,
            DATABASE,
            UID,
            PWD
        )


@flow(name="Serie Workflow")
def main_serie_flow(timeout_seconds=1800):
    series_flow()
       


if __name__ == "__main__":
    main_serie_flow()    
        