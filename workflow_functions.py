from data_extraction import scrape_imdb_titles_selenium, extract_movies_data, extrair_filmes_e_notas, extrair_dados_adorocinema, extrair_filmes_letterboxd, extrair_dados_trakt, discover_movies, now_playing_movies, fetch_movie_details_by_name,load_data 
from processor import get_imdb_name, concat_rt, concat_filmow, join_omdbdfs, merge_dfs, printar_filmes, filter_processing_final_df, merge_new_movies, print_film, join_omdbdfs_series,weekly_filter,replace_rt
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
import pyodbc
from prefect import flow, task
import os
from prefect.blocks.system import Secret
import warnings
warnings.simplefilter(action='ignore')

pt = "pt-BR"
en = "en-US"

API_KEY = Secret.load("api-key").get()
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

@task
def get_columns(df):
    print(df)

@flow(name="WorkFlow dos Filmes.")
def movies_flow():
    
    end_point_cinema = "discover/movie"
    end_point_streaming = "movie/now_playing"
    tipo = "movie"

    filter_columns = ['movie_id','nome_filme', 'movie_original', 'nome_filmes_en', 'data_lancamento_omdb',
       'poster', 'nota_omdb', 'nota_imdb_omdb_en0', 'nota_imdb_en0', 'nota_adorocinema',       
       'nota_filmow', 'nota_rottentomatoes', 'nota_letterbox','nota_trakt']

    columns_to_convert = ['nota_imdb_omdb_en0', 'nota_imdb_en0', 'nota_adorocinema',
        'nota_filmow','nota_letterbox','nota_trakt']


    columns_to_multiply = ['nota_filmow','nota_adorocinema', 'nota_letterbox']
    columns_to_divide = ['nota_rottentomatoes']

    columns_to_score = ['nota_omdb', 'nota_imdb_en0', 'nota_imdb_en0','nota_filmow',
        'nota_adorocinema', 'nota_rottentomatoes', 'nota_letterbox', 'nota_trakt']
    

    url_imdb = f"https://www.imdb.com/search/title/?title_type=feature&release_date={data_ontem},{data_hoje}&num_votes=1,&adult=include&sort=user_rating,desc"
    df_imdb = scrape_imdb_titles_selenium.submit(url_imdb)

    df_imdb2 = get_imdb_name(df_imdb.result())
    

    url_rt = "https://www.rottentomatoes.com/browse/movies_at_home/sort:newest?page=4"
    url_cinema = "https://www.rottentomatoes.com/browse/movies_in_theaters/sort:newest?page=4"

    df_rt = extract_movies_data.submit(url_rt)
    df_rt_cinema = extract_movies_data.submit(url_cinema)
    df_rt_final = concat_rt.submit(df_rt.result(),df_rt_cinema.result())
    

    url_filmow = "https://filmow.com/filmes-nos-cinemas/?order=newer"
    url_filmow_streaming = "https://filmow.com/filmes-em-dvd/?order=new-release"

    df_filmow = extrair_filmes_e_notas.submit(url_filmow,2)
    df_filmow_streaming = extrair_filmes_e_notas.submit(url_filmow_streaming,21)
    df_filmow_final = concat_filmow.submit(df_filmow.result(),df_filmow_streaming.result())
    

    url_adorocinema = "https://www.adorocinema.com/filmes-todos/"
    df_adorocinema = extrair_dados_adorocinema.submit(url_adorocinema, num_paginas=16)           
   
    letterbox_df = extrair_filmes_letterboxd.submit()    

    en_movies = discover_movies.submit(data_ontem, data_hoje,en,API_KEY,base_url,end_point_cinema)
    pt_movies = discover_movies.submit(data_ontem, data_hoje,pt,API_KEY,base_url,end_point_cinema)

    pt_movies_cinema = now_playing_movies.submit(pt,API_KEY,base_url,end_point_streaming,'title','original_title')
    en_movies_cinema = now_playing_movies.submit(en,API_KEY,base_url,end_point_streaming,'title','original_title')    

    omdb_df = join_omdbdfs.submit(
        pt_movies.result(),
        en_movies.result(),
        pt_movies_cinema.result(),
        en_movies_cinema.result()
        )

    url_base = "https://api.trakt.tv/movies/"
    trakt_df = extrair_dados_trakt.submit(url_base,omdb_df.result(),CLIENT_ID)

    pt_dfs = [df_imdb2,df_filmow_final.result(),df_adorocinema.result()]
    en_dfs = [df_imdb2,df_rt_final.result(),letterbox_df.result(),trakt_df.result()]

    df_final = merge_dfs(omdb_df.result(),pt_dfs,en_dfs)

    df_final = weekly_filter(df_final)

    filmes = filter_processing_final_df(df_final,
                                        6,
                                        filter_columns,
                                        columns_to_convert,
                                        columns_to_multiply,
                                        columns_to_divide,
                                        columns_to_score)    
    

    movies_df = fetch_movie_details_by_name(filmes,API_KEY,base_url,tipo)

    new_filmes = merge_new_movies(filmes,movies_df)       

    load_data(
            new_filmes,
            'Notas_Filmes',            
            SERVER,
            DATABASE,
            UID,
            PWD
        )



@flow(name="WorkFlow das Series.")
def series_flow():    
    
    end_point_streaming = "tv/on_the_air"
    tipo = "tv"

    filter_columns = ['movie_id','nome_filme', 'movie_original', 'nome_filmes_en', 'data_lancamento_omdb',
       'poster', 'nota_omdb', 'nota_imdb_omdb_en0', 'nota_imdb_en0', 'nota_adorocinema',       
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

    url_imdb = f"https://www.imdb.com/search/title/?title_type=tv_series&num_votes=1,&adult=include"
    df_imdb = scrape_imdb_titles_selenium.submit(url_imdb)

    df_imdb2 = get_imdb_name(df_imdb.result())    


    url_rt = "https://www.rottentomatoes.com/browse/tv_series_browse/sort:newest?page=10"    

    df_rt = extract_movies_data.submit(url_rt)
    df_rt_clean = replace_rt(df_rt.result())  


    url_filmow = "https://filmow.com/series/?order=best"    
    df_filmow = extrair_filmes_e_notas.submit(url_filmow,16)
    

    url_adorocinema = "https://www.adorocinema.com/series-tv/"
    df_adorocinema = extrair_dados_adorocinema.submit(url_adorocinema, num_paginas=16)      

    

    pt_movies_cinema = now_playing_movies.submit(pt,API_KEY,base_url,end_point_streaming,'name','original_name')
    en_movies_cinema = now_playing_movies.submit(en,API_KEY,base_url,end_point_streaming,'name','original_name')    

    omdb_df = join_omdbdfs_series.submit(        
        pt_movies_cinema.result(),
        en_movies_cinema.result()
        )   
    

    url_base = "https://api.trakt.tv/shows/"
    trakt_df = extrair_dados_trakt.submit(url_base,omdb_df.result(),CLIENT_ID)

    pt_dfs = [df_imdb2,df_filmow.result(),df_adorocinema.result()]
    en_dfs = [df_imdb2,df_rt_clean,trakt_df.result()]

    df_final = merge_dfs(omdb_df.result(),pt_dfs,en_dfs)    
    

    filmes = filter_processing_final_df(df_final,
                                        4,
                                        filter_columns,
                                        columns_to_convert,
                                        columns_to_multiply,
                                        columns_to_divide,
                                        columns_to_score)    
    

    movies_df = fetch_movie_details_by_name(filmes,API_KEY,base_url,tipo)    

    new_filmes = merge_new_movies(filmes,movies_df)       

    load_data(
            new_filmes,
            'Notas_Series',
            SERVER,
            DATABASE,
            UID,
            PWD
        )


@flow(name="Main Workflow")
def main_flow():
    movies_flow()
    series_flow()


if __name__ == "__main__":
    main_flow()    
        