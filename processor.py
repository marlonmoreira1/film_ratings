import pandas as pd
import re
import random
import numpy as np
import unicodedata
from datetime import datetime, timedelta
import time
from unidecode import unidecode
from prefect import flow, task
import warnings
warnings.simplefilter(action='ignore')


@task(retries=5, retry_delay_seconds=15)
def join_omdbdfs(pt_movies,en_movies,pt_movies_cinema,en_movies_cinema):
    en_movies = en_movies.rename(columns={"movies": "movies_en"})
    en_movies_cinema = en_movies_cinema.rename(columns={"movies": "movies_en"})

    theomdb_df = pd.merge(
        pt_movies, en_movies[['movies_en','movie_original']], 
        on="movie_original",  
        how="inner"              
    )

    theomdbcinema_df = pd.merge(
    pt_movies_cinema, en_movies_cinema[['movies_en','movie_original']], 
    on="movie_original",  
    how="inner"              
    )

    omdb_df = pd.concat([theomdb_df,theomdbcinema_df])

    return omdb_df




@task(retries=5, retry_delay_seconds=15)
def join_omdbdfs_series(pt_movies_cinema,en_movies_cinema):
    
    en_movies_cinema = en_movies_cinema.rename(columns={"movies": "movies_en"})    

    theomdbcinema_df = pd.merge(
    pt_movies_cinema, en_movies_cinema[['movies_en','movie_original']], 
    on="movie_original",  
    how="inner"              
    )   

    return theomdbcinema_df



def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])



def preprocess_text(col):
    return (
        col.fillna("")
           .str.lower()                          
           .str.strip()                          
           .apply(remove_accents)               
           .str.replace(r'[^\w\s]', '', regex=True)  
           .str.replace(r'\s+', '', regex=True)  
    )



@task(retries=5, retry_delay_seconds=15)
def merge_dfs(omdb_df,pt_dfs,en_dfs):

    omdb_df['nome_filme'] = omdb_df['movies']
    omdb_df['nome_filmes_en'] = omdb_df['movies_en']

    omdb_df['movies'] = preprocess_text(omdb_df['movies'])
    omdb_df['movies_en'] = preprocess_text(omdb_df['movies_en'])

    df_final = omdb_df    

    for df in pt_dfs:
        df['filmes'] = preprocess_text(df['filmes'])
        df_final = pd.merge(
            df_final,
            df,
            left_on="movies",
            right_on="filmes",
            how="outer",
            suffixes=("_omdb", "_pt")
        )

    for i, df in enumerate(en_dfs):
        df['filmes'] = preprocess_text(df['filmes'])
        df_final = pd.merge(
            df_final,
            df,
            left_on="movies_en",
            right_on="filmes",
            how="outer",
            suffixes=(f"_omdb_en{i}", f"_en{i}")
        )

    return df_final
    


def replace_zeros(row):
    if row == 0:
        return np.nan
    return row



def replace_duplicate_imdb_notes(x, y):
    if pd.notna(x) and pd.notna(y):
        return np.nan
    return y



def where_is_now(row):
    if row == 'N/A':
        return 'Cinema'
    return 'Streaming'



def filter_films(df_final,n_count,filter_columns):    

    df_filtro = df_final[filter_columns]          

    df_filtro['NaN_count'] = df_filtro.isna().sum(axis=1)

    filmes_duplicados = df_filtro[df_filtro['NaN_count']<n_count]

    filmes_duplicados = filmes_duplicados.drop_duplicates(subset=['nome_filme', 'movie_original', 'nome_filmes_en'])    

    return filmes_duplicados


@task(retries=5, retry_delay_seconds=15)
def weekly_filter(df):
    data_inicio = datetime.today() - timedelta(days=17)
    data_ontem = data_inicio.strftime('%Y-%m-%d')
    df = df[df['data_lancamento_omdb']>data_ontem]
    return df



def convert_columns(filmes_atuais,columns):

    columns_to_convert = columns

    for col in columns_to_convert:             
        filmes_atuais[col] = (
                                filmes_atuais[col]
                                .astype(str)
                                .str.replace(',', '.')  
                                .apply(lambda x: x if x.replace('.', '', 1).isdigit() else None)                                  
                            )

        filmes_atuais[col] = filmes_atuais[col].astype(float)       

    return filmes_atuais



def scale_columns(filmes_atuais,columns_to_multiply,columns_to_divide):

    for col in columns_to_multiply:
        filmes_atuais[col] = filmes_atuais[col] * 2
    
    for col in columns_to_divide:
        filmes_atuais[col] = filmes_atuais[col] / 10

    return filmes_atuais



def get_median(filmes_atuais,columns_to_score):
    filmes_atuais['nota_score'] = filmes_atuais[columns_to_score].apply(lambda row: row.dropna().median(), axis=1)
    return filmes_atuais



@task(retries=5, retry_delay_seconds=15)
def filter_processing_final_df(
                            df_final,
                            n_count,
                            filter_columns,
                            columns_to_convert,
                            columns_to_multiply,
                            columns_to_divide,
                            columns_to_score
                            ):

    filmes_atuais = filter_films(df_final,n_count,filter_columns)    

    filmes_atuais = convert_columns(filmes_atuais,columns_to_convert)    

    filmes_atuais['nota_omdb'] = filmes_atuais['nota_omdb'].apply(replace_zeros)

    filmes_atuais['nota_imdb_en0'] = filmes_atuais.apply(lambda row: 
                             replace_duplicate_imdb_notes(row['nota_imdb_omdb_en0'],
                                                          row['nota_imdb_en0']),
                             axis=1)

    filmes_atuais['nota_rottentomatoes'] = filmes_atuais['nota_rottentomatoes'].str.rstrip('%').astype(float)    

    filmes_atuais =  scale_columns(filmes_atuais,columns_to_multiply,columns_to_divide)    

    filmes = get_median(filmes_atuais,columns_to_score)

    filmes = filmes_atuais.sort_values(by='nota_score', ascending=False)

    return filmes



@task(retries=5, retry_delay_seconds=15)
def print_film(new_films):
    print(new_films)



@task(retries=5, retry_delay_seconds=15)
def get_imdb_name(df_imdb):
    df_imdb["filmes"] = df_imdb["filmes"].str.replace(r"^\d+\.\s*", "", regex=True)
    return df_imdb

@task(retries=5, retry_delay_seconds=15)
def concat_rt(df_rt,df_rt_cinema):
    df_rt_final = pd.concat([df_rt,df_rt_cinema])
    df_rt_final['nota_rottentomatoes'] = df_rt_final['nota_rottentomatoes'].replace('',np.nan)
    return df_rt_final

@task(retries=5, retry_delay_seconds=15)
def replace_rt(df_rt):
    df_rt['nota_rottentomatoes'] = df_rt['nota_rottentomatoes'].replace('',np.nan)
    return df_rt


@task(retries=5, retry_delay_seconds=15)
def concat_filmow(df_filmow,df_filmow_streaming):
    df_filmow_final = pd.concat([df_filmow,df_filmow_streaming])
    return df_filmow_final


@task(retries=5, retry_delay_seconds=15)
def merge_new_movies(filmes,movies_df):
    new_filmes = pd.merge(
        filmes,
        movies_df[['movie_id', 'streaming', 'studio']],
        on="movie_id",        
        how="inner"
    )

    new_filmes['film_type'] = new_filmes['streaming'].apply(where_is_now)

    return new_filmes
    
@task(retries=5, retry_delay_seconds=15)
def printar_filmes(df_final):
    df_columns = df_final.columns
    print(df_columns)
