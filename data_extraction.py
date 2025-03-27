import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import unicodedata
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from unidecode import unidecode
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from datetime import datetime, timedelta
import time
from prefect.cache_policies import NO_CACHE
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import random
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
import urllib.parse
import json
import pymssql
import chromedriver_autoinstaller
from prefect import flow, task
import warnings
warnings.simplefilter(action='ignore')


@task(retries=5, retry_delay_seconds=15)
def fetch_imdb_rating(movies, omdb_api_key):    
    
    data = []

    for movie in movies:
        url = f"http://www.omdbapi.com/?apikey={omdb_api_key}&t={movie}"
        response = requests.get(url)
        
        if response.status_code == 200:
            response_data = response.json()
            title = response_data.get('Title', 'vazio')
            imdb_rating = response_data.get('imdbRating', 'vazio')
            if imdb_rating not in ['vazio', 'N/A', None, '']:
                data.append({"filmes": title, "nota_imdb": imdb_rating})          
    
    df = pd.DataFrame(data)    
    return df


@task(retries=5, retry_delay_seconds=15)
def extract_movies_data(url):   
    
    response = requests.get(url)

    
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a página: {response.status_code}")

    
    soup = BeautifulSoup(response.text, 'html.parser')

    
    movies_data = []

    
    movies = soup.find_all('div', class_='flex-container')

    
    for movie in movies:
        movie_data = {}

        
        title_tag = movie.find('span', {'data-qa': 'discovery-media-list-item-title'})
        movie_data['filmes'] = title_tag.text.strip() if title_tag else 'N/A'

        
        date_tag = movie.find('span', {'data-qa': 'discovery-media-list-item-start-date'})
        movie_data['streaming_date'] = date_tag.text.strip() if date_tag else 'N/A'

        
        is_video = movie.find('tile-dynamic', {'isvideo': 'true'})
        movie_data['has_trailer'] = True if is_video else False

        
        critics_score_tag = movie.find('rt-text', {'slot': 'criticsScore'})
        movie_data['critics_score'] = critics_score_tag.text.strip() if critics_score_tag else 'N/A'

        
        audience_score_tag = movie.find('rt-text', {'slot': 'audienceScore'})
        movie_data['nota_rottentomatoes'] = audience_score_tag.text.strip() if audience_score_tag else 'N/A'

        
        movies_data.append(movie_data)

    
    df = pd.DataFrame(movies_data)
    return df




@task(retries=5, retry_delay_seconds=15)
def extrair_filmes_e_notas(url,num_pages):
    
    try:
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        
        nomes_filmes = []
        notas_filmes = []

        
        for page in range(1,num_pages):
            url_din = f"{url}&pagina={page}"
            response = requests.get(url_din, headers=headers)
            response.raise_for_status()

            
            soup = BeautifulSoup(response.text, 'html.parser')

            
            filmes_blocos = soup.find_all('a', class_='cover')            

            
            for bloco in filmes_blocos:
                
                nome_filme = bloco.get('title')
                
                nota_element = bloco.find_next('span', class_='movie-rating-average')

                if nota_element:
                    nota = nota_element.get_text(strip=True)
                else:
                    nota = "Sem nota"

                if nome_filme:  
                    nomes_filmes.append(nome_filme)
                    notas_filmes.append(nota)            
        
        
        df = pd.DataFrame({
            'filmes': nomes_filmes,
            'nota_filmow': notas_filmes
        })
        
        
        df = df.drop_duplicates()
        
        return df
    
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return pd.DataFrame(columns=['filmes', 'nota_filmow'])





@task(retries=5, retry_delay_seconds=15)
def extrair_dados_adorocinema(base_url, num_paginas=16):    
    
    nomes_filmes = []
    datas_lancamento = []
    notas_usuarios = []

    try:
        for page in range(1, num_paginas + 1):
            
            url = f"{base_url}?page={page}"
            response = requests.get(url)
            response.raise_for_status()

            
            soup = BeautifulSoup(response.text, 'html.parser')

            
            filmes_blocos = soup.find_all('div', class_='card entity-card entity-card-list cf')

            for bloco in filmes_blocos:
                
                nome_element = bloco.find('h2', class_='meta-title')
                nome_filme = nome_element.get_text(strip=True) if nome_element else "Sem título"

                
                data_element = bloco.find('span', class_='date')
                data_lancamento = data_element.get_text(strip=True) if data_element else "Data não encontrada"
                
                notas_usuario = []

                
                rating_items = bloco.find_all('div', class_='rating-item')
                
                for item in rating_items:
                    rating_title = item.find('span', class_='rating-title')
                    if rating_title and 'Usuários' in rating_title.get_text(strip=True):
                        
                        nota_element = item.find('span', class_='stareval-note')
                        if nota_element:
                            notas_usuario.append(nota_element.get_text(strip=True))

                
                nota_usuario = ', '.join(notas_usuario) if notas_usuario else "Sem nota"
                                   

                
                nomes_filmes.append(nome_filme)
                datas_lancamento.append(data_lancamento)
                notas_usuarios.append(nota_usuario)

        
        df = pd.DataFrame({
            'filmes': nomes_filmes,
            'data_lancamento': datas_lancamento,
            'nota_adorocinema': notas_usuarios
        })

        return df

    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return pd.DataFrame(columns=['Nome do Filme', 'Data de Lançamento', 'Nota dos Usuários'])




@task(retries=5, retry_delay_seconds=15)
def extrair_filmes_letterboxd():
    
    data = []
    
    try:
        for page in range(1, 15):
            
            if page == 1:
                url_page = 'https://letterboxd.com/films/ajax/popular/this/week/?esiAllowFilters=true'
            else:
                url_page = f'https://letterboxd.com/films/ajax/popular/this/week/page/{page}/?esiAllowFilters=true'
            
            
            response = requests.get(url_page)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            
            for elemento in soup.select('li.listitem'):
                filme = {
                    'filmes': elemento.img.get('alt'),                    
                    'nota_letterbox': elemento.get('data-average-rating')
                    
                }
                data.append(filme)
            
            
            time.sleep(2)
    
    except Exception as e:
        print(f"Erro durante a extração: {str(e)}")
        raise
    
    
    df = pd.DataFrame(data)
    return df

    


def slugify(title):    
    title = re.sub(r'[:;,.]', '', title.lower())  
    title = re.sub(r'[\s_-]+', '-', title)  
    return title.strip('-')


@task(retries=5, retry_delay_seconds=15)
def extrair_dados_trakt(url,movie_slugs, client_id):
    url_base = url
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id
    }

    movie_details = []

    for _,row in movie_slugs.iterrows():
        slug = slugify(row["movie_original"])
        url = f"{url_base}{slug}?extended=full"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            movie_data = response.json()
            movie_details.append({
                "filmes": movie_data["title"],
                "nota_trakt": movie_data.get("rating", "N/A"),
                "genres": ", ".join(movie_data.get("genres", "N/A")),
                "certification": movie_data.get("certification", "N/A"),
                "streaming_trakt": movie_data.get("network", "N/A")
            })
        else:
            pass
    
    
    df_movies = pd.DataFrame(movie_details)
    return df_movies




@task(retries=5, retry_delay_seconds=15)
def discover_movies(start_date, end_date,language,API_KEY,base_url,end_point):    


    discover_url = f"{base_url}/{end_point}"
    params = {
        "api_key": API_KEY,
        "primary_release_date.gte": start_date,
        "primary_release_date.lte": end_date,
        "language": language
    }
    response = requests.get(discover_url, params=params)
    if response.status_code == 200:
        movies = response.json()
        
        if movies and movies['results']:
            movie_data = [{
                "movie_id": movie.get('id'),
                "movies": movie.get('title'),
                "movie_original": movie.get('original_title'),
                "nota_omdb": movie.get('vote_average'),
                "data_lancamento": movie.get('release_date'),                
                "poster": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}" if movie.get('poster_path') else None
            } for movie in movies['results']]
            
            return pd.DataFrame(movie_data)
        else:
            print("Nenhum filme encontrado.")
            return pd.DataFrame()  
    else:
        print(f"Erro: {response.status_code}, {response.json()}")
        return pd.DataFrame()  




@task(retries=5, retry_delay_seconds=15)
def now_playing_movies(language,API_KEY,base_url,end_point,name,original_name, region="BR", max_pages=16):
    
    now_playing_url = f"{base_url}/{end_point}"
    all_movies = []
    
    for page in range(1, max_pages + 1):  
        params = {
            "api_key": API_KEY,            
            "language": language,
            "region": region,
            "page": page,  
        }
        response = requests.get(now_playing_url, params=params)
        
        if response.status_code == 200:
            movies = response.json()
            
            
            if movies and movies['results']:
                all_movies.extend([{
                    "movie_id": movie.get('id'),
                    "movies": movie.get(name),
                    "movie_original": movie.get(original_name),
                    "nota_omdb": movie.get('vote_average'),
                    "data_lancamento": movie.get('release_date'),                    
                    "poster": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}" if movie.get('poster_path') else None
                } for movie in movies['results']])
            else:
                break  
        else:
            print(f"Erro: {response.status_code}, {response.json()}")
            break
    
    
    return pd.DataFrame(all_movies)


@task(retries=5, retry_delay_seconds=15)
def now_playing_series(language,start_date,end_date,API_KEY,base_url,end_point,name,original_name, region="BR", max_pages=16):
    
    now_playing_url = f"{base_url}/{end_point}"
    all_movies = []
    
    for page in range(1, max_pages + 1):  
        params = {
            "api_key": API_KEY,
            "air_date.gte": start_date,
            "air_date.lte": end_date,            
            "language": language,
            "watch_region": region,
            "page": page,  
        }
        response = requests.get(now_playing_url, params=params)
        
        if response.status_code == 200:
            movies = response.json()
            
            
            if movies and movies['results']:
                all_movies.extend([{
                    "movie_id": movie.get('id'),
                    "movies": movie.get(name),
                    "movie_original": movie.get(original_name),
                    "nota_omdb": movie.get('vote_average'),
                    "data_lancamento": movie.get('first_air_date'),                                      
                    "poster": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}" if movie.get('poster_path') else None
                } for movie in movies['results']])
            else:
                break  
        else:
            print(f"Erro: {response.status_code}, {response.json()}")
            break
    
    
    return pd.DataFrame(all_movies)


@task(retries=5, retry_delay_seconds=15)
def fetch_movie_details_by_name(movie_titles,API_KEY,base_url,type, language="pt-BR", region="BR"):
    all_details = []     

    for _, row in movie_titles.iterrows():
       
        original_title = row['movie_original']
        movie_title = row['nome_filme']
        movie_id = row['movie_id']
        
        movie_url = f"{base_url}/{type}/{movie_id}"
        movie_details_response = requests.get(movie_url, params={"api_key": API_KEY, "language": language})
        
        movie_details = movie_details_response.json()
        studios_details = movie_details.get("production_companies", [])
        studios = studios_details[0]['name'] if studios_details else "N/A"        
       
        
        streaming_url = f"{base_url}/{type}/{movie_id}/watch/providers"
        streaming_response = requests.get(streaming_url, params={"api_key": API_KEY})
        
        streaming_data = streaming_response.json()
        streaming_providers = streaming_data.get("results", {}).get(region, {}).get("flatrate", [])
        streaming_names = streaming_providers[0]['provider_name'] if streaming_providers else "N/A"
        
        all_details.append({
            "movie_id": movie_id,
            "nome_filme": movie_title,
            "movie_original": original_title,            
            "streaming": streaming_names,
            "studio": studios
        })
    
    return pd.DataFrame(all_details)





@task(retries=10, retry_delay_seconds=10,cache_policy=NO_CACHE)
def load_data(df, name_table, server, database, uid, pwd):

    data = datetime.today().strftime('%Y-%m-%d')
    df['data'] = data

    
    connection_string = f"mssql+pymssql://{uid}:{pwd}@{server}/{database}"
    
    
    engine = create_engine(connection_string)

    print("Conexão bem-sucedida!")

    if name_table == "Notas_Series":        
        df = df.drop('streaming_trakt', axis=1)
       

    column_mappings = {
        "Notas_Filmes": {
            "movie_id": "movie_id",
            "nome_filme": "nome_filme",
            "movie_original": "movie_original",
            "nome_filmes_en": "nome_filmes_en",
            "data_lancamento_omdb": "data_lancamento",
            "poster": "poster",
            "nota_omdb": "nota_omdb",
            "nota_imdb_omdb_en0": "nota_imdb_pt",
            "nota_imdb_en0": "nota_imdb_en",
            "nota_adorocinema": "nota_adorocinema",
            "nota_filmow": "nota_filmow",
            "nota_rottentomatoes": "nota_rottentomatoes",
            "nota_letterbox": "nota_letterbox",
            "nota_trakt": "nota_trakt",
            "NaN_count": "NaN_count",
            "nota_score": "nota_score",
            "streaming": "streaming",
            "studio": "studio",
            "film_type": "film_type",
            "data": "data"
        },
        "Notas_Series": {
            "movie_id": "serie_id",
            "nome_filme": "nome_serie",
            "movie_original": "serie_original",
            "nome_filmes_en": "nome_series_en",
            "data_lancamento_omdb": "data_lancamento",
            "poster": "poster",
            "nota_omdb": "nota_omdb",
            "nota_imdb_omdb_en0": "nota_imdb_pt",
            "nota_imdb_en0": "nota_imdb_en",
            "nota_adorocinema": "nota_adorocinema",
            "nota_filmow": "nota_filmow",
            "nota_rottentomatoes": "nota_rottentomatoes",
            "nota_trakt": "nota_trakt",
            "NaN_count": "NaN_count",
            "nota_score": "nota_score",
            "streaming": "streaming",
            "studio": "studio",
            "film_type": "serie_type",
            "data": "data"
        }
    }

    
    if name_table in column_mappings:        
        df = df.rename(columns=column_mappings[name_table])        

    
    df.to_sql(
        name=name_table,  
        con=engine,
        index=False,
        if_exists="append",  
        schema="dbo"
    )

    print(f"Dados inseridos na tabela {name_table} com sucesso! {df.shape[0]}")