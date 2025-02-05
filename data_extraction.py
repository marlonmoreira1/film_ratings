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




def get_driver():    
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")    
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)    
    return driver

@task
def scrape_imdb_titles_selenium(url):
    """
    Scrape all movie titles, ratings, and vote counts from IMDb using Selenium to handle dynamic content.
    """
    
    driver = get_driver()

    titles = []
    ratings = []
    votes = []

    try:
        # Carregar a página
        driver.get(url)
        
         # Lista para armazenar os votos
        contador = 0
        while contador < 16:
            # Esperar pelos elementos carregarem
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'h3.ipc-title__text'))
            )
            
            movie_containers = driver.find_elements(By.CSS_SELECTOR, 'div.sc-e2db8066-1.QxXCO')

            # Inicializar listas para armazenar os dados            

            # Iterar pelos containers de filmes
            for container in movie_containers:
                # Tentar pegar o título do filme
                try:
                    title = container.find_element(By.CSS_SELECTOR, 'h3.ipc-title__text').text
                except:
                    title = None  # Se não encontrar o título, atribuir None

                # Tentar pegar a nota do filme
                try:
                    rating = container.find_element(By.CSS_SELECTOR, 'span.ipc-rating-star--rating').text
                except:
                    rating = None  # Se não encontrar a nota, atribuir None

                # Tentar pegar o número de votos
                try:
                    votes_count = container.find_element(By.CSS_SELECTOR, 'span.ipc-rating-star--voteCount').text
                except:
                    votes_count = None  # Se não encontrar os votos, atribuir None

                # Adicionar os dados nas listas
                titles.append(title)
                ratings.append(rating)
                votes.append(votes_count)
            

            
            try:
                # Procurar e clicar no botão "Mais 50"
                more_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.ipc-see-more__text'))
                )
                driver.execute_script("arguments[0].click();", more_button)
                time.sleep(2)  # Aguardar o carregamento do conteúdo
                contador += 1
            except:
                # Se não encontrar o botão, assumimos que chegamos ao fim
                break
                
    except Exception as e:
        print(f"Erro durante o scraping: {str(e)}")
    
    finally:
        driver.quit()
    
    #Retornar os dados como um DataFrame
    return pd.DataFrame({'filmes': titles, 'nota_imdb': ratings, 'votes': votes})


@task
def extract_movies_data(url):
    """
    Raspa dados de filmes do Rotten Tomatoes e retorna um DataFrame do pandas.

    Args:
        url (str): URL da página do Rotten Tomatoes.

    Returns:
        pd.DataFrame: Um DataFrame com os dados extraídos.
    """
    # Fazer uma requisição HTTP para obter o conteúdo HTML
    response = requests.get(url)

    # Verificar se a requisição foi bem-sucedida
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a página: {response.status_code}")

    # Parsear o HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Lista para armazenar os dados extraídos
    movies_data = []

    # Encontrar todos os containers de filmes
    movies = soup.find_all('div', class_='flex-container')

    # Iterar sobre cada filme e extrair informações
    for movie in movies:
        movie_data = {}

        # Título do filme
        title_tag = movie.find('span', {'data-qa': 'discovery-media-list-item-title'})
        movie_data['filmes'] = title_tag.text.strip() if title_tag else 'N/A'

        # Data de streaming
        date_tag = movie.find('span', {'data-qa': 'discovery-media-list-item-start-date'})
        movie_data['streaming_date'] = date_tag.text.strip() if date_tag else 'N/A'

        # Trailer disponível?
        is_video = movie.find('tile-dynamic', {'isvideo': 'true'})
        movie_data['has_trailer'] = True if is_video else False

        # Nota dos críticos
        critics_score_tag = movie.find('rt-text', {'slot': 'criticsScore'})
        movie_data['critics_score'] = critics_score_tag.text.strip() if critics_score_tag else 'N/A'

        # Nota da audiência
        audience_score_tag = movie.find('rt-text', {'slot': 'audienceScore'})
        movie_data['nota_rottentomatoes'] = audience_score_tag.text.strip() if audience_score_tag else 'N/A'

        # Adicionar os dados do filme à lista
        movies_data.append(movie_data)

    # Converter para DataFrame do pandas
    df = pd.DataFrame(movies_data)
    return df

@task
def extrair_filmes_e_notas(url,num_pages):
    """
    Extrai o nome de todos os filmes e suas respectivas notas do Filmow.
    
    Parâmetros:
        url (str): URL da página do Filmow contendo a lista de filmes.
        
    Retorna:
        pandas.DataFrame: DataFrame contendo os nomes dos filmes e as notas.
    """
    try:
        # Configurar headers para simular um navegador
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Listas para armazenar os dados
        nomes_filmes = []
        notas_filmes = []

        # Fazer a requisição HTTP para a URL
        for page in range(1,num_pages):
            url_din = f"{url}&pagina={page}"
            response = requests.get(url_din, headers=headers)
            response.raise_for_status()

            # Analisar o conteúdo HTML com BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Localizar todos os elementos de filme usando a classe cover
            filmes_blocos = soup.find_all('a', class_='cover')            

            # Iterar por cada bloco de filme e extrair dados
            for bloco in filmes_blocos:
                # Nome do filme está no atributo title
                nome_filme = bloco.get('title')

                # A nota está em um elemento próximo com valor numérico
                # Procurar no elemento pai ou irmão
                nota_element = bloco.find_next('span', class_='movie-rating-average')

                if nota_element:
                    nota = nota_element.get_text(strip=True)
                else:
                    nota = "Sem nota"

                if nome_filme:  # Só adiciona se encontrou um nome válido
                    nomes_filmes.append(nome_filme)
                    notas_filmes.append(nota)
            
        
        # Criar um DataFrame com os dados extraídos
        df = pd.DataFrame({
            'filmes': nomes_filmes,
            'nota_filmow': notas_filmes
        })
        
        # Remover possíveis duplicatas
        df = df.drop_duplicates()
        
        return df
    
    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return pd.DataFrame(columns=['filmes', 'nota_filmow'])


@task
def extrair_dados_adorocinema(base_url, num_paginas=16):
    """
    Extrai informações dos filmes do AdoroCinema em várias páginas.
    
    Parâmetros:
        base_url (str): URL base da página do AdoroCinema contendo os filmes.
        num_paginas (int): Número de páginas para percorrer. Default é 10.
        
    Retorna:
        pandas.DataFrame: DataFrame contendo nome do filme, data de lançamento e nota de usuários.
    """
    # Listas para armazenar os dados
    nomes_filmes = []
    datas_lancamento = []
    notas_usuarios = []

    try:
        for page in range(1, num_paginas + 1):
            # Monta a URL da página atual
            url = f"{base_url}?page={page}"
            response = requests.get(url)
            response.raise_for_status()

            # Analisar o HTML da página
            soup = BeautifulSoup(response.text, 'html.parser')

            # Localizar todos os blocos de filmes
            filmes_blocos = soup.find_all('div', class_='card entity-card entity-card-list cf')

            for bloco in filmes_blocos:
                # Nome do filme
                nome_element = bloco.find('h2', class_='meta-title')
                nome_filme = nome_element.get_text(strip=True) if nome_element else "Sem título"

                # Data de lançamento
                data_element = bloco.find('span', class_='date')
                data_lancamento = data_element.get_text(strip=True) if data_element else "Data não encontrada"
                
                notas_usuario = []

                # Nota de usuários
                rating_items = bloco.find_all('div', class_='rating-item')
                
                for item in rating_items:
                    rating_title = item.find('span', class_='rating-title')
                    if rating_title and 'Usuários' in rating_title.get_text(strip=True):
                        # Encontrar a nota do usuário
                        nota_element = item.find('span', class_='stareval-note')
                        if nota_element:
                            notas_usuario.append(nota_element.get_text(strip=True))

                # Se houver notas de usuários, pega a primeira; se não, deixa "Sem nota"
                nota_usuario = ', '.join(notas_usuario) if notas_usuario else "Sem nota"
                                   

                # Adicionar aos dados
                nomes_filmes.append(nome_filme)
                datas_lancamento.append(data_lancamento)
                notas_usuarios.append(nota_usuario)

        # Criar um DataFrame com os dados extraídos
        df = pd.DataFrame({
            'filmes': nomes_filmes,
            'data_lancamento': datas_lancamento,
            'nota_adorocinema': notas_usuarios
        })

        return df

    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return pd.DataFrame(columns=['Nome do Filme', 'Data de Lançamento', 'Nota dos Usuários'])

@task
def extrair_filmes_letterboxd():
    """
    Extrai os nomes dos filmes e suas respectivas notas da página do Letterboxd.
    
    Parâmetros:
        url (str): URL da página do Letterboxd.
        
    Retorna:
        pandas.DataFrame: DataFrame contendo informações dos filmes.
    """
    data = []
    
    try:
        for page in range(1, 15):
            # Construir URL com paginação
            if page == 1:
                url_page = 'https://letterboxd.com/films/ajax/popular/this/week/?esiAllowFilters=true'
            else:
                url_page = f'https://letterboxd.com/films/ajax/popular/this/week/page/{page}/?esiAllowFilters=true'
            
            # Fazer a requisição
            response = requests.get(url_page)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extrair dados
            for elemento in soup.select('li.listitem'):
                filme = {
                    'filmes': elemento.img.get('alt'),                    
                    'nota_letterbox': elemento.get('data-average-rating')
                    
                }
                data.append(filme)
            
            # Pequena pausa entre requisições
            time.sleep(2)
    
    except Exception as e:
        print(f"Erro durante a extração: {str(e)}")
        raise
    
    # Criar DataFrame
    df = pd.DataFrame(data)
    return df

    



def slugify(title):
    # Substitui caracteres especiais por um hífen e converte para minúsculas
    title = re.sub(r'[:;,.]', '', title.lower())  # Remove caracteres não alfanuméricos
    title = re.sub(r'[\s_-]+', '-', title)  # Substitui espaços e underlines por hífens
    return title.strip('-')


@task
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
                "certification": movie_data.get("certification", "N/A")
            })
        else:
            pass
    
    # Converte a lista de filmes em um DataFrame
    df_movies = pd.DataFrame(movie_details)
    return df_movies



# Função para buscar filmes lançados entre duas datas e retornar um DataFrame
@task
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
        # Processando os dados e retornando como DataFrame
        if movies and movies['results']:
            movie_data = [{
                "movie_id": movie.get('id'),
                "movies": movie.get('title'),
                "movie_original": movie.get('original_title'),
                "nota_omdb": movie.get('vote_average'),
                "data_lancamento": movie.get('release_date'),                
                "poster": f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}" if movie.get('poster_path') else None
            } for movie in movies['results']]
            # Criando e retornando o DataFrame
            return pd.DataFrame(movie_data)
        else:
            print("Nenhum filme encontrado.")
            return pd.DataFrame()  # Retorna um DataFrame vazio caso não haja resultados
    else:
        print(f"Erro: {response.status_code}, {response.json()}")
        return pd.DataFrame()  # Retorna um D


@task
def now_playing_movies(language,API_KEY,base_url,end_point,name,original_name, region="BR", max_pages=21):
    
    now_playing_url = f"{base_url}/{end_point}"
    all_movies = []
    
    for page in range(1, max_pages + 1):  # Iterar apenas pelas 3 primeiras páginas
        params = {
            "api_key": API_KEY,
            "language": language,
            "region": region,
            "page": page,  # Página atual
        }
        response = requests.get(now_playing_url, params=params)
        
        if response.status_code == 200:
            movies = response.json()
            
            # Adicionar os filmes retornados à lista
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
                break  # Interromper se não houver mais resultados
        else:
            print(f"Erro: {response.status_code}, {response.json()}")
            break
    
    # Criar DataFrame final
    return pd.DataFrame(all_movies)



@task
def fetch_movie_details_by_name(movie_titles,API_KEY,base_url,type, language="pt-BR", region="BR"):
    all_details = []
     

    for _, row in movie_titles.iterrows():
        # Endpoint para buscar o filme pelo nome
        original_title = row['movie_original']
        movie_title = row['nome_filme']
        movie_id = row['movie_id']
        
        movie_url = f"{base_url}/{type}/{movie_id}"
        movie_details_response = requests.get(movie_url, params={"api_key": API_KEY, "language": language})
        
        movie_details = movie_details_response.json()
        studios_details = movie_details.get("production_companies", [])
        studios = studios_details[0]['name'] if studios_details else "N/A"
        
       
        # Buscar as plataformas de streaming
        streaming_url = f"{base_url}/{type}/{movie_id}/watch/providers"
        streaming_response = requests.get(streaming_url, params={"api_key": API_KEY})
        
        streaming_data = streaming_response.json()
        streaming_providers = streaming_data.get("results", {}).get(region, {}).get("flatrate", [])
        streaming_names = streaming_providers[0]['provider_name'] if streaming_providers else "N/A"        

        # Adiciona todos os detalhes à lista
        all_details.append({
            "movie_id": movie_id,
            "nome_filme": movie_title,
            "movie_original": original_title,            
            "streaming": streaming_names,
            "studio": studios
        })
                

    # Criar DataFrame final com os resultados
    return pd.DataFrame(all_details)


@task(retries=10, retry_delay_seconds=10,cache_policy=NO_CACHE)
def load_data(df, name_table, server, database, uid, pwd):

    data = datetime.today().strftime('%Y-%m-%d')
    df['data'] = data

    # Criando a string de conexão para pymssql
    connection_string = f"mssql+pymssql://{uid}:{pwd}@{server}/{database}"
    
    # Criando o engine SQLAlchemy com pymssql
    engine = create_engine(connection_string)

    print("Conexão bem-sucedida!")      

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

    # Inserindo os dados no SQL Server
    df.to_sql(
        name=name_table,  
        con=engine,
        index=False,
        if_exists="append",  
        schema="dbo"
    )

    print(f"Dados inseridos na tabela {name_table} com sucesso! {df.shape[0]}")