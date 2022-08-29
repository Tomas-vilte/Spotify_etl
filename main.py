import requests
import sqlalchemy
from datetime import *
import pandas as pd
import psycopg2

DATABASE = "postgresql://postgres:postgres@localhost/spotify_etl"
USER_ID = 'Lord chaqueño ll'  # Tu usuario de spotify
TOKEN = 'BQA8rmqyVys4l6qh869XIpUwwY_Ua5Q0kMuQ6APGf7XPxZIUL2X1MvAviPzkxwmDQOYft2ROUhiQphftcDO1yfcmLCbzovciDNUmcV_fCVKwvhdlBGyFC0hDOrztPNtiC0lwd8AN40qW1GaWO_twsdxKaXptWx7YDnlzB2n94f0dN1Ti12ZctHN2JiZdsNR2KyF7GXXf'

def comprobar_datos(df: pd.DataFrame) -> bool:
    # Comprobrar si el DataFrame esta vacio
    if df.empty:
        print('No hay musicas descargadas. Termino la ejecuccion')
        return False

    # Chequea primary key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Chequeo invalido')

    # Chequea si hay nulos
    if df.isnull().values.any():
        raise Exception('Valor nulo encontrado')

    # Chequea que todas las marcas de tiempo sean de la fecha de ayer
    ayer = datetime.now() - timedelta(days=1)
    ayer = ayer.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.strptime(timestamp, '%Y-%m-%d') != ayer:
            raise Exception('A más tardar una de las canciones devueltas no proviene de las últimas 24 horas')
    return True


if __name__ == '__main__':
    # Extraer parte del proceso ETL
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }
    # Convertimos el tiempo a la marca de tiempo de Unix en milisegundos
    Hoy = datetime.now()
    Ayer = Hoy - timedelta(days=1)
    yesterday_unix_timestamp = int(Ayer.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp),headers=headers)

    data = r.json()

    song_name = []
    artist_name = []
    played_at_list = []
    timestamp = []

    for song in data['items']:
        song_name.append(song['track']['name'])
        artist_name.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamp.append(song['played_at'][0:10])

    song_dict = {
        'Cancion': song_name,
        'Artista': artist_name,
        'played_at': played_at_list,
        'timestamp': timestamp
    }

    song_df = pd.DataFrame(song_dict, columns=['Cancion', 'Artista', 'played_at', 'timestamp'])

    # Valida
    if comprobar_datos(song_df):
        print('Datos válidos, procede a la etapa de carga')

    # Carga
    engine = sqlalchemy.create_engine(DATABASE)
    conn = psycopg2.connect(DATABASE)
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS mis_reproduciones(
        Cancion VARCHAR(200),
        Artista VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(query)

    try:
        song_df.to_sql('mis_reproducciones', engine, index=False, if_exists='append')
    except:
        print('Los datos ya existen en la base de datos')

    conn.close()
    print('Base de datos cerrada con exito')
    print(song_df)
