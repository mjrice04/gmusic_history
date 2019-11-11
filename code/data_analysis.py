import pandas as pd
import numpy as np
import random
import base64
import requests
from PIL import Image
from io import BytesIO
from IPython.display import HTML


def file_storage():


def top_songs(df, year=None):




def top_albums():


def top_artist():




# ETL for data

# Reading data
df = pd.read_csv('/home/matt/projects/google_music_analytics/data/song_library.csv')

# Data Cleaning

# Song set
cols = ['playCount', 'artist', 'genre', 'title', 'year', 'album', 'durationMillis',
        'totalTrackCount', 'albumId', 'artistId']
songs = df[cols].copy()
songs['duration'] = pd.to_timedelta(songs['durationMillis'], unit='ms')


# Album art
album_cols = ['albumId', 'albumarturl']
album_art = df[album_cols].copy()
clean_album_art = album_art.drop_duplicates()

# Artist art
artist_cols = ['artistId', 'artistarturl']
artist_art = df[artist_cols].copy()
clean_artist_art = artist_art.drop_duplicates()

