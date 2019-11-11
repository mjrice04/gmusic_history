import pandas as pd
from image_handler import get_image


def file_storage():
    pass


def get_top_songs(songs, year=None, years=None):
    """
    returns Dataframe with top songs. year can be passed in to get songs from a year.
    years can be passed in to get a range of years
    :param songs: songs dataframe
    :param year: year to get top songs for
    :param years: range of years to get top songs for
    :return:
    """
    if year:
        song_subset = songs[['title', 'year', 'artist', 'albumId', 'playCount']]
        song_subset_year = song_subset.loc[song_subset['year'] == year]  # place year here
        top_songs = song_subset_year.sort_values(by=['playCount'], ascending=False)
        ts = top_songs.head(25)
    elif years:
        year_start = years[0]
        year_end = years[1]
        song_subset = songs[['title', 'year', 'artist', 'albumId', 'playCount']]
        song_subset_year = song_subset.loc[(song_subset['year'] >= year_start) & (song_subset['year'] <= year_end)]
        top_songs = song_subset_year.sort_values(by=['playCount'], ascending=False)
        ts = top_songs.head(25)
    else:
        song_subset = songs[['title', 'artist', 'playCount', 'album', 'albumId']]
        top_songs = song_subset.sort_values(by=['playCount'], ascending=False)
        ts = top_songs.head(50)
    return ts


def get_top_albums(albums, year=None, years=None):
    """
    returns Dataframe with top albums. year can be passed in to get albums from a year.
    years can be passed in to get a range of years
    :param albums: albums dataframe
    :param year: year to get top albums for
    :param years: range of years to get top albums for
    :return:
    """
    if year:
        album_subset = albums[['album', 'artist', 'playCount', 'albumId', 'year']]
        album_subset_year = album_subset.loc[album_subset['year'] == year]
        clean_album_subset = album_subset_year[['album', 'artist', 'playCount', 'albumId']]
        grouped_subset = clean_album_subset.groupby(['album', 'artist' 'albumId'], as_index=False)[['playCount']].sum()
        top_albums_2019 = grouped_subset.sort_values(by=['playCount'], ascending=False)
        ta = top_albums_2019.head(25)
    elif years:
        year_start = years[0]
        year_end = years[1]
        album_subset = albums[['album', 'artist', 'playCount', 'albumId', 'year']]
        album_subset_year = album_subset.loc[(album_subset['year'] >= year_start) & (album_subset['year'] <= year_end)]
        clean_album_subset = album_subset_year[['album', 'artist', 'playCount', 'albumId']]
        grouped_subset = clean_album_subset.groupby(['album', 'artist' 'albumId'], as_index=False)[['playCount']].sum()
        top_albums_2019 = grouped_subset.sort_values(by=['playCount'], ascending=False)
        ta = top_albums_2019.head(25)
    else:
        album_subset = albums[['album', 'artist', 'playCount', 'albumId']]
        grouped_subset = album_subset.groupby(['album', 'artist' 'albumId'], as_index=False)[['playCount']].sum()
        top_albums = grouped_subset.sort_values(by=['playCount'], ascending=False)
        ta = top_albums.head(50)
    return ta


def get_top_artist(artists):
    song_subset = artists[['artist', 'artistId', 'playCount']]
    grouped_subset = song_subset.groupby(['artist', 'artistId'])[['playCount']].sum()
    top_artists = grouped_subset.sort_values(by=['playCount'], ascending=False)
    ta = top_artists.head(50)
    return ta


def merge_in_id(df1, df2, col):
    df = pd.merge(df1, df2, how='inner', on=col)
    return df



# ETL for data

# Reading data
df = pd.read_csv('/home/matt/projects/google_music_analytics/data/song_library.csv')

# Data Cleaning

# Song set
cols = ['playCount', 'artist', 'genre', 'title', 'year', 'album', 'durationMillis','totalTrackCount', 'albumId',
        'artistId']
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

