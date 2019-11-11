import pandas as pd
from image_handler import get_image


def file_storage(df, name, year=None):
    if year:
        df.to_csv(f'/home/matt/projects/google_music_analytics/data/output/{name}-{year}.csv')
    else:
        df.to_csv(f'/home/matt/projects/google_music_analytics/data/output/{name}-alltime.csv')


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
        grouped_subset = clean_album_subset.groupby(['album', 'artist', 'albumId'], as_index=False)[['playCount']].sum()
        top_albums_2019 = grouped_subset.sort_values(by=['playCount'], ascending=False)
        ta = top_albums_2019.head(25)
    elif years:
        year_start = years[0]
        year_end = years[1]
        album_subset = albums[['album', 'artist', 'playCount', 'albumId', 'year']]
        album_subset_year = album_subset.loc[(album_subset['year'] >= year_start) & (album_subset['year'] <= year_end)]
        clean_album_subset = album_subset_year[['album', 'artist', 'playCount', 'albumId']]
        grouped_subset = clean_album_subset.groupby(['album', 'artist', 'albumId'], as_index=False)[['playCount']].sum()
        top_albums_2019 = grouped_subset.sort_values(by=['playCount'], ascending=False)
        ta = top_albums_2019.head(25)
    else:
        album_subset = albums[['album', 'artist', 'playCount', 'albumId']]
        grouped_subset = album_subset.groupby(['album', 'artist', 'albumId'], as_index=False)[['playCount']].sum()
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

top_songs = get_top_songs(songs)
file_storage(top_songs, 'songs')

top_albums = get_top_albums(songs)
file_storage(top_albums, 'albums')

top_artists = get_top_artist(songs)
file_storage(top_artists, 'artists')

year_list = [2019.0, 2018.0, 2017.0, 2016.0, 2015.0, 2014.0, 2013.0, 2012.0, 2011.0, 2010.0, [2000.0, 2009.0],
             [1990.0, 1999.0], [1980.0, 1989.0], [1970.0, 1979.0], [0.0, 1969.0]]

for year in year_list:
    print(year)
    if isinstance(year, list):
        top_songs = get_top_songs(songs, years=year)
        top_albums = get_top_albums(songs, years=year)
    else:
        top_songs = get_top_songs(songs, year=year)
        top_albums = get_top_albums(songs, year=year)
    formatted_top_songs = merge_in_id(top_songs, clean_album_art, 'albumId')
    formatted_top_albums = merge_in_id(top_albums, clean_album_art, 'albumId')
    top_songs_image = formatted_top_songs.apply(get_image, axis=1)
    top_albums_image = formatted_top_albums.apply(get_image, axis=1)
    file_storage(top_songs_image, 'songs', year)
    file_storage(top_albums_image, 'albums', year)