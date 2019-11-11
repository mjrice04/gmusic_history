#Techincal Challenges:
#Making sure I was using the right client
#Manually having to add thumbs up to libary
from gmusicapi import Mobileclient
import pandas as pd
import numpy as np
import requests

#device_id = '357536084965542'
#print(device_id)

def get_url(url_json):
    print(url_json)
    if url_json is np.nan:
        print('nan')
        url_column = np.nan
    else:
        print(url_json[0]['url'])
        url_column = url_json[0]['url']
    return url_column



mc = Mobileclient()
mc.oauth_login(Mobileclient.FROM_MAC_ADDRESS)  # currently named oauth_login for the Mobileclient

mc.is_authenticated()

print(mc.is_subscribed)

a = mc.get_registered_devices()


songs = mc.get_all_songs()
df_songs = pd.DataFrame(songs)

print(df_songs)

##TO-DO -- get the album artwork for the analysis you need



df_songs['albumarturl'] = df_songs['albumArtRef'].apply(get_url)
df_songs['artistarturl'] = df_songs['artistArtRef'].apply(get_url)

df_songs.to_csv('/home/matt/projects/google_music_analytics/data/song_library.csv')



#img_data = requests.get(image).content
#with open('test_image.jpg', 'wb') as handler:
#    handler.write(img_data)


