import pandas as pd
import numpy as np
import random
import base64
import requests
from PIL import Image
from io import BytesIO
from IPython.display import HTML
pd.set_option('display.max_colwidth', -1)


# Image handling

def get_image(df_url):
	a_id = df_url['albumId']
	print(a_id)
	image_id = df_url['albumarturl']
	print(image_id)
	img_data = requests.get(image_id).content
	with open(f"/home/matt/projects/google_music_analytics/data/images/{a_id}.jpg", 'wb') as handler:
		handler.write(img_data)


def get_thumbnail(path):
	i = Image.open(path)
	i.thumbnail((150, 150), Image.LANCZOS)
	return i


def image_base64(im):
	if isinstance(im, str):
		im = get_thumbnail(im)
	with BytesIO() as buffer:
		im.save(buffer, 'jpeg')
		return base64.b64encode(buffer.getvalue()).decode()


def image_formatter(im):
	return f'<img src="data:image/jpeg;base64,{image_base64(im)}">'