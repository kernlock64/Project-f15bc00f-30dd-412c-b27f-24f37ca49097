import zipfile
import io
import csv
import uuid
from service import get_httpx
from fastapi import Response
import os

MEDIA_KEY = os.getenv('MEDIA_KEY')

PATH_TO_ID = {'avto-moto/zapchasti-auto': 11, 'avto-moto/shiny-i-diski/shiny': 121,
              'avto-moto/shiny-i-diski/diski': 122,
              'avto-moto/shiny-i-diski/kolesa-v-sbore': 123, 'avto-moto/shiny-i-diski/kolpaki': 124,
              'avto-moto/shiny-i-diski/prinadlezhnosti-dlya-koles/kamery': 1251,
              'avto-moto/shiny-i-diski/prinadlezhnosti-dlya-koles/metizy-kolesnye': 1252, 'avto-moto/auto-media': 16,
              'avto-moto/signalizacii': 17, 'avto-moto/avtokresla': 18, 'avto-moto/autoliteratura': 20,
              'avto-moto/moto/motozapchasti': 131, 'vodnaya-tekhnika/zapchasti-voda': 21,
              'cifrovaya-tekhnika/foto-video': 31,
              'cifrovaya-tekhnika/telefony-gadzhety': 32, 'cifrovaya-tekhnika/kompyutery/noutbuki': 331,
              'bytovaya-tekhnika/holodilniki': 41, 'bytovaya-tekhnika/stiralnye-mashiny': 46, 'stroyka/santehnika': 51,
              'stroyka/plitka': 54, 'stroyka/pilomaterialy/doska': 5111, 'stroyka/drevesnye-plity/fanera': 5121,
              'mebel/divany': 71,
              'mebel/kuhni': 74, 'odezhda-obuv/zhenskaya-odezhda': 91,
              'odezhda-obuv/zhenskaya-odezhda/dlya-beremennyh': 911,
              'odezhda-obuv/detskaya-odezhda': 93, 'zhivotnye/dlya-koshek/korm-koshki': 1421,
              'deti/kolyaski/kolyaski-3-v-1': 1911}


async def initial_proceed(filename):
    try:
        posts = []
        response = {}
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open('init.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    metadata = {
                        'price-list-name': row['price-list-name'],
                        'author_id': row['author_id'],
                        'is_images': row['is-images'],
                    }

                response["metadata"] = metadata
                response["posts"] = posts

                if row['is-images'] == 't':
                    return await with_images_proceed(filename, posts, response)

                if row['is-images'] == 'f':
                    return await without_images_proceed(filename, posts, response)

                else:
                    return {
                        "status": "False",
                        "message": "Error parsing!"
                    }

    except Exception as e:
        return {
            "status": "False",
            "message": "Something went wrong!"
        }


async def with_images_proceed(
        filename,
        posts,
        response
):
    try:
        images = []
        client = await get_httpx()
        images_dir = str(uuid.uuid4())
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open('posts.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    post = {
                        'title': row['title'],
                        'description': row['description'],
                        'coordinates': [float(row['coordinates'].split(',')[0]),
                                        float(row['coordinates'].split(',')[1])],
                        'category_path': PATH_TO_ID[row['category_path']],
                        'images_dir': images_dir,
                        'images': images,
                        'price': row['price'],
                        'attributes': row['attributes'],

                    }

                    imgs = row['images'].split(',')

                    if not row['title_image'] in imgs:
                        return {
                            "status": "False",
                            "message": "Something went wrong!"
                        }

                    for m in imgs:
                        with z.open(f'imgs/{m}', "r") as k:

                            name = str(uuid.uuid4()) + '.png'

                            if str(m) == row['title_image']:
                                post["title_image"] = name

                            async def read_file():
                                while chunk := k.read(64 * 1024):
                                    yield chunk

                            req = client.build_request(
                                method="POST",
                                url=f"http://media-service:5001/api/v1/media/add-image/png?p_directory={images_dir}",
                                headers={"X-Key": MEDIA_KEY, "X-Name": name},
                                content=read_file(),
                            )

                            resp = await client.send(req, stream=True)

                            images.append(name)

                            if resp.status_code != 200:
                                return {
                                    "status": "False",
                                    "message": "Error adding image",
                                }

                posts.append(post)

        return response

    except Exception as e:
        return {
            "status": "False",
            "message": str(e)
        }


async def without_images_proceed(filename, posts, response):
    try:
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open('posts.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    post = {
                        'title': row['title'],
                        'description': row['description'],
                        'coordinates': [float(row['coordinates'].split(',')[0]),
                                        float(row['coordinates'].split(',')[1])],
                        'category_path': PATH_TO_ID[row['category_path']],
                        'price': row['price'],
                        'attributes': row['attributes'],
                    }
                posts.append(post)

        return response

    except Exception:
        return {
            "status": "False",
            "message": "Something went wrong!"
        }