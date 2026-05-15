import zipfile
import io
import csv
import uuid
from service import get_httpx
from httpx import AsyncClient
import os

MEDIA_KEY = os.getenv('MEDIA_KEY')

posts = []
response = {}

PATH_TO_ID = {
              11: 'avto-moto/zapchasti-auto', 121: 'avto-moto/shiny-i-diski/shiny',
              122: 'avto-moto/shiny-i-diski/diski', 123: 'avto-moto/shiny-i-diski/kolesa-v-sbore',
              124: 'avto-moto/shiny-i-diski/kolpaki', 1251: 'avto-moto/shiny-i-diski/prinadlezhnosti-dlya-koles/kamery',
              1252: 'avto-moto/shiny-i-diski/prinadlezhnosti-dlya-koles/metizy-kolesnye', 16: 'avto-moto/auto-media',
              17: 'avto-moto/signalizacii', 18: 'avto-moto/avtokresla', 20: 'avto-moto/autoliteratura',
              131: 'avto-moto/moto/motozapchasti', 21: 'vodnaya-tekhnika/zapchasti-voda',
              31: 'cifrovaya-tekhnika/foto-video', 32: 'cifrovaya-tekhnika/telefony-gadzhety',
              331: 'cifrovaya-tekhnika/kompyutery/noutbuki', 41: 'bytovaya-tekhnika/holodilniki',
              46: 'bytovaya-tekhnika/stiralnye-mashiny', 51: 'stroyka/santehnika', 54: 'stroyka/plitka',
              5111: 'stroyka/pilomaterialy/doska', 5121: 'stroyka/drevesnye-plity/fanera', 71: 'mebel/divany',
              74: 'mebel/kuhni', 91: 'odezhda-obuv/zhenskaya-odezhda',
              911: 'odezhda-obuv/zhenskaya-odezhda/dlya-beremennyh', 93: 'odezhda-obuv/detskaya-odezhda',
              1421: 'zhivotnye/dlya-koshek/korm-koshki', 1911: 'deti/kolyaski/kolyaski-3-v-1'
              }



def initial_proceed(filename):
    try:
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open(f'{filename.split(".")[0]}/init.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    metadata = {
                        'price-list-name': row['price-list-name'],
                        'author_id': row['author_id'],
                    }

                response["metadata"] = metadata
                response["posts"] = posts

                if row['is-images'] == 't':
                    return with_images_proceed(filename)

                if row['is-images'] == 'f':
                    return without_images_proceed(filename)

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
                        client: AsyncClient = get_httpx()
                        ):
    try:
        images = []
        images_dir = str(uuid.uuid4())
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open(f'{filename.split(".")[0]}/posts.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    post = {
                        'title': row['title'],
                        'description': row['description'],
                        'coordinates': f"POINT({float(row['coordinates'].split(',')[0]), float(row['coordinates'].split(',')[1])})",
                        'category_path': PATH_TO_ID[row['category_path']],
                        'images_dir': images_dir,
                        'images': images,

                    }
                    imgs = row['images'].split(',')

                    for m in imgs:
                        with z.open(f'{filename.split(".")[0]}/imgs/{m}', "rb") as k:

                            name = str(uuid.uuid4())

                            async def read_file():
                                while chunk := await k.read(64 * 1024):
                                    yield chunk

                            req = client.build_request(
                                method="POST",
                                url=f"http://media-service:5001/api/v1/media/add-image/{filename}?p_directory={images_dir}",
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
            "message": "Something went wrong!"
        }


def without_images_proceed(filename):
    try:
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open(f'{filename.split(".")[0]}/posts.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    post = {
                        'title': row['title'],
                        'description': row['description'],
                        'coordinates': f"POINT({float(row['coordinates'].split(',')[0]), float(row['coordinates'].split(',')[1])})",
                        'category_path': PATH_TO_ID[row['category_path']],
                    }
                posts.append(post)

        return response

    except Exception:
        return {
            "status": "False",
            "message": "Something went wrong!"
        }
