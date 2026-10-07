import zipfile
import csv
import uuid
from service import get_httpx
import os
from watermark import *
from categories_cache import *

MEDIA_KEY = os.getenv('MEDIA_KEY')


async def type_checker(data, datatype, name):
    if datatype == bool:
        if data not in ["True", "False"]:
            return False

    if datatype == str:

        if not data.isalpha():
            return False

        if name == "sostoianie":
            if data not in ["used", "new"]:
                return False
        if name == "tip":
            if data not in ["litye", "kovanye", "shtampovka"]:
                return False
        if name == "poljarnost":
            if data not in ["pryamaya", "obratnya"]:
                return False

    if datatype == int:
        if not data.isdigit():
            return False

    return True


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
            "message": e
        }


async def with_images_proceed(
        filename,
        posts,
        response
):
    try:
        images = []
        client = await get_httpx()
        with zipfile.ZipFile(f'temp/{filename}', 'r') as z:
            with z.open('posts.csv') as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                reader = csv.DictReader(text_file)
                for row in reader:
                    images_dir = str(uuid.uuid4())
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

                    for element in row['attributes'].split(','):
                        if str(element).split(":")[0] not in ATTRIBUTES_CACHE[PATH_TO_ID[row['category_path']]] and row[
                            'attributes'].split(',')[0] != "":
                            return {
                                "status": "False",
                                "message": "Attribute not found!"
                            }

                    if not await type_checker(str(element).split(":")[1], ATTRIBUTES_COND[str(element).split(":")[0]],
                                       str(element).split(":")[0]):
                        return {
                            "status": "False",
                            "message": "Wrong attribute or value!"
                        }

                    imgs = row['images'].split(',')

                    for i in imgs:
                        if i.split(".")[1] != "jpeg":
                            return {
                                "status": "False",
                                "message": "Incorrect file extension!"
                            }

                    if not row['title_image'] in imgs:
                        return {
                            "status": "False",
                            "message": "Something went wrong!"
                        }

                    for m in imgs:
                        with z.open(f'imgs/{m}', "r") as k:

                            info = z.getinfo(f'imgs/{m}')

                            if info.file_size > 10000000:
                                return {
                                    "status": "False",
                                    "message": "Incorrect file size!"
                                }

                            watermarked_i = watermark(k)

                            name = str(uuid.uuid4())
                            mod_name = name + ".jpeg"

                            if str(m) == row['title_image']:
                                post["title_image"] = mod_name

                            req = client.build_request(
                                method="POST",
                                url=f"http://media-service:5001/api/v1/media/add-image/jpeg?p_directory={images_dir}",
                                headers={"X-Key": MEDIA_KEY, "X-Name": name},
                                content=watermarked_i.getvalue(),
                            )

                            resp = await client.send(req)

                            images.append(mod_name)

                            if resp.status_code != 200:
                                return {
                                    "status": "False",
                                    "message": "Error adding image",
                                }
                posts.append(post)
            response["metadata"]["status"] = "True"
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
                    for element in row['attributes'].split(','):
                        if str(element).split(":")[0] not in ATTRIBUTES_CACHE[PATH_TO_ID[row['category_path']]] and row[
                            'attributes'].split(',')[0] != "":
                            return {
                                "status": "False",
                                "message": "Attribute not found!"
                            }

                    if not await type_checker(str(element).split(":")[1], ATTRIBUTES_COND[str(element).split(":")[0]],
                                       str(element).split(":")[0]):
                        return {
                            "status": "False",
                            "message": "Wrong attribute or value!"
                        }


                posts.append(post)

            response["metadata"]["status"] = "True"
            return response


    except Exception as e:
        return {
            "status": "False",
            "message": e
        }
