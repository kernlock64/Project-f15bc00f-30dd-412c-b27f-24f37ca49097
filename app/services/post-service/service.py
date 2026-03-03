from fastapi import FastAPI
import random

'''TEST DATA/TEST ENDPOINTS'''

app = FastAPI()

@app.get("/posts/get-post")
async def get_post():
    return {"status": "True",
            "p_image": str(random.randint(1, 100)),
            "p_title": str(random.randint(1, 100)),
            }

@app.post("/posts/get-posts")
async def get_posts():
    return {"status": "True",}

@app.post("/posts/deletion")
async def get_post():
    return {"status": "True",
            }


'''TEST DATA/TEST ENDPOINTS'''