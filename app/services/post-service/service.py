from fastapi import FastAPI
import random

app = FastAPI()

@app.get("/posts/get-post")
async def get_post():
    return {"status": "True",
            "p_image": str(random.randint(1, 100)),
            "p_title": str(random.randint(1, 100)),
            }