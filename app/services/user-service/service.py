from fastapi import FastAPI

app = FastAPI()

@app.post("/users/add-user")
async def add_user():
    return {"status": "True"}

@app.post("/users/delete-user")
async def delete_user():
    return {"status": "True"}