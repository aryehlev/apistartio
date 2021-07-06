from fastapi import FastAPI

from data_handler import data_handler


app = FastAPI()

@app.on_event("startup")
def startup_event():
    dh = data_handler()

    dh.setup_session_df()

@app.get("/keepalive")
async def root():
    return {"ready": True, "message": "waiting for query"}


@app.get("/userStats")
async def root():
    import asyncio
    await asyncio.sleep(100000)

    return "hello"

@app.get("/sessionId")
async def root():
    return {"ready": True, "message": "waiting for query"}