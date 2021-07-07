from fastapi import FastAPI

from data_handler import data_handler

dh = data_handler()
app = FastAPI()

@app.on_event("startup")
def startup_event():
    dh.setup_df()
  

@app.get("/keepalive")
async def root():
    return {"ready": True, "message": "waiting for query"}


@app.get("/userStats/{user_id}")
async def root(user_id):
    
    data = dh.get_user_info(user_id)
    result = await data
    return result

@app.get("/sessionId/{session_id}")
async def root(session_id):
    
    data =  await dh.get_session_info(session_id)
   
    return data


