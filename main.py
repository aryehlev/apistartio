from fastapi import FastAPI

from data import data_handler

dh = data_handler.data_handler()
app = FastAPI()

@app.on_event("startup")
def startup_event():
    dh.setup_schema(host='stario.cna1qj9bze8h.us-east-2.rds.amazonaws.com', user='admin', password='lqw120&8%mna')


@app.on_event("shutdown")
def shutdown_event():
    dh.close_all()

@app.get("/keepalive")
async def root():
    return {"ready": True, "message": "waiting for query"}


@app.get("/userStats/{user_id}")
async def root(user_id):
    
    data = await dh.get_user_info(user_id)
    
    return data

@app.get("/sessionId/{session_id}")
async def root(session_id):
    
    data =  await dh.get_session_info(session_id)
   
    return data


