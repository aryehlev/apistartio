"""main module to run the api- contains endpoints
"""
from fastapi import FastAPI
from data import data_handler # pylint: disable=import-error

dh = data_handler.DataHandler()
app = FastAPI()

@app.on_event("startup")
def startup_event():
    """
    runs when server is set up- sets up database on aws from the files
    """
    dh.setup_schema(host='stario.cna1qj9bze8h.us-east-2.rds.amazonaws.com',
                    user='admin',
                    password='lqw120&8%mna')

@app.on_event("shutdown")
def shutdown_event():
    """runs when server shuts down closes the database connections
    """
    dh.close_all()

@app.get("/keepalive")
async def root():
    """endpoint to check that api is up and running

    Returns:
        dict: just saying its ready(json format)
    """
    return {"ready": True, "message": "waiting for query"}


@app.get("/userStats/{user_id}")
async def user(user_id):
    """endpoint to get a users  info base on id.
    gets this info:
    1.Num of requests
    2.Num of impressions
    3.Num of clikcs
    4.Average price for bid (include only wins)
    5.Median Impression duration
    6.Max time passed till click

    Args:
        user_id (string): the user id to get the info for

    Returns:
        dict: returns dictionary with the values stated above(json format)
        1.num_of_requests
        2.num_of_impression
        3.num_of_clicks
        4.avg_price_bid
        5.median_impression_duration
        6.max_time_passed
    """
    data = await dh.get_user_info(user_id)

    return data

@app.get("/sessionId/{session_id}")
async def session(session_id):
    """endpoint to get a session info base on id.
    gets this info:
    1.Begin: request timestamp
    2.Finish: latest timestamp (request/click/impression)
    3.Partner name
    Args:
        session_id (string): the session id to get the info for

    Returns:
        dict: returns dictionary with the values stated above(json format)
        1.begin
        2.finish
        3.partner
    """
    data =  await dh.get_session_info(session_id)

    return data
