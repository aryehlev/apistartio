"""testing module of api
"""
from fastapi.testclient import TestClient

from apistartio.main import app



def test_keepalive():
    """runs tests for three endpoints
    """
    with TestClient(app) as client:
        response = client.get("/keepalive")
        assert response.status_code == 200
        assert response.json() == {"ready": True, "message": "waiting for query"}

        response = client.get("/userStats/efb64b4e-3655-4a4a-af2d-4d62945eb6d0")
        assert response.status_code == 200
        assert response.json() == {"num_requests":2,"num_impressions":2,
                                "num_clicks":1,"avg_price_for_bid_won":2.1,
                                "mediun_impression_duration":21.0,
                                "max_time_passed_till_click":63}


        response = client.get("/sessionId/8df4f33b-ba8b-4d82-ab42-46d5fc72f8d0")
        assert response.status_code == 200
        assert response.json() == {"begin":1625003107,"finish":1625003618,"partner":"ynet"}
