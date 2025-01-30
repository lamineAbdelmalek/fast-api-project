from awesome_api.fastapi_views import app
from fastapi.testclient import TestClient


def test_hello_world():
    with TestClient(app) as client:
        response = client.get("/hello")
        assert response.status_code == 200
