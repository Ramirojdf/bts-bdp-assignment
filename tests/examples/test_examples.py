import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette import status

from bdi_api.examples import v0_router


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="Common test")
    app.include_router(v0_router)
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    app.include_router(v0_router)
    yield TestClient(app)


class TestExamples:
    def test_hello_world(self, client) -> None:
        with client as client:
            response = client.get("/api/v0")
        assert response.status_code == 200
        assert response.json() == {"Hello": "World"}

    @pytest.mark.parametrize(
        "item_id,params,should_be",
        [
            (1, "?q=param", {"item_id": 1, "q": "param"}),
            (5, "", {"item_id": 5, "q": None}),
        ],
    )
    def test_id_endpoint(self, client: TestClient, item_id, params, should_be) -> None:
        with client as client:
            response = client.get(f"/api/v0/items/{item_id}{params}")
        assert response.status_code == 200
        assert response.json() == should_be

    def test_not_exists(self, client) -> None:
        with client as client:
            response = client.get("/api/v0/not_existant")
        assert response.status_code == status.HTTP_404_NOT_FOUND
