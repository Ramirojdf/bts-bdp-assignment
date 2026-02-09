import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from bdi_api.app import app as real_app


@pytest.fixture(scope="class")
def app() -> FastAPI:
    return real_app


@pytest.fixture(scope="class")
def client(app: FastAPI) -> TestClient:
    yield TestClient(app)
