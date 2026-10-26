"""FastAPI application."""

from fastapi import FastAPI

from app.routers.query import router


def create_app() -> FastAPI:
    """Build the application with the query and stream routes."""
    application = FastAPI(title="ADLC FastAPI")
    application.include_router(router)
    return application


app = create_app()
