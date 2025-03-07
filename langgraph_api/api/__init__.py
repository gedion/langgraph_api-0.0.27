import asyncio
import importlib
import importlib.util
import os

import structlog
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import BaseRoute, Mount, Route

from langgraph_api.api.assistants import assistants_routes
from langgraph_api.api.meta import meta_info, meta_metrics
from langgraph_api.api.openapi import get_openapi_spec
from langgraph_api.api.runs import runs_routes
from langgraph_api.api.store import store_routes
from langgraph_api.api.threads import threads_routes
from langgraph_api.auth.middleware import auth_middleware
from langgraph_api.config import HTTP_CONFIG, MIGRATIONS_PATH
from langgraph_api.graph import js_bg_tasks
from langgraph_api.validation import DOCS_HTML
from langgraph_storage.database import connect, healthcheck

logger = structlog.stdlib.get_logger(__name__)

async def ok(request: Request):
    check_db = int(request.query_params.get("check_db", "0"))
    logger.info(f"Received /ok request, check_db={check_db}")
    
    if check_db:
        await healthcheck()
        logger.info("Database health check completed successfully")

    if js_bg_tasks:
        from langgraph_api.js.remote import js_healthcheck
        await js_healthcheck()
        logger.info("JS background tasks health check completed successfully")

    return JSONResponse({"ok": True})

async def openapi(request: Request):
    logger.info("Received request for OpenAPI spec")
    spec = await asyncio.to_thread(get_openapi_spec)
    return Response(spec, media_type="application/json")

async def docs(request: Request):
    logger.info("Received request for API documentation")
    return HTMLResponse(DOCS_HTML)

# Define meta routes
meta_routes: list[BaseRoute] = [
    Route("/ok", ok, methods=["GET"]),
    Route("/openapi.json", openapi, methods=["GET"]),
    Route("/docs", docs, methods=["GET"]),
    Route("/info", meta_info, methods=["GET"]),
    Route("/metrics", meta_metrics, methods=["GET"]),
]

logger.debug("Meta routes initialized", meta_routes=[route.path for route in meta_routes])

protected_routes: list[BaseRoute] = []

# Configure protected routes based on HTTP_CONFIG
if HTTP_CONFIG:
    logger.info("Processing HTTP_CONFIG for protected routes")

    if not HTTP_CONFIG.get("disable_assistants"):
        protected_routes.extend(assistants_routes)
        logger.debug("Added assistant routes")

    if not HTTP_CONFIG.get("disable_runs"):
        protected_routes.extend(runs_routes)
        logger.debug("Added runs routes")

    if not HTTP_CONFIG.get("disable_threads"):
        protected_routes.extend(threads_routes)
        logger.debug("Added threads routes")

    if not HTTP_CONFIG.get("disable_store"):
        protected_routes.extend(store_routes)
        logger.debug("Added store routes")

else:
    logger.warning("No HTTP_CONFIG found, enabling all default routes")
    protected_routes.extend(assistants_routes)
    protected_routes.extend(runs_routes)
    protected_routes.extend(threads_routes)
    protected_routes.extend(store_routes)

logger.debug("Protected routes initialized", protected_routes=[route.path for route in protected_routes])

routes: list[BaseRoute] = []
user_router = None

def load_custom_app(app_import: str) -> Starlette | None:
    logger.info(f"Attempting to load custom app from {app_import}")
    path, name = app_import.rsplit(":", 1)
    
    try:
        os.environ["__LANGGRAPH_DEFER_LOOPBACK_TRANSPORT"] = "true"
        
        if os.path.isfile(path) or path.endswith(".py"):
            logger.debug(f"Importing app from file path: {path}")
            spec = importlib.util.spec_from_file_location("user_router_module", path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load spec from {path}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            logger.debug(f"Importing app as module: {path}")
            module = importlib.import_module(path)

        user_router = getattr(module, name)
        logger.info(f"Loaded user router: {name} from {path}")

        if not isinstance(user_router, Starlette):
            raise TypeError(f"Object '{name}' in module '{path}' is not a Starlette or FastAPI application.")

    except ImportError as e:
        logger.error(f"Failed to import app module '{path}'", exc_info=True)
        raise ImportError(f"Failed to import app module '{path}'") from e
    except AttributeError as e:
        logger.error(f"App '{name}' not found in module '{path}'", exc_info=True)
        raise AttributeError(f"App '{name}' not found in module '{path}'") from e
    finally:
        os.environ.pop("__LANGGRAPH_DEFER_LOOPBACK_TRANSPORT", None)

    return user_router

# Initialize routes based on configuration
if HTTP_CONFIG:
    if router_import := HTTP_CONFIG.get("app"):
        user
