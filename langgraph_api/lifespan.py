from contextlib import asynccontextmanager

from starlette.applications import Starlette

import langgraph_api.config as config
from langgraph_api.asyncio import SimpleTaskGroup
from langgraph_api.cron_scheduler import cron_scheduler
from langgraph_api.graph import collect_graphs_from_env, stop_remote_graphs
from langgraph_api.http import start_http_client, stop_http_client
from langgraph_api.metadata import metadata_loop
from langgraph_api.queue import queue
from langgraph_license.validation import get_license_status, plus_features_enabled
from langgraph_storage.database import start_pool, stop_pool


@asynccontextmanager
async def lifespan(app: Starlette):
    print('ls1')
    if not await get_license_status():
        print('ls2')
        raise ValueError(
            "License verification failed. Please ensure proper configuration:\n"
            "- For local development, set a valid LANGSMITH_API_KEY for an account with LangGraph Cloud access "
            "in the environment defined in your langgraph.json file.\n"
            "- For production, configure the LANGGRAPH_CLOUD_LICENSE_KEY environment variable "
            "with your LangGraph Cloud license key.\n"
            "Review your configuration settings and try again. If issues persist, "
            "contact support for assistance."
        )
    print('ls3')
    await start_http_client()
    print('ls4')
    await start_pool()
    print('ls5')
    await collect_graphs_from_env(True)
    print('ls6')
    try:
        async with SimpleTaskGroup(cancel=True) as tg:
            tg.create_task(metadata_loop())
            tg.create_task(queue(config.N_JOBS_PER_WORKER, config.BG_JOB_TIMEOUT_SECS))
            if config.FF_CRONS_ENABLED and plus_features_enabled():
                tg.create_task(cron_scheduler())
            print('ls7')
            yield
    finally:
        print('ls8')
        await stop_remote_graphs()
        await stop_http_client()
        await stop_pool()
        print('ls10')