
import time
from typing import List, Tuple

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from worker.analyse import Analyse
from worker.config import manage_sensitive
from worker.valuation import Valuation

from celery import Celery, group, signals
from celery.result import allow_join_result

# Initialize Celery
celery = Celery(
    'worker',
    broker = manage_sensitive("CELERY_BROKER_URL"),
    backend = manage_sensitive("CELERY_RESULT_BACKEND"),
    task_ingnore_result= True,
    task_acks_late = True,
    worker_prefetch_multiplier= 1,
)

@signals.celeryd_init.connect
def init_sentry(**_kwargs):
    if not bool(manage_sensitive("DEBUG", default="False")):
        sentry_sdk.init(
            dsn ="https://463e69188a7e46fca4408d5f23284fe9@o4504585220980736.ingest.sentry.io/4504649931489280",
            traces_sample_rate = 1.0,
            integrations=[
                CeleryIntegration(),
            ]
        )

@celery.task(name="worker.analyse")
def analyse_task(area: str, area_type: str):
    area = area.upper()
    area_type = area_type.upper()
    aggregator = Analyse()
    if area == "ALL" and area_type == "COUNTRY":
        aggregator.run(area, area_type)
    else:
        aggregator.run(area, area_type)
    aggregator.clean_up()
    return area + area_type

@celery.task(name="worker.valuation")
def valuation_task(houseid: str):
    valuater = Valuation()
    if valuater.check_house(houseid):
        areas = valuater.get_areas()
        get_analysis_of_areas(areas)
        aggs = valuater.load_aggregations(areas)
        perc_changes = valuater.find_monthly_averages(aggs)
        sales = valuater.get_house_sales()
        valuations = valuater.calc_latest_price(sales, perc_changes)
        price_range = valuater.calculate_range(valuations)
        return {
            "price_range": price_range,
        }
    else:
        return "No House Found"

@celery.task(name="worker.analyse_multiple")
def get_analysis_of_areas(areas: List[Tuple[str,str]]) -> None:
    tasks = []
    for area in areas:
        tasks.append(analyse_task.subtask(area))
    tasks = group(tasks)
    job = tasks.apply_async()
    while not job.ready():
        time.sleep(0.2)
    with allow_join_result():
        results = job.get()
