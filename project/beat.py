import os
from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

beat = Celery(__name__)
# app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
# app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@beat.task(name='minute_check')
def check():
    print('I am checking your stuff')
    return True
       
beat.conf.beat_schedule = {
    'run-me-every-ten-minutes': {
        'task': 'minute_check',
        'schedule': 10.0
    }
}
