import os
import time
from pathlib import Path
from typing import List, Dict

from celery import Celery, chain, group, chord
# from celery.utils.log import get_task_logger


celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True


def unpak(paks: dict):
    un_paks = []
    for w in paks.keys():
        for x in paks[w]:
            for y in paks[w][x]:
                un_paks.append((w,x,y))
    return un_paks


@celery.task(name="report_paks")
def report_paks(paks:dict):
    return paks


@celery.task(name="chord_paks")
def chord_paks(paks:dict):
    # print(f'make_pak: { db_file }')
    time.sleep(1)
    # tiles = { 1 : 12, 2 : 42 }
    res = chord(make_pak.s(w,x,y) for (w, x, y) in unpak(paks))(report_paks.s())
    return res # work_folder, tiles, db_file, repo_path


@celery.task(name="make_paks")
def make_paks(paks:dict):
    # print(f'make_pak: { db_file }')
    time.sleep(1)
    # tiles = { 1 : 12, 2 : 42 }
    res = group(make_pak.s(w,x,y) for (w, x, y) in unpak(paks)).apply_async()
    return res # work_folder, tiles, db_file, repo_path


@celery.task(name="make_pak")
def make_pak(w:int, x:int, y:int):
    work_folder = Path('/work/folder')
    repo_path = Path(f'/repo/{w}/{x}')
    db_file = Path(f'/work/folder/{y}.mbtiles')
    print(f'make_pak: { db_file }')
    time.sleep(1)
    tiles = { 1 : 12, 2 : 42 }
    res = chain(scrape_tiles.s(str(work_folder), tiles), 
                move_tiles_to_db.s(str(db_file)),
                populate_dem.s(),
                move_to_repo.s(str(repo_path))).apply_async()
    return res # work_folder, tiles, db_file, repo_path


@celery.task(name="scrape_tiles")
def scrape_tiles(work_folder:Path, tiles:dict):
    print(f'scrape_tiles: { tiles } -> { work_folder } START')
    time.sleep(10)
    print(f'scrape_tiles: { tiles } -> { work_folder } COMPLETE')
    return str(work_folder)


@celery.task(name="move_tiles_to_db")
def move_tiles_to_db(work_folder:Path, db_file:Path):
    print(f'move_tiles_to_db: { work_folder } -> { db_file } START')
    time.sleep(3)
    print(f'move_tiles_to_db: { work_folder } -> { db_file } COMPLETE')
    return str(db_file)


@celery.task(name="populate_dem")
def populate_dem(db_file:Path):
    print(f'populate_dem: DEM -> { db_file } START')
    time.sleep(10)
    print(f'populate_dem: DEM -> { db_file } COMPLETE')
    return str(db_file)


@celery.task(name="move_to_repo")
def move_to_repo(db_file:Path, repo_path:Path):
    print(f'move_to_repo: { db_file } -> { repo_path } START')
    time.sleep(1)
    print(f'move_to_repo: { db_file } -> { repo_path } COMPLETE')
    return str(repo_path)