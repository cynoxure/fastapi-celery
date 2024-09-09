import os
import time
from pathlib import Path
from typing import List, Dict
import json

from celery import Celery, Task, chain, group, chord
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
from celery.utils.graph import DependencyGraph

logger = get_task_logger(__name__)

celery = Celery(__name__, result_extended=True)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery.task(name='minute_check')
def check():
    print('I am checking stuff')
    return True
       
celery.conf.beat_schedule = {
    'run-me-every-ten-minutes': {
        'task': 'minute_check',
        'schedule': 600.0
    }
}

# class PakTask(Task):
#     abstract = True    

#     task_id = None
#     scrape_id = None
#     tiles_id = None
#     populate_id = None
#     move_id = None

#     task_results = None
#     scrape_results = None
#     tiles_results = None
#     populate_results = None
#     move_results = None

#     def __init__(self, w:int, x:int, y:int):
#         self.w = w
#         self.x = x
#         self.y = y

#     def __call__(self, *args, **kwargs):
#         self.w:int = kwargs['w']
#         self.x:int = kwargs['x']
#         self.y:int = kwargs['y']
#         if len(args) == 1 and isinstance(args[0], dict):
#             kwargs.update(args[0])
#             args = ()
#         task = super(PakTask, self).__call__(*args, **kwargs)
#         self.task_id = task.id
#         return self.task_id

#     # def start(self):
#     #     kwargs = {"w": self.w, "x":self.x, "y":self.y}
#     #     task = make_pak.apply_async(kwargs=kwargs)
#     #     self.task_id = task.id
#     #     return self.task_id
    
#     def get_results(self, task_id, dump: bool = False):
#         task = AsyncResult(task_id)
#         result = task.result
#         if dump:
#             self.dump_task(task)
#         data = {
#             "task_id": task.id,
#             "task_status": task.status,
#             "task_result": result
#         }
#         return data

#     def get_status(self):
#         if self.scrape_results is None:
#             return "NONE"
        
#         if (self.task_results.status == "SUCCESS") and \
#             not ((self.scrape_results is None) or \
#                  (self.tiles_results is None) or \
#                  (self.populate_results is None) or \
#                  (self.move_results is None)):
        
#             if (self.scrape_results['task_status'] == "SUCCESS") and \
#                 (self.tiles_results['task_status'] == "SUCCESS") and \
#                 (self.populate_results['task_status'] == "SUCCESS") and \
#                 (self.move_results['task_status'] == "SUCCESS"):
#                 return "COMPLETE"
            
#             if (self.scrape_results['task_status'] == "FAILURE") or \
#                 (self.tiles_results['task_status'] == "FAILURE") or \
#                 (self.populate_results['task_status'] == "FAILURE") or \
#                 (self.move_results['task_status'] == "FAILURE"):
#                 return "FAILURE"
            
#         return self.task_results.status
        

#     def get_result_dict(self, dump: bool = False):
#         self.task_results = AsyncResult(self.task_id)
#         result = self.task_results.result
#         status = self.task_results.status
#         if dump:
#             self.dump_task(self.task_results)

#         if (not self.task_results.info is None) and len(self.task_results.info) > 0:
#             move = self.task_results.info.pop(0)
#             print(f'move: {move}')
#             self.move_id = move[0]
#             populate = move[1].pop(0)
#             print(f'populate: {populate}')
#             self.populate_id = populate[0]
#             tiles = populate[1].pop(0)
#             print(f'tiles: {tiles}')
#             self.tiles_id = tiles[0]
#             scrape = tiles[1].pop(0)
#             print(f'scrape: {scrape}')
#             self.scrape_id = scrape[0]
#             self.scrape_results = self.get_results(self.scrape_id)
#             self.tiles_results = self.get_results(self.tiles_id)
#             self.populate_results = self.get_results(self.populate_id)
#             self.move_results = self.get_results(self.move_id)

#             status = self.get_status()

#             result = json.dumps({"scrape": self.scrape_results['task_status'],
#                       "tiles": self.tiles_results['task_status'],
#                       "populate": self.populate_results['task_status'],
#                       "move": self.move_results['task_status'],
#                       }, separators=(',', ':'))

#         data = {
#             "task_id": self.task_id,
#             "w": self.w,
#             "x": self.x,
#             "y": self.y,
#             "task_status": status,
#             "task_result": result
#         }
#         return data

#     def dump_task(self, task):
#         print(f'task[{task.id}')
#         print(f'  > name: {task.name}')
#         print(f'  > info: {task.info}')
#         print(f'  > queue: {task.queue}')
#         print(f'  > state: {task.state}')
#         print(f'  > status: {task.status}')
#         print(f'  > worker: {task.worker}')
#         # print(f'  > graph: {task.graph}')
#         print(f'  > ignored: {task.ignored}')
#         print(f'  > children: {task.children}')
#         print(f'  > kwargs: {task.kwargs}')
#         print(f'  > as_list: {task.as_list()}')


class Job:

    task_results = None
    children_ids = []
    children = {}
    parent_id = None
    task_id = None
    pct = 0
    verbose = False

    def __init__(self, task_id, parent_id = None, verbose = False):
        self.task_id = task_id
        self.parent_id = parent_id
        self.verbose = verbose

    def id(self):
        return self.task_id

    def root(self):
        return self.parent_id is None

    def decode_info(self):
        return NotImplementedError
    
    def name(self):
        if self.task_results is None:
            return '---'
        return self.task_results.name

    def status(self):
        if self.task_results is None:
            return '---'
        return self.task_results.status

    def result(self):
        if self.task_results is None:
            return '---'
        return self.task_results.result

    def update(self, force:bool = False):
        self.task_results = AsyncResult(self.task_id)
        self.parent_id = self.task_results.parent

        self.decode_info()

        graph = None
        try:
            if self.task_results.children:
                graph : DependencyGraph = self.task_results.graph
        except Exception as e:
            print(f'>>>>>  graph exception: {e}')
        if graph:
            for (parent, children)  in graph.items():
                print(f'>>>>>  children: {children}')
                for child in children:
                    if child.id not in self.children_ids:
                        self.children_ids.append(child.id) 
                    # print(f'>>>>>  add job: {child.id}')
                    # jobs.addChild(parent_id=self.task_id, child_id=child.id)

        for child_id in self.children_ids:
            self.children[child_id] = AsyncResult(child_id)
        return self.children_ids
    
class PakJob(Job):

    scrape_id = None
    tiles_id = None
    populate_id = None
    move_id = None
    complete_id = None

    tasks = []

    def __init__(self, task_id, parent_id = None, verbose = False):
        Job.__init__(self, task_id=task_id, parent_id=parent_id, verbose=verbose)

    def name(self):
        if self.task_results is None:
            return 'MakePak/*/*/*'
        elif (self.task_results.args is None) or \
             (len(self.task_results.args) < 3):
            print(f'>>>> PakJob args: {self.task_results.args}')
            return self.task_results.name
        w = int(self.task_results.args[0])
        x = int(self.task_results.args[1])
        y = int(self.task_results.args[2])
        return f'MakePak ({w}/{x}/{y})'
        
    def decode_info(self):
        if (not self.task_results.info is None):
            info = self.task_results.info
            while (not info is None) and isinstance(info, list) and (len(info) > 0):
                set = info.pop(0)
                if (not set is None) and (len(set) > 0):
                    id = set[0]
                    if id not in self.children_ids:
                        self.children_ids.insert(0,id) 
                info = set[1]
            return len(self.children_ids) > 0
        return False

    def status(self):
        if self.task_results is None:
            return '???'
        
        if (self.task_results.status == "SUCCESS"):
        
            for child_id in self.children_ids:
                if (child_id in self.children) and \
                    (self.children[child_id].status == "FAILURE"):
                    return "FAILURE"
            
            if self.complete_id and (self.complete_id in self.children) and \
                (self.children[self.complete_id].status == "SUCCESS"):
                self.pct = 100
                return "COMPLETE"
            elif self.move_id and (self.move_id in self.children) and \
                (self.children[self.move_id].status== "SUCCESS"):
                self.pct = 90
                return "MOVING"
            elif self.populate_id and (self.populate_id in self.children) and \
                (self.children[self.populate_id].status == "SUCCESS"):
                self.pct = 60
                return "DEMING"
            elif self.tiles_id and (self.tiles_id in self.children) and \
                (self.children[self.tiles_id].status == "SUCCESS"):
                self.pct = 40
                return "TILING"
            elif self.scrape_id and (self.scrape_id in self.children) and \
                (self.children[self.scrape_id].status == "SUCCESS"):
                self.pct = 10
                return "SCRAPING"
            
        return '???'

    def result(self):
        if self.task_results is None:
            return '---'
        return self.task_results.result

    def update(self, force:bool = False):
        child_ids = Job.update(self, force)

        if len(self.children_ids) > 0:
            self.scrape_id = self.children_ids[0]
        if len(self.children_ids) > 1:
            self.tiles_id = self.children_ids[1]
        if len(self.children_ids) > 2:
            self.populate_id = self.children_ids[2]
        if len(self.children_ids) > 3:
            self.move_id = self.children_ids[3]
        if len(self.children_ids) > 4:
            self.complete_id = self.children_ids[4]
        self.setTasks()
        return child_ids

    def setTasks(self):
        scrape = jobs.get_job(self.scrape_id)
        self.tasks.clear()
        if scrape:
            scrape.parent_id = self.task_id
            self.tasks.append(scrape)
        tiles = jobs.get_job(self.tiles_id)
        if tiles:
            tiles.parent_id = self.task_id
            self.tasks.append(tiles)
        populate = jobs.get_job(self.populate_id)
        if populate:
            populate.parent_id = self.task_id
            self.tasks.append(populate)
        move = jobs.get_job(self.move_id)
        if move:
            move.parent_id = self.task_id
            self.tasks.append(move)
        complete = jobs.get_job(self.complete_id)
        if complete:
            complete.parent_id = self.task_id
            self.tasks.append(complete)
        self.updateResults()

    def updateResults(self):
        if self.scrape_id:
            self.children[self.scrape_id] = AsyncResult(self.scrape_id)
        if self.tiles_id:
            self.children[self.tiles_id] = AsyncResult(self.tiles_id)
        if self.populate_id:
            self.children[self.populate_id] = AsyncResult(self.populate_id)
        if self.move_id:
            self.children[self.move_id] = AsyncResult(self.move_id)
        if self.complete_id:
            self.children[self.complete_id] = AsyncResult(self.complete_id)


class GroupJob(Job):

    group_name = None

    group_id= None
    group_results = None

    pak_ids = []
    pak_jobs = {}

    def __init__(self, name, task_id, parent_id = None, verbose = False):
        Job.__init__(self, task_id=task_id, parent_id=parent_id, verbose=verbose)
        self.group_name = name


    def name(self):
        if self.group_name is None:
            return '---'
        return self.group_name

        
    def status(self):
        if self.task_results is None:
            return '???'
        
        if (self.task_results.status == "SUCCESS"):
        
            if self.group_results is None:
                return 'NO GROUP'
        
            if self.group_results.status == "FAILURE":
                return "FAILURE"
            
            success = 0
            for pak_id, pak_job in self.pak_jobs.items():
                if self.children[pak_id].status == "FAILURE":
                    return "FAILURE"
            
                success = success + 1 if pak_job.status == "COMPLETE" else success
            
            if success == len(self.pak_ids):
                return "COMPLETE"
            return f'{success}/{len(self.pak_ids)} Paks Completed'
        
        return '???'

    def result(self):
        if self.task_results is None:
            return '---'
        return self.task_results.result

    def decode_info(self):
        if self.task_results.info:
            info = self.task_results.info
            print(f'DECODE INFO - info: {info}')
            set = info.pop(0)
            print(f'DECODE INFO - set: {set}')
            if (not set is None) and (len(set) > 0):
                self.group_id = set[0]
                if self.group_id not in self.children_ids:
                    self.children_ids.insert(0,self.group_id) 
            print(f'DECODE INFO - self.group_id: {self.group_id}')
            pak_list = info.pop(0)
            while (not pak_list is None) and isinstance(pak_list, list) and (len(pak_list) > 0):
                print(f'DECODE INFO - info: {pak_list}')
                pak_set = pak_list.pop(0)
                print(f'DECODE INFO - pak_set: {pak_set}')
                while (not pak_set is None) and isinstance(pak_set, list) and (len(pak_set) > 0):
                    print(f'DECODE INFO - subset: {pak_set}')
                    id = pak_set[0][0]
                    print(f'DECODE INFO - self.pak_id: {id}')
                    if id not in self.children_ids:
                        self.children_ids.insert(0,id) 
                    if id not in self.pak_ids:
                        self.pak_ids.append(id) 
                    pak_set = pak_list.pop(0) if (len(pak_list) > 0) else None
                pak_list = info.pop(0) if (len(info) > 0) else None
            return len(self.children_ids) > 0
        return False

    def update(self, force:bool = False):
        self.task_results = AsyncResult(self.task_id)
        self.parent_id = self.task_results.parent

        if self.group_id:
            self.group_results = AsyncResult(self.task_id)

        self.decode_info()

        print(f' MAKE_PAKS[{self.group_id}]: {self.group_results}')

        child_ids = []
        graph = None
        try:
            if self.task_results.children:
                graph : DependencyGraph = self.task_results.graph
        except Exception as e:
            print(f'>>>>>  graph exception: {e}')
        if graph:
            for (parent, children)  in graph.items():
                print(f'>>>>>  children: {children}')
                # if (parent is self.task_id) and (len(children) == 1):
                #     self.group_id = children[0]
                for child in children:
                    if child.id not in self.children_ids:
                        self.children_ids.append(child.id)
                    if parent is self.group_id: 
                        self.pak_ids.append(child.id)

        self.setTasks()
        return child_ids

    def setTasks(self):
        for id in self.pak_ids:
            pak_job = jobs.get_job(id)
            if not pak_job:
                pak_job = jobs.addPakJob(id)
            pak_job.parent_id = self.task_id
            self.pak_jobs[id] = pak_job
        self.updateResults()

    def updateResults(self):
        pct = 0
        for job in self.pak_jobs.values():
            job.update()
            pct = pct + job.pct / len(self.pak_ids)
        self.pct = pct


class Jobs:

    job_dict = {}

    def addGroupJob(self, name, task_id):
        print(f'ADD GROUPJOB: {task_id}')
        job = GroupJob(name, task_id)
        self.job_dict[task_id] = job
        return job

    def addPakJob(self, task_id):
        print(f'ADD PAKJOB: {task_id}')
        job = PakJob(task_id)
        self.job_dict[task_id] = job
        return job

    def addJob(self, task_id):
        print(f'ADD JOB: {task_id}')
        self.job_dict[task_id] = Job(task_id)

    # def addChild(self, child_id):
    #     if not child_id in self.job_dict:
    #         job = Job(child_id)
    #         self.job_dict[child_id] = job

    def updateJobs(self):
        new_ids = []
        try:
            for job_id , job in self.job_dict.items():
                children_ids = job.update()
                for child_id in children_ids:
                    if (child_id in self.job_dict) or (child_id in new_ids):
                        continue
                    new_ids.append(child_id)
        except Exception as e:
            pass
        # for child_id in new_ids:
        #     self.addChild(child_id=child_id)

    def get_job(self, id):
        return self.job_dict.get(id)

    def get_jobs(self):
        job_list = []
        for job in self.job_dict.values():
            if job.root():
                print(f'JOB: {job}')
                job_list.append(job)
        return job_list
    
    def clear(self):
        self.job_dict.clear()

jobs = Jobs()

@celery.task(name="create_task")
def create_task(task_type):
    logger.info('create_task')
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
    logger.info('report_paks')
    return paks


@celery.task(name="chord_paks")
def chord_paks(paks:dict):
    logger.info('chord_paks')
    # print(f'make_pak: { db_file }')
    time.sleep(1)
    # tiles = { 1 : 12, 2 : 42 }
    res = chord(make_pak.s(w,x,y) for (w, x, y) in unpak(paks))(report_paks.s())
    return res # work_folder, tiles, db_file, repo_path


@celery.task(name="make_paks")
def make_paks(paks):
    logger.info('make_paks')
    # print(f'make_pak: { db_file }')
    time.sleep(1)
    # tiles = { 1 : 12, 2 : 42 }
    res = group(make_pak.s(w, x, y) for (w, x, y) in unpak(paks)).apply_async()
    return res # work_folder, tiles, db_file, repo_path

@celery.task(name="make_pak")
def make_pak(w:int, x:int, y:int):

# @celery.task(name="make_pak")
# def make_pak(kwargs):
#     w:int = kwargs['w']
#     x:int = kwargs['x']
#     y:int = kwargs['y']
    work_folder = Path('/work/folder')
    repo_path = Path(f'/repo/{w}/{x}')
    db_file = Path(f'/work/folder/{y}.mbtiles')
    logger.info(f'make_pak: { repo_path }/ { y }.mbtiles')
    time.sleep(1)
    tiles = { 1 : 12, 2 : 42 }
    kwargs = {}
    kwargs['db_file'] = str(db_file)
    kwargs['work_folder'] = str(work_folder)
    kwargs['repo_path'] = str(repo_path)
    kwargs['tiles'] = tiles
    res = chain(scrape_tiles.s(kwargs=kwargs), 
                move_tiles_to_db.s(),
                populate_dem.s(),
                move_to_repo.s(),
                pak_complete.s()).apply_async()
    return res # work_folder, tiles, db_file, repo_path


@celery.task(name="scrape_tiles")
def scrape_tiles(kwargs):
    # logger.info(f'scrape_tiles: { tiles } -> { work_folder } START')
    for i in range(10):
        time.sleep(1)
    # logger.info(f'scrape_tiles: { tiles } -> { work_folder } COMPLETE')
    return kwargs


@celery.task(name="move_tiles_to_db")
def move_tiles_to_db(kwargs):
    work_folder:Path = kwargs['work_folder']
    # logger.info(f'move_tiles_to_db: { work_folder } -> { db_file } START')
    for i in range(3):
        time.sleep(1)
    # logger.info(f'move_tiles_to_db: { work_folder } -> { db_file } COMPLETE')
    return kwargs


@celery.task(name="populate_dem")
def populate_dem(kwargs):
    # logger.info(f'populate_dem: DEM -> { db_file } START')
    for i in range(10):
        time.sleep(1)
    # logger.info(f'populate_dem: DEM -> { db_file } COMPLETE')
    return kwargs


@celery.task(name="move_to_repo")
def move_to_repo(kwargs):
    # logger.info(f'move_to_repo: { db_file } -> { repo_path } START')
    for i in range(5):
        time.sleep(1)
    # logger.info(f'move_to_repo: { db_file } -> { repo_path } COMPLETE')
    return kwargs

@celery.task(name="pak_complete")
def pak_complete(kwargs):
    # logger.info(f'move_to_repo: { db_file } -> { repo_path } START')
    time.sleep(1)
    # logger.info(f'move_to_repo: { db_file } -> { repo_path } COMPLETE')
    return kwargs