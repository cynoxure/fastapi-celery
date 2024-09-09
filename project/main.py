import collections
import json
import pydot

from fastapi import Body, FastAPI, Form, Request, BackgroundTasks
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from celery import Celery
from celery.result import AsyncResult, GroupResult, ResultSet
from celery.utils.graph import DependencyGraph
from worker import create_task, make_pak, make_paks, chord_paks
from worker import Jobs, Job, jobs

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


class PakDef:
    from worker import make_pak
    
    task_id = None
    scrape_id = None
    tiles_id = None
    populate_id = None
    move_id = None
    complete_id = None

    task_results = None
    scrape_results = None
    tiles_results = None
    populate_results = None
    move_results = None
    complete_results = None

    def __init__(self, w:int, x:int, y:int):
        self.w = w
        self.x = x
        self.y = y

    def start(self):
        # kwargs = {"w": self.w, "x":self.x, "y":self.y}
        # task = make_pak.delay(kwargs)
        task = make_pak.delay(self.w, self.x, self.y)
        jobs.addPakJob(task_id=task.id)
        self.task_id = task.id
        return self.task_id
    
    def get_results(self, task_id, dump: bool = False):
        task = AsyncResult(task_id)
        result = task.result
        if dump:
            self.dump_task(task)
        data = {
            "task_id": task.id,
            "task_status": task.status,
            "task_result": result
        }
        return data

    def get_status(self):
        if self.task_results is None:
            return "NONE"
        
        if (self.task_results.status == "SUCCESS") and \
            not ((self.scrape_results is None) or \
                 (self.tiles_results is None) or \
                 (self.populate_results is None) or \
                 (self.move_results is None) or \
                 (self.complete_results is None)):
        
            if (self.scrape_results['task_status'] == "SUCCESS") and \
                (self.tiles_results['task_status'] == "SUCCESS") and \
                (self.populate_results['task_status'] == "SUCCESS") and \
                (self.move_results['task_status'] == "SUCCESS") and \
                (self.complete_results['task_status'] == "SUCCESS"):
                return "COMPLETE"
            
            if (self.scrape_results['task_status'] == "FAILURE") or \
                (self.tiles_results['task_status'] == "FAILURE") or \
                (self.populate_results['task_status'] == "FAILURE") or \
                (self.move_results['task_status'] == "FAILURE") or \
                (self.complete_results['task_status'] == "FAILURE"):
                return "FAILURE"
            
        return self.task_results.status
        

    def get_result_dict(self, dump: bool = False):
        self.task_results = AsyncResult(self.task_id)
        result = self.task_results.result
        status = self.task_results.status
        if dump:
            self.dump_task(self.task_results)

        if (not self.task_results.info is None) and len(self.task_results.info) > 0:
            complete = self.task_results.info.pop(0)
            print(f'complete: {complete}')
            self.complete_id = complete[0]
            move = complete[1].pop(0)
            print(f'move: {move}')
            self.move_id = move[0]
            populate = move[1].pop(0)
            print(f'populate: {populate}')
            self.populate_id = populate[0]
            tiles = populate[1].pop(0)
            print(f'tiles: {tiles}')
            self.tiles_id = tiles[0]
            scrape = tiles[1].pop(0)
            print(f'scrape: {scrape}')
            self.scrape_id = scrape[0]
            self.scrape_results = self.get_results(self.scrape_id)
            self.tiles_results = self.get_results(self.tiles_id)
            self.populate_results = self.get_results(self.populate_id)
            self.move_results = self.get_results(self.move_id)
            self.complete_results = self.get_results(self.complete_id)

            status = self.get_status()

            result = json.dumps({"scrape": self.scrape_results['task_status'],
                      "tiles": self.tiles_results['task_status'],
                      "populate": self.populate_results['task_status'],
                      "move": self.move_results['task_status'],
                      "complete": self.complete_results['task_status'],
                      }, separators=(',', ':'))

        data = {
            "task_id": self.task_id,
            "w": self.w,
            "x": self.x,
            "y": self.y,
            "task_status": status,
            "task_result": result
        }
        return data

    def dump_task(self, task:AsyncResult):
        if not task.ready():
            return
        
        print(f'task[{task.id}')
        print(f'  > name: {task.name}')
        print(f'  > info: {task.info}')
        print(f'  > queue: {task.queue}')
        print(f'  > state: {task.state}')
        print(f'  > status: {task.status}')
        print(f'  > worker: {task.worker}')
        print(f'  > ignored: {task.ignored}')
        print(f'  > children: {task.children}')
        print(f'  > parent: {task.parent}')
        print(f'  > args: {task.args}')
        print(f'  > kwargs: {task.kwargs}')
        print(f'  > as_list: {task.as_list()}')
        try:
            if not task.children is None:
                graph : DependencyGraph = task.graph

                for parent, children  in graph.iteritems():
                    if len(children) > 0:
                        print(f'edge: {parent.id} -> {children[0].id}')

                print(f'  > graph: {graph}')
                # dot_txt = 'digraph G {\n    start -> end;\n}'
                # graph, = pydot.graph_from_dot_data(dot_txt)
                # graph.write_png('logs/f.png')
                # dg = DependencyGraph(it=d)
                pipo = open("logs/make_dot.dot", "w+")
                graph.to_dot(pipo)

                print(f'  > topsort:')
                for ob in graph.topsort():
                    print(f'  >        : {ob}')

        except Exception as e:
            print(f'graph exception: {e}')

class GroupDef:
    from worker import make_pak
    
    group_id = None
    group_name = "group"
    # scrape_id = None
    # tiles_id = None
    # populate_id = None
    # move_id = None

    group_results = None
    # scrape_results = None
    # tiles_results = None
    # populate_results = None
    # move_results = None

    def __init__(self, name:str, paks: dict):
        self.paks = paks
        self.group_name = name

    def start(self):
        group = make_paks.s(self.paks).apply_async()
        self.group_id = group.id
        jobs.addGroupJob(name=self.group_name, task_id=group.id)
        return self.group_id
    
    def get_results(self, group_id, dump: bool = False):
        group = AsyncResult(group_id)
        result = group.result
        if dump:
            self.dump_group(group)
        data = {
            "task_id": group.id,
            "task_status": group.status,
            "task_result": result
        }
        return data

    def get_status(self):
        if self.group_results is None:
            return "NONE"
        
        # if (self.task_results.status == "SUCCESS") and \
        #     not ((self.scrape_results is None) or \
        #          (self.tiles_results is None) or \
        #          (self.populate_results is None) or \
        #          (self.move_results is None)):
        
        #     if (self.scrape_results['task_status'] == "SUCCESS") and \
        #         (self.tiles_results['task_status'] == "SUCCESS") and \
        #         (self.populate_results['task_status'] == "SUCCESS") and \
        #         (self.move_results['task_status'] == "SUCCESS"):
        #         return "COMPLETE"
            
        #     if (self.scrape_results['task_status'] == "FAILURE") or \
        #         (self.tiles_results['task_status'] == "FAILURE") or \
        #         (self.populate_results['task_status'] == "FAILURE") or \
        #         (self.move_results['task_status'] == "FAILURE"):
        #         return "FAILURE"
            
        return self.group_results.status
        

    def get_result_dict(self, dump: bool = False):
        self.group_results = AsyncResult(self.group_id)
        result = self.group_results.result
        status = self.group_results.status
        if dump:
            self.dump_group(self.group_results)

        # if (not self.task_results.info is None) and len(self.task_results.info) > 0:
        #     move = self.task_results.info.pop(0)
        #     print(f'move: {move}')
        #     self.move_id = move[0]
        #     populate = move[1].pop(0)
        #     print(f'populate: {populate}')
        #     self.populate_id = populate[0]
        #     tiles = populate[1].pop(0)
        #     print(f'tiles: {tiles}')
        #     self.tiles_id = tiles[0]
        #     scrape = tiles[1].pop(0)
        #     print(f'scrape: {scrape}')
        #     self.scrape_id = scrape[0]
        #     self.scrape_results = self.get_results(self.scrape_id)
        #     self.tiles_results = self.get_results(self.tiles_id)
        #     self.populate_results = self.get_results(self.populate_id)
        #     self.move_results = self.get_results(self.move_id)

        #     status = self.get_status()

        #     result = json.dumps({"scrape": self.scrape_results['task_status'],
        #               "tiles": self.tiles_results['task_status'],
        #               "populate": self.populate_results['task_status'],
        #               "move": self.move_results['task_status'],
        #               }, separators=(',', ':'))

        data = {
            "task_id": self.group_id,
            # "w": self.w,
            # "x": self.x,
            # "y": self.y,
            "task_status": status,
            "task_result": result
        }
        return data

    def dump_group(self, group):
        print(f'task[{group.id}')
        print(f'  > name: {group.name}')
        print(f'  > info: {group.info}')
        print(f'  > queue: {group.queue}')
        print(f'  > state: {group.state}')
        print(f'  > status: {group.status}')
        print(f'  > worker: {group.worker}')
        print(f'  > ignored: {group.ignored}')
        print(f'  > children: {group.children}')
        print(f'  > args: {group.args}')
        print(f'  > kwargs: {group.kwargs}')
        print(f'  > as_list: {group.as_list()}')
        try:
            if not group.children is None:
                graph : DependencyGraph = group.graph

                for parent, children  in graph.iteritems():
                    # print(f'* edges: {parent.id} -> {children}')
                    for child in children:
                        if  isinstance(child, GroupResult):
                            print(f'edge: {parent.id} -> {child.id}| GROUP [{child.results}]')
                        else:
                            print(f'edge: {parent.id} -> {child.id}| {child.name} [{child.kwargs}]')

                print(f'  > graph: {graph}')
                pipo = open("logs/group.dot", "w+")
                graph.to_dot(pipo)

                print(f'  > topsort:')
                for ob in graph.topsort():
                    print(f'  >        : {ob}')

        except Exception as e:
            print(f'graph exception: {e}')

pak_tasks = {}

class Inspector:

    methods = {'stats', 'active_queues', 'registered', 'scheduled', 
               'active', 'reserved', 'revoked', 'conf'}
    
    celery = Celery('celery', broker='redis://redis:6379/0')
    verbose = False

    def __init__(self, backgroundTasks : BackgroundTasks, verbose = False) -> None:
        self.backgroundTasks = backgroundTasks
        self.result = {}
        self.workers = collections.defaultdict(dict)
        self.verbose = verbose

    def on_update(self, workername, method, response):
        info = self.workers[workername]
        info[method] = response
        if (self. verbose): print(f'*** BACKGROUND INSPECT[{method}]: {response}')

    def actives(self, workername = None):
        inspect = self.celery.control.inspect(destination = None if workername is None else workername)
        actives = inspect.active()
        if (self. verbose): print(f'*** actives: {actives}')
        return actives

    def inspect(self, workername = None):
        features = []
        for method in self.methods:
            features.append(self.backgroundTasks.add_task(self._inspect, method, workername))
        if (self. verbose): print(f'*** INSPECT RESULT: {self.result}')
        return features

    def _inspect(self, method, workername):
        if (self. verbose): print(f'*** BACKGROUND INSPECT')
        inspect = self.celery.control.inspect(timeout = 2, destination = None if workername is None else workername)

        result = (
            getattr(inspect, method)()
            if method != 'active'
            else getattr(inspect, method)(safe=True)
        )

        if result is None or 'error' in result:
            print('BACKGROUND INSPECT ERROR')
            return
        for worker, response in result.items():
            if response is not None:
                self.backgroundTasks.add_task(self.on_update, worker, method, response)


@app.get("/")
def home(request: Request, backgroundTasks:BackgroundTasks):
    inspector = Inspector(backgroundTasks=backgroundTasks)

    inspection = inspector.inspect()
    
    return templates.TemplateResponse("home.html", context={"request": request,
                                      "inspection": inspection})


@app.get("/clear")
def home(request: Request, backgroundTasks:BackgroundTasks):
    jobs.clear()
    return RedirectResponse("/")


@app.get("/status")
def home(request: Request, backgroundTasks:BackgroundTasks):
    inspector = Inspector(backgroundTasks=backgroundTasks)

    inspection = inspector.inspect()
    jobs.updateJobs()
    job_list = jobs.get_jobs()
    print(f'job_list: {job_list}')
    for job in job_list:
        print(f'      id: {job.id()}')
        print(f'    name: {job.name()}')
        print(f'     pct: {job.pct}')
        print(f'  status: {job.status()}')
        print(f'  result: {job.result()}')

    return templates.TemplateResponse("status.html", 
                                      context={
                                          "request": request,
                                          "inspection": inspection,
                                          "jobs" : job_list
                                          })


@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = create_task.delay(int(task_type))
    data = {"task_id": task.id}

    # print(f'data: {data}')
    return JSONResponse(data)
    return JSONResponse(task_type)


@app.get("/tasks/{task_id}")
def get_status(task_id, backgroundTasks:BackgroundTasks):

    pak = pak_tasks[task_id]
    # inspector = Inspector(backgroundTasks=backgroundTasks)

    # result = inspector.actives()

    result = pak.get_result_dict(dump = True)

    return JSONResponse(result)


@app.get("/paks/{task_id}")
def get_status(task_id, backgroundTasks:BackgroundTasks):
    result = 'error'
    if task_id in pak_tasks:
        pak = pak_tasks[task_id]

        result = pak.get_result_dict(dump = True)

    # print(f'paks/{task_id} result: {result}')
    return JSONResponse(result)


@app.post("/group_paks")
def app_group_paks():
    paks = {}
    for w in {1}:
        paks[w] = {}
        for x in range(w*2):
            paks[w][x] = []
            for y in range(w*2):
                paks[w][x].append(y)
    # task = make_paks.delay(paks)
    group = GroupDef(name='MakePaks', paks=paks)
    task = group.start()
    pak_tasks[task] = group
    result = group.get_result_dict(dump = True)
    return JSONResponse(result)


@app.post("/chord_paks")
def app_chord_paks():
    paks = {} 
    for w in {1}:
        paks[w] = {}
        for x in range(w):
            paks[w][x] = []
            for y in range(w+1):
                paks[w][x].append(y)
    task = chord_paks.delay(paks)
    result = {
        "paks": task.get(),
        "task_status": task.status,
        "task_result": task.result
    }
    return JSONResponse({"task_id": task.id})


@app.post("/make_pak")
def app_make_pak(payload = Body(...)):
    w = payload["w"]
    x = payload["x"]
    y = payload["y"]
    pak = PakDef(w,x,y)
    task = pak.start()
    pak_tasks[task] = pak
    result = pak.get_result_dict(dump = True)
    # print(f'make_pak result: {result}')
    return JSONResponse(result)
