from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from celery.result import AsyncResult
from worker import create_task, make_pak, make_paks, chord_paks


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")



@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("home.html", context={"request": request})


@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = create_task.delay(int(task_type))
    return JSONResponse({"task_id": task.id})
    return JSONResponse(task_type)


@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)


@app.get("/group_paks")
def app_group_paks():
    paks = {}
    for w in {1,2}:
        paks[w] = {}
        for x in range(w*4):
            paks[w][x] = []
            for y in range(w*4+1):
                paks[w][x].append(y)
    task = make_paks.delay(paks)
    result = {
        "paks": paks,
        "task_status": task.status,
        "task_result": task.result
    }
    return JSONResponse(result)


@app.get("/chord_paks")
def app_chord_paks():
    paks = {}
    for w in {1,2}:
        paks[w] = {}
        for x in range(w*4):
            paks[w][x] = []
            for y in range(w*4+1):
                paks[w][x].append(y)
    task = make_paks.delay(paks)
    result = {
        "paks": task.get(),
        "task_status": task.status,
        "task_result": task.result
    }
    return JSONResponse(result)


@app.get("/make_pak/{w}/{x}/{y}")
def app_make_pak(w,x,y):
    vals = (w,x,y)
    task = make_pak.delay(*vals)
    result = {
        "w": w,
        "x": x,
        "y": y,
        "task_status": task.status,
        "task_result": task.result
    }
    return JSONResponse(result)