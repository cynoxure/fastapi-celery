// custom javascript

(function() {
  console.log('Sanity Check!');
})();

function handleClick(type) {
  console.log(type);
  if (type < 4) {
    console.log('task');
    fetch('/tasks', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ type: type }),
    })
    .then(response => response.json())
    .then(data => {
      getTaskStatus(data.task_id)
    })
  } else if (type == 4) {
    console.log('make_pak');
    fetch('/make_pak', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ w: 1, x: 2, y: 3 }),
    })
    .then(response => response.json())
    .then(data => {
      getPakStatus(data.task_id)
    })
  } else if (type == 5) {
    console.log('group_paks');
    fetch('/group_paks', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ }),
    })
    .then(response => response.json())
    .then(data => {
      getGroupStatus(data.task_id)
    })
  } else if (type == 6) {
    console.log('chord_paks');
    fetch('/chord_paks', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ }),
    }
    )
    .then(response => response.json())
    .then(data => {
      getTaskStatus(data.task_id)
    })
  }
}

function getTaskStatus(taskID) {
  fetch(`/tasks/${taskID}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json'
    },
  })
  .then(response => response.json())
  .then(res => {
    console.log(res)
    const html = `
      <tr>
        <td>${taskID}</td>
        <td>${res.task_status}</td>
        <td>${res.task_result}</td>
      </tr>`;
    const newRow = document.getElementById('tasks').insertRow(0);
    newRow.innerHTML = html;

    const taskStatus = res.task_status;
    if (taskStatus === 'SUCCESS' || taskStatus === 'FAILURE') return false;
    setTimeout(function() {
      getTaskStatus(res.task_id);
    }, 1000);
  })
  .catch(err => console.log(err));
}
  
function getPakStatus(taskID) {
  fetch(`/paks/${taskID}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json'
    },
  })
  .then(response => response.json())
  .then(res => {
    console.log(res);
    const html = `
      <tr>
        <td>${taskID}</td>
        <td>${res.w}</td>
        <td>${res.x}</td>
        <td>${res.y}</td>
        <td>${res.task_status}</td>
        <td>${res.task_result}</td>
      </tr>`;
    const newRow = document.getElementById('paks').insertRow(0);
    newRow.innerHTML = html;

    const taskStatus = res.task_status;
    if (taskStatus === 'COMPLETE' || taskStatus === 'FAILURE') return false;
    setTimeout(function() {
      getPakStatus(res.task_id);
    }, 1000);
  })
  .catch(err => console.log(err));
}
  
function getGroupStatus(taskID) {
  fetch(`/paks/${taskID}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json'
    },
  })
  .then(response => response.json())
  .then(res => {
    console.log(res);
    const html = `
      <tr>
        <td>${taskID}</td>
        <td>${res.w}</td>
        <td>${res.x}</td>
        <td>${res.y}</td>
        <td>${res.task_status}</td>
        <td>${res.task_result}</td>
      </tr>`;
    const newRow = document.getElementById('paks').insertRow(0);
    newRow.innerHTML = html;

    const taskStatus = res.task_status;
    if (taskStatus === 'COMPLETE' || taskStatus === 'FAILURE') return false;
    setTimeout(function() {
      getPakStatus(res.task_id);
    }, 1000);
  })
  .catch(err => console.log(err));
}
