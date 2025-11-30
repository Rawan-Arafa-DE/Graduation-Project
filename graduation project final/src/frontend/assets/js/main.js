// Chart 1 - Line Chart
const ctx1 = document.getElementById('chart1');
if (ctx1) {
  new Chart(ctx1, {
    type: 'line',
    data: {
      labels: ['Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr'],
      datasets: [{
        label: 'Patients',
        data: [130, 150, 165, 180, 190, 230],
        borderWidth: 2,
        borderColor: '#f48acb',
        tension: 0.3
      }]
    }
  });
}

// Chart 2 - Bar Chart
const ctx2 = document.getElementById('chart2');
if (ctx2) {
  new Chart(ctx2, {
    type: 'bar',
    data: {
      labels: ['Stimulation', 'Fertilization', 'Transfer', 'Pregnancy Test'],
      datasets: [{
        label: 'Count',
        data: [80, 50, 40, 28],
        backgroundColor: '#9BBDF9'
      }]
    }
  });
}
