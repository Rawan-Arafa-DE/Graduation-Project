// Make sure Chart.js is loaded globally (usually in base.html)
// This file uses data injected from Flask: monthlyData, outcomeData, ageData

document.addEventListener("DOMContentLoaded", function () {

    // =========================
    // 1. Monthly Patient Registrations (LINE CHART)
    // =========================
    const ctxPatients = document.getElementById('patientsChart');

    if (ctxPatients && typeof Chart !== "undefined") {
        const labels = monthlyData.length
            ? monthlyData.map(p => p.label)
            : ["No Data"];

        const values = monthlyData.length
            ? monthlyData.map(p => p.value)
            : [0];

        new Chart(ctxPatients, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Patients',
                    data: values,
                    fill: true,
                    tension: 0.4,
                    borderColor: 'rgba(50, 150, 255, 1)',   // نفس الثيم القديم 💙
                    backgroundColor: 'rgba(50, 150, 255, 0.15)'
                }]
            },
            options: {
                scales: {
                    y: { min: 700 }  // يبدأ من 700 زي الصورة الأولى
                }
            }
        });
    }


    // =========================
    // 2. IVF Outcomes (PIE CHART)
    // =========================
    const ctxStages = document.getElementById('stagesChart');

    if (ctxStages && typeof Chart !== "undefined") {
        const labels = outcomeData.length
            ? outcomeData.map(p => p.label)
            : ["No Data"];

        const values = outcomeData.length
            ? outcomeData.map(p => p.value)
            : [0];

        new Chart(ctxStages, {
            type: 'pie',
            data: {
                labels: labels,
                datasets: [{
                    data: values
                }]
            },
            options: {
                plugins: {
                    legend: { position: 'bottom' }
                }
            }
        });
    }


    // =========================
    // 3. Patient Age Distribution (BAR CHART)
    // =========================
    const ctxAge = document.getElementById('ageChart');

    if (ctxAge && typeof Chart !== "undefined") {
        const labels = ageData.length ? ageData.map(p => p.label) : ["No Data"];
        const values = ageData.length ? ageData.map(p => p.value) : [1];

        new Chart(ctxAge, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Patients',
                    data: values,
                    borderWidth: 1
                }]
            },
            options: {
                scales: {
                    y: { beginAtZero: true }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });
    }

});
