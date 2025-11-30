// ==========================
// Patients Line Chart
// ==========================
const patientsCtx = document.getElementById("patientsChart");

if (patientsCtx) {
    new Chart(patientsCtx, {
        type: "line",
        data: {
            labels: ["Nov", "Dec", "Jan", "Feb", "Mar", "Apr"],
            datasets: [{
                label: "Patients",
                data: [120, 145, 160, 180, 190, 215],
                borderWidth: 2,
                fill: true,
                borderColor: "rgba(255, 105, 180, 1)", // Pink
                backgroundColor: "rgba(255, 105, 180, 0.15)", 
                tension: 0.4,
                pointRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
        }
    });
}


// ==========================
// IVF Stages Bar Chart
// ==========================
const stagesCtx = document.getElementById("stagesChart");

if (stagesCtx) {
    new Chart(stagesCtx, {
        type: "bar",
        data: {
            labels: ["Stimulation", "Fertilization", "Transfer", "Pregnancy Test"],
            datasets: [{
                label: "Count",
                data: [80, 60, 50, 30],
                backgroundColor: "rgba(173, 216, 230, 0.7)",   // Light Blue
                borderColor: "rgba(173, 216, 230, 1)",
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
        }
    });
}


// ==========================
// Age Distribution Doughnut Chart
// ==========================
const ageCtx = document.getElementById("ageChart");

if (ageCtx) {
    new Chart(ageCtx, {
        type: "doughnut",
        data: {
            labels: ["Under 30", "30-34", "35-39", "40-44", "45+"],
            datasets: [{
                data: [20, 30, 25, 15, 10],
                backgroundColor: [
                    "#FFB3C1", "#FFDCA9", "#A7E9AF", "#A5D8FF", "#E0BBE4"
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: "bottom" }
            }
        }
    });
}
