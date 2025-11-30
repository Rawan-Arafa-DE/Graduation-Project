document.addEventListener("DOMContentLoaded", function () {
    const form = document.querySelector(".patient-form");
    form.addEventListener("submit", function (e) {
        const confirmSubmit = confirm("Are you sure you want to save these lab results?");
        if (!confirmSubmit) e.preventDefault();
    });
});
