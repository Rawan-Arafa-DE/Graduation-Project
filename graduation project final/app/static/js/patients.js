document.addEventListener("DOMContentLoaded", () => {
  const searchInput = document.getElementById("searchInput");
  const statusFilter = document.getElementById("statusFilter");
  const rows = document.querySelectorAll("#patientTable tbody tr");
  const addBtn = document.querySelector(".add-patient-btn");

  // 🔎 Real-time search
  if (searchInput) {
    searchInput.addEventListener("keyup", function () {
      const searchValue = this.value.toLowerCase();

      rows.forEach(row => {
        const text = row.textContent.toLowerCase();
        row.style.display = text.includes(searchValue) ? "" : "none";
      });
    });
  }

  // 🎯 Filter by Status (based on text in Status column)
  if (statusFilter) {
    statusFilter.addEventListener("change", function () {
      const selected = this.value.toLowerCase();

      rows.forEach(row => {
        const statusText = row
          .querySelector("td:nth-child(3)")
          .textContent.toLowerCase();   // النص جوه عمود الـ Status

        if (selected === "" || statusText.includes(selected)) {
          row.style.display = "";
        } else {
          row.style.display = "none";
        }
      });
    });
  }

  // Pointer effect on Add Patient button
  if (addBtn) {
    addBtn.addEventListener("mouseover", () => {
      addBtn.style.cursor = "pointer";
    });
  }
});
