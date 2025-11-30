
const searchInput = document.getElementById("searchInput");
const tableRows = document.querySelectorAll("#patientTable tr");

searchInput.addEventListener("keyup", () => {
  const value = searchInput.value.toLowerCase();
  tableRows.forEach(row => {
    row.style.display = row.textContent.toLowerCase().includes(value)
      ? ""
      : "none";
  });
});
