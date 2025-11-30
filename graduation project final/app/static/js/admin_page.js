// Hover effect for Add User Button
document.addEventListener("DOMContentLoaded", () => {
    const addBtn = document.querySelector(".add-user-btn");
    if (addBtn) {
        addBtn.addEventListener("mouseover", () => {
            addBtn.style.cursor = "pointer";
        });
    }
});
// Animation when hovering rows
document.addEventListener("DOMContentLoaded", () => {
    const rows = document.querySelectorAll(".admin-table tbody tr");
    rows.forEach(row => {
        row.addEventListener("mouseover", () => row.style.background = "#fafafa");
        row.addEventListener("mouseout", () => row.style.background = "white");
    });
});
// Fade in animation for cards
document.querySelectorAll('.admin-card').forEach(card => {
    card.style.opacity = 0;
    setTimeout(() => {
        card.style.transition = "0.4s ease-in-out";
        card.style.opacity = 1;
    }, 200);
});
