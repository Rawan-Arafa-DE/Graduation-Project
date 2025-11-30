function showTab(tab) {
    document.querySelectorAll('.settings-section').forEach(div => div.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

    document.getElementById(tab).classList.add('active');
    event.target.classList.add('active');
}
