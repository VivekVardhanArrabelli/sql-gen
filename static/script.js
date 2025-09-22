
const encodeURiComponent = window.encodeURIComponent;

document.getElementById("chat-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("user-input");
    const message = input.value.trim();
    if (!message) return;

    const messagesDiv = document.getElementById("messages");
    messagesDiv.innerHTML += `<p><strong>You:</strong> ${message}</p>`;

    // First, generate SQL
    const sqlResponse = await fetch("/generate_sql", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: `query=${encodeURiComponent(message)}`
    });
    const sqlData = await sqlResponse.json();
    if (sqlData.error) {
        messagesDiv.innerHTML += `<p><strong>Bot:</strong> ${sqlData.error}</p>`;
        return;
    }

    // Then execute SQL
    const execResponse = await fetch("/execute_sql", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: `sql=${encodeURiComponent(sqlData.sql)}&action=${encodeURiComponent(sqlData.action)}&date_field=${encodeURiComponent(sqlData.date_field)}&csv=${encodeURiComponent(sqlData.csv)}`
    });

    if (execResponse.headers.get("content-type") === "text/csv") {
        const blob = await execResponse.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = execResponse.headers.get("content-disposition").split("filename=")[1];
        a.click();
        messagesDiv.innerHTML += `<p><strong>Bot:</strong> CSV downloaded with results.</p>`;
    } else {
        const data = await execResponse.json();
        if (data.query) {
            messagesDiv.innerHTML += `<p class="query">Query: ${data.query}</p>`;
        }
        messagesDiv.innerHTML += `<p><strong>Bot:</strong> ${data.message || data.error}</p>`;
    }

    input.value = "";
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
});


