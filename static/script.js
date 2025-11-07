// Trigger schema loading when database changes
document.getElementById("dbSelect").addEventListener("change", async function () {
  const dbName = this.value;
  const resultsDiv = document.getElementById("results");
  resultsDiv.innerHTML = `<p><em>Loading schema for ${dbName}...</em></p>`;

  try {
    const res = await fetch("/load_schema", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ database: dbName })
    });

    const data = await res.json();
    if (res.ok) {
      resultsDiv.innerHTML = `<p>${data.status}</p>`;
    } else {
      resultsDiv.innerHTML = `<p class="error">${data.error}</p>`;
    }
  } catch (err) {
    resultsDiv.innerHTML = `<p class="error">Schema load failed: ${err.message}</p>`;
  }
});

// Analyze prompt and display results
async function analyze() {
  const prompt = document.getElementById("userPrompt").value.trim();
  const dbName = document.getElementById("dbSelect").value;
  const resultsDiv = document.getElementById("results");

  if (!prompt) {
    resultsDiv.innerHTML = `<p class="error">Please enter a prompt.</p>`;
    return;
  }

  resultsDiv.innerHTML = "<p><em>Analyzing... please wait.</em></p>";

  try {
    const response = await fetch("/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt, database: dbName })
    });

    const text = await response.text();
    let data;

    try {
      data = JSON.parse(text);
    } catch (err) {
      throw new Error(`Response is not valid JSON:\n${text}`);
    }

    if (response.ok) {
      let html = `<h2>SQL Query</h2><pre>${data.sql}</pre>`;

      if (data.rows.length > 0) {
        html += `<h2>Query Results</h2><table border="1"><thead><tr>`;
        data.columns.forEach(col => html += `<th>${col}</th>`);
        html += `</tr></thead><tbody>`;
        data.rows.forEach(row => {
          html += `<tr>${row.map(val => `<td>${val}</td>`).join("")}</tr>`;
        });
        html += `</tbody></table>`;
      }

      html += `<h2>Insight</h2><p>${data.insight}</p>`;
      resultsDiv.innerHTML = html;
    } else {
      resultsDiv.innerHTML = `<p class="error">Error: ${data.error || "Unexpected server error"}</p>`;
    }
  } catch (err) {
    resultsDiv.innerHTML = `<p class="error">Request failed: ${err.message}</p>`;
  }
}
