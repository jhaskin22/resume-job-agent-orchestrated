const resumeInput = document.getElementById("resume");
const runButton = document.getElementById("run");
const statusEl = document.getElementById("status");
const tilesEl = document.getElementById("tiles");
const template = document.getElementById("tile-template");

const backendBase = `${window.location.protocol}//${window.location.hostname}:18000`;

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#8a1c1c" : "#244f31";
}

function renderTiles(tiles) {
  tilesEl.innerHTML = "";

  if (!tiles.length) {
    tilesEl.innerHTML = "<p>No tiles returned.</p>";
    return;
  }

  tiles.forEach((tile) => {
    const clone = template.content.cloneNode(true);
    clone.querySelector(".title").textContent = tile.title;
    clone.querySelector(".company").textContent = tile.company;
    clone.querySelector(".meta").textContent = `${tile.location} | ${tile.work_type} | ${tile.salary || "Salary not listed"}`;
    clone.querySelector(".scores").textContent = `Match ${tile.match_score.toFixed(1)} | ATS ${tile.ats_score.toFixed(1)} | Resume ${tile.resume_alignment.toFixed(1)}`;
    clone.querySelector(".summary").textContent = tile.summary;

    const jobLink = clone.querySelector(".job-link");
    jobLink.href = tile.job_link;

    const resumeLink = clone.querySelector(".resume-link");
    resumeLink.href = `${backendBase}${tile.generated_resume_link}`;

    tilesEl.appendChild(clone);
  });
}

async function runWorkflow() {
  const file = resumeInput.files?.[0];
  if (!file) {
    setStatus("Choose a PDF or DOCX resume first.", true);
    return;
  }

  const form = new FormData();
  form.append("resume", file);

  setStatus("Running workflow...");
  runButton.disabled = true;

  try {
    const response = await fetch(`${backendBase}/api/workflow/run`, {
      method: "POST",
      body: form,
    });

    if (!response.ok) {
      const details = await response.json().catch(() => ({}));
      throw new Error(details.detail || "Workflow request failed.");
    }

    const payload = await response.json();
    renderTiles(payload.tiles || []);

    const verificationSummary = Object.entries(payload.diagnostics?.verification || {})
      .map(([stage, result]) => `${stage}:${result.ok ? "ok" : "failed"}`)
      .join(" | ");

    setStatus(`Workflow completed. ${verificationSummary}`);
  } catch (error) {
    setStatus(error.message || "Workflow failed.", true);
  } finally {
    runButton.disabled = false;
  }
}

runButton.addEventListener("click", runWorkflow);
