const resumeInput = document.getElementById("resume");
const runButton = document.getElementById("run");
const statusEl = document.getElementById("status");
const tilesEl = document.getElementById("tiles");
const template = document.getElementById("tile-template");

const defaultBackendBase = `${window.location.protocol}//${window.location.hostname}:18000`;
let activeBackendBase = "";

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#8a1c1c" : "#244f31";
}

function normalizeBackendBase(candidate) {
  if (candidate.startsWith("http")) {
    return candidate.replace(/\/$/, "");
  }
  return `${window.location.origin}${candidate}`.replace(/\/$/, "");
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
    if (tile.generated_resume_link) {
      const base = activeBackendBase || window.location.origin.replace(/\/$/, "");
      resumeLink.href = `${base}${tile.generated_resume_link}`;
      resumeLink.setAttribute("download", "");
    } else {
      resumeLink.removeAttribute("href");
      resumeLink.textContent = "Resume unavailable";
    }

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
    const response = await postWorkflowWithFallback(form);

    if (!response.ok) {
      const details = await response.json().catch(() => ({}));
      throw new Error(details.detail || "Workflow request failed.");
    }

    const payload = await response.json();
    renderTiles(payload.tiles || []);

    const verificationSummary = Object.entries(payload.diagnostics?.verification || {})
      .map(([stage, result]) => `${stage}:${result.ok ? "ok" : "failed"}`)
      .join(" | ");

    const state = payload.diagnostics?.failed ? "failed" : "completed";
    setStatus(`Workflow ${state}. ${verificationSummary}`);
  } catch (error) {
    setStatus(error.message || "Workflow failed. Check backend URL and connectivity.", true);
  } finally {
    runButton.disabled = false;
  }
}

runButton.addEventListener("click", runWorkflow);

function backendCandidates() {
  const origin = window.location.origin.replace(/\/$/, "");
  const path = window.location.pathname;
  const proxyMarker = "/proxy/8090";
  const prefix = path.includes(proxyMarker) ? path.split(proxyMarker)[0] : "";

  const candidates = [
    `${prefix}/proxy/18000`,
    "/proxy/18000",
    `${origin}${prefix}/proxy/18000`,
    `${origin}/proxy/18000`,
    defaultBackendBase,
  ];
  return [...new Set(candidates.map((value) => value.replace(/\/$/, "")))];
}

async function postWorkflowWithFallback(form) {
  const candidates = backendCandidates();
  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}/api/workflow/run`, {
        method: "POST",
        body: form,
      });
      if (response.ok || response.status >= 400) {
        activeBackendBase = normalizeBackendBase(candidate);
        return response;
      }
    } catch (_error) {
      // Try next candidate.
    }
  }

  throw new Error("Could not reach backend through proxy.");
}
