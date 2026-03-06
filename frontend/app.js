const resumeInput = document.getElementById("resume");
const runButton = document.getElementById("run");
const statusEl = document.getElementById("status");
const tilesEl = document.getElementById("tiles");
const template = document.getElementById("tile-template");

const defaultBackendBase = `${window.location.protocol}//${window.location.hostname}:18000`;
let activeBackendBase = "";
let activeRunId = "";

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
    clone.querySelector(".company").textContent = `${tile.company} | run ${tile.run_id || activeRunId || "n/a"}`;
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
    const startPayload = await startWorkflowRun(form);
    activeRunId = startPayload.run_id;
    setStatus(`Run ${activeRunId} started. Discovering jobs...`);
    await pollWorkflowRun(activeRunId);
  } catch (error) {
    await reportFrontendFailure("workflow_exception", {
      message: error?.message || "Workflow failed",
      activeBackendBase,
    });
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

function logCandidates() {
  const candidates = backendCandidates();
  const direct = `${window.location.origin.replace(/\/$/, "")}/api/frontend/log`;
  const proxied = candidates.map(
    (candidate) => `${normalizeBackendBase(candidate)}/api/frontend/log`,
  );
  return [...new Set([direct, ...proxied])];
}

async function reportFrontendFailure(kind, details = {}) {
  const payload = {
    kind,
    details,
    url: window.location.href,
    userAgent: navigator.userAgent,
    ts: new Date().toISOString(),
  };
  const body = JSON.stringify(payload);

  for (const endpoint of logCandidates()) {
    try {
      await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        keepalive: true,
      });
      return;
    } catch (_error) {
      // Try next candidate.
    }
  }
}

async function startWorkflowRun(form) {
  const candidates = backendCandidates();
  const failures = [];
  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}/api/workflow/runs`, {
        method: "POST",
        body: form,
      });
      if (response.ok || response.status >= 400) {
        activeBackendBase = normalizeBackendBase(candidate);
        if (!response.ok) {
          const details = await response.json().catch(() => ({}));
          throw new Error(details.detail || "Workflow request failed.");
        }
        return response.json();
      }
    } catch (error) {
      failures.push({
        candidate,
        message: error?.message || "request failed",
      });
    }
  }

  await reportFrontendFailure("backend_unreachable", { failures, candidates });
  throw new Error("Could not reach backend through proxy.");
}

async function fetchRunStatus(runId) {
  const candidates = backendCandidates();
  let lastError = null;
  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}/api/workflow/runs/${encodeURIComponent(runId)}`);
      if (response.ok) {
        activeBackendBase = normalizeBackendBase(candidate);
        return response.json();
      }
      const details = await response.json().catch(() => ({}));
      lastError = new Error(details.detail || `Status request failed (${response.status}).`);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("Could not reach backend status endpoint.");
}

async function pollWorkflowRun(runId) {
  const startedAt = Date.now();
  const timeoutMs = 20 * 60 * 1000;

  while (Date.now() - startedAt < timeoutMs) {
    const statusPayload = await fetchRunStatus(runId);
    const tiles = statusPayload.tiles || [];
    renderTiles(tiles);

    const status = statusPayload.status || "running";
    const current = Number(statusPayload.progress_current || 0);
    const total = Number(statusPayload.progress_total || 0);
    const company = statusPayload.progress_company || "";
    const diagnostics = statusPayload.diagnostics || null;

    if (status === "completed" || status === "failed") {
      const verificationSummary = Object.entries(diagnostics?.verification || {})
        .map(([stage, result]) => `${stage}:${result.ok ? "ok" : "failed"}`)
        .join(" | ");
      setStatus(`Run ${runId} ${status}. ${verificationSummary}`);
      return;
    }

    const progressText = total > 0 ? `${current}/${total}` : `${current}`;
    const companyText = company ? ` (${company})` : "";
    setStatus(`Run ${runId} ${status}. Discovery progress ${progressText}${companyText}.`);
    // Poll interval.
    await new Promise((resolve) => {
      window.setTimeout(resolve, 1500);
    });
  }
  throw new Error(`Run ${runId} timed out while polling.`);
}

window.addEventListener("error", (event) => {
  reportFrontendFailure("window_error", {
    message: event.message,
    source: event.filename,
    line: event.lineno,
    column: event.colno,
  });
});

window.addEventListener("unhandledrejection", (event) => {
  const reason = event.reason;
  reportFrontendFailure("unhandled_rejection", {
    message:
      typeof reason === "string"
        ? reason
        : (reason && reason.message) || "unhandled promise rejection",
  });
});
