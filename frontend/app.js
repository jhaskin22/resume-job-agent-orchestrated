const resumeInput = document.getElementById("resume");
const runButton = document.getElementById("run");
const resetButton = document.getElementById("reset");
const statusEl = document.getElementById("status");
const tilesEl = document.getElementById("tiles");
const template = document.getElementById("tile-template");

const defaultBackendBase = `${window.location.protocol}//${window.location.hostname}:18000`;
const RUN_STATE_KEY = "resume_job_agent_run_state_v1";
const LAST_TILES_KEY = "resume_job_agent_last_tiles_v1";
let activeBackendBase = "";
let activeRunId = "";
let activePollPromise = null;

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#8a1c1c" : "#244f31";
}

function saveRunState(extra = {}) {
  const payload = {
    runId: activeRunId,
    backendBase: activeBackendBase,
    ...extra,
  };
  try {
    window.localStorage.setItem(RUN_STATE_KEY, JSON.stringify(payload));
  } catch (_error) {
    // Ignore storage issues.
  }
}

function readRunState() {
  try {
    const raw = window.localStorage.getItem(RUN_STATE_KEY);
    if (!raw) {
      return null;
    }
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function clearRunState() {
  activeRunId = "";
  activeBackendBase = "";
  activePollPromise = null;
  try {
    window.localStorage.removeItem(RUN_STATE_KEY);
    window.localStorage.removeItem(LAST_TILES_KEY);
  } catch (_error) {
    // Ignore storage issues.
  }
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
    const metaEl = clone.querySelector(".meta");

    const workBadge = clone.querySelector(".work-badge");
    const workBadgeConfig = workBadgeState(tile);
    workBadge.textContent = workBadgeConfig.label;
    workBadge.classList.add(workBadgeConfig.className);

    const normalizedLocation = String(tile.location || "").trim();
    if (shouldShowLocationMeta(normalizedLocation, workBadgeConfig.label)) {
      metaEl.textContent = normalizedLocation;
      metaEl.style.display = "";
    } else {
      metaEl.textContent = "";
      metaEl.style.display = "none";
    }

    const salaryBadge = clone.querySelector(".salary-badge");
    const salaryBadgeConfig = salaryBadgeState(tile);
    salaryBadge.textContent = salaryBadgeConfig.label;
    salaryBadge.classList.add(salaryBadgeConfig.className);

    clone.querySelector(".scores").textContent = `Match ${tile.match_score.toFixed(1)} | ATS ${tile.ats_score.toFixed(1)} | Resume ${tile.resume_alignment.toFixed(1)}`;

    const jobLink = clone.querySelector(".job-link");
    jobLink.href = tile.job_link;

    const resumeLink = clone.querySelector(".resume-link");
    if (tile.generated_resume_link) {
      const base = activeBackendBase || window.location.origin.replace(/\/$/, "");
      resumeLink.href = `${base}${tile.generated_resume_link}`;
      resumeLink.setAttribute("download", "");
      resumeLink.removeAttribute("data-action");
      resumeLink.removeAttribute("data-job-link");
      resumeLink.textContent = "Download Resume";
    } else {
      resumeLink.href = "#";
      resumeLink.removeAttribute("download");
      resumeLink.dataset.action = "generate";
      resumeLink.dataset.jobLink = tile.job_link;
      resumeLink.textContent = "Generate Resume";
    }

    tilesEl.appendChild(clone);
  });
}

function saveLastTiles(tiles, runId = "") {
  try {
    const payload = {
      runId: String(runId || activeRunId || ""),
      tiles: Array.isArray(tiles) ? tiles : [],
      ts: new Date().toISOString(),
    };
    window.localStorage.setItem(LAST_TILES_KEY, JSON.stringify(payload));
  } catch (_error) {
    // Ignore storage issues.
  }
}

function readLastTiles() {
  try {
    const raw = window.localStorage.getItem(LAST_TILES_KEY);
    if (!raw) {
      return null;
    }
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function shouldShowLocationMeta(locationLabel, workLabel) {
  if (!locationLabel) {
    return false;
  }
  const loc = locationLabel.toLowerCase();
  const work = String(workLabel || "").toLowerCase();
  if (loc === work) {
    return false;
  }
  const generic = new Set(["remote", "dfw remote", "dfw hybrid", "hybrid", "in person"]);
  if (generic.has(loc) && (loc === work || work.includes(loc) || loc.includes(work))) {
    return false;
  }
  return true;
}

function workBadgeState(tile) {
  const location = String(tile.location || "").toLowerCase();
  const workType = String(tile.work_type || "").toLowerCase();
  const dfwCities = ["dallas", "fort worth", "plano", "richardson", "arlington", "hurst"];
  const isDfw = location.includes("dfw") || dfwCities.some((city) => location.includes(city));

  if (isDfw && workType === "remote") {
    return {
      label: location.includes("remote") ? String(tile.location || "DFW Remote") : "DFW Remote",
      className: "badge-blue",
    };
  }
  if (isDfw && workType === "hybrid") {
    return {
      label: location.includes("hybrid") ? String(tile.location || "DFW Hybrid") : "DFW Hybrid",
      className: "badge-blue",
    };
  }
  if (workType === "remote") {
    return { label: "Remote", className: "badge-green" };
  }
  if (workType === "hybrid") {
    return { label: "Hybrid", className: "badge-black" };
  }
  if (workType === "onsite") {
    return { label: "In Person", className: "badge-black" };
  }
  return { label: "In Person", className: "badge-black" };
}

function salaryBadgeState(tile) {
  const salary = String(tile.salary || "").trim();
  if (salary) {
    return { label: salary, className: "badge-green" };
  }
  return { label: "No Salary", className: "badge-black" };
}

async function generateResume(jobLink) {
  if (!activeRunId) {
    throw new Error("No active run available for resume generation.");
  }
  const response = await fetch(
    `${activeBackendBase}/api/workflow/runs/${encodeURIComponent(activeRunId)}/resume`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job_link: jobLink }),
    },
  );
  if (!response.ok) {
    const details = await response.json().catch(() => ({}));
    throw new Error(details.detail || "Resume generation failed.");
  }
  return response.json();
}

async function runWorkflow() {
  if (activePollPromise) {
    setStatus(`Run ${activeRunId} is already in progress. Use Reset to clear it.`);
    return;
  }
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
    saveRunState({ status: "running" });
    setStatus(`Run ${activeRunId} started. Discovering jobs...`);
    activePollPromise = pollWorkflowRun(activeRunId);
    await activePollPromise;
  } catch (error) {
    await reportFrontendFailure("workflow_exception", {
      message: error?.message || "Workflow failed",
      activeBackendBase,
    });
    setStatus(error.message || "Workflow failed. Check backend URL and connectivity.", true);
  } finally {
    activePollPromise = null;
    runButton.disabled = false;
  }
}

runButton.addEventListener("click", runWorkflow);
tilesEl.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const link = target.closest(".resume-link[data-action='generate']");
  if (!(link instanceof HTMLAnchorElement)) {
    return;
  }
  event.preventDefault();
  if (!activeBackendBase) {
    setStatus("Backend is not connected yet.", true);
    return;
  }
  const jobLink = link.dataset.jobLink || "";
  if (!jobLink) {
    setStatus("Missing job link for resume generation.", true);
    return;
  }

  const originalText = link.textContent;
  link.textContent = "Generating...";
  link.setAttribute("aria-disabled", "true");
  try {
    await generateResume(jobLink);
    const statusPayload = await fetchRunStatus(activeRunId);
    renderTiles(statusPayload.tiles || []);
    setStatus(`Generated resume for ${jobLink}.`);
  } catch (error) {
    setStatus(error?.message || "Resume generation failed.", true);
    link.textContent = originalText || "Generate Resume";
  } finally {
    link.removeAttribute("aria-disabled");
  }
});
resetButton.addEventListener("click", () => {
  clearRunState();
  renderTiles([]);
  resumeInput.value = "";
  setStatus("Run state reset. You can start a new job search.");
});

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
        saveRunState({ status: "starting" });
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
  let sawNotFound = false;
  for (const candidate of candidates) {
    try {
      const response = await fetch(`${candidate}/api/workflow/runs/${encodeURIComponent(runId)}`);
      if (response.ok) {
        activeBackendBase = normalizeBackendBase(candidate);
        saveRunState({ status: "running" });
        return response.json();
      }
      const details = await response.json().catch(() => ({}));
      if (response.status === 404) {
        sawNotFound = true;
        continue;
      }
      lastError = new Error(details.detail || `Status request failed (${response.status}).`);
    } catch (error) {
      lastError = error;
    }
  }
  if (sawNotFound) {
    const error = new Error("Run not found.");
    error.code = "RUN_NOT_FOUND";
    throw error;
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
    const stage = statusPayload.current_stage || "";
    const stageElapsed = Number(statusPayload.stage_elapsed_seconds || 0);
    saveRunState({ status, stage, stageElapsed });

    if (status === "completed" || status === "failed") {
      const verificationSummary = Object.entries(diagnostics?.verification || {})
        .map(([stage, result]) => `${stage}:${result.ok ? "ok" : "failed"}`)
        .join(" | ");
      setStatus(`Run ${runId} ${status}. ${verificationSummary}`);
      saveLastTiles(tiles, runId);
      if (status === "completed") {
        saveRunState({ status: "completed" });
      }
      return;
    }

    const progressText = total > 0 ? `${current}/${total}` : `${current}`;
    const companyText = company ? ` (${company})` : "";
    const stageText = stage ? ` Stage ${stage} (${stageElapsed}s).` : "";
    setStatus(`Run ${runId} ${status}. Discovery progress ${progressText}${companyText}.${stageText}`);
    // Poll interval.
    await new Promise((resolve) => {
      window.setTimeout(resolve, 1500);
    });
  }
  throw new Error(`Run ${runId} timed out while polling; refresh will resume this run.`);
}

async function resumeRunFromStorage() {
  const persisted = readRunState();
  const lastTilesPayload = readLastTiles();
  if ((!persisted || !persisted.runId) && lastTilesPayload?.tiles?.length) {
    renderTiles(lastTilesPayload.tiles);
    setStatus(`Showing last results from run ${lastTilesPayload.runId || "n/a"}.`);
    return;
  }
  if (!persisted || !persisted.runId) {
    return;
  }
  activeRunId = String(persisted.runId);
  activeBackendBase = String(persisted.backendBase || "");
  runButton.disabled = true;
  try {
    // Validate saved run before showing resume UI.
    const initialStatus = await fetchRunStatus(activeRunId);
    const status = String(initialStatus?.status || "");
    if (status === "completed" || status === "failed") {
      renderTiles(initialStatus.tiles || []);
      saveLastTiles(initialStatus.tiles || [], activeRunId);
      saveRunState({ status, runId: activeRunId });
      setStatus(`Previous run ${activeRunId} already ${status}.`);
      return;
    }
    setStatus(`Resuming run ${activeRunId} after refresh...`);
    activePollPromise = pollWorkflowRun(activeRunId);
    await activePollPromise;
  } catch (error) {
    if (error?.code === "RUN_NOT_FOUND" || String(error?.message || "").includes("Run not found")) {
      const fallback = readLastTiles();
      if (fallback?.tiles?.length) {
        renderTiles(fallback.tiles);
        clearRunState();
        setStatus(
          `Previous run expired after restart. Showing saved results from run ${fallback.runId || "n/a"}.`,
        );
        return;
      }
      clearRunState();
      renderTiles([]);
      setStatus("Previous run expired after restart. Start a new job search.");
      return;
    }
    setStatus(error?.message || "Could not resume existing run.", true);
  } finally {
    activePollPromise = null;
    runButton.disabled = false;
  }
}

void resumeRunFromStorage();

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
