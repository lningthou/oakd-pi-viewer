/* ── OAK-D Recording Viewer ── */

(() => {
  // DOM refs
  const $ = (sel) => document.querySelector(sel);
  const fileTree = $("#file-tree");
  const emptyState = $("#empty-state");
  const recordingView = $("#recording-view");
  const progressOverlay = $("#progress-overlay");
  const progressStage = $("#progress-stage");
  const progressFill = $("#progress-fill");
  const progressDetail = $("#progress-detail");
  const videoRgb = $("#video-rgb");
  const btnPlay = $("#btn-play");
  const iconPlay = $("#icon-play");
  const iconPause = $("#icon-pause");
  const seekBar = $("#seek-bar");
  const seekFillEl = $("#seek-fill");
  const timeCurrent = $("#time-current");
  const timeTotal = $("#time-total");
  const infoId = $("#info-id");
  const infoDuration = $("#info-duration");
  const infoFps = $("#info-fps");
  const infoResolution = $("#info-resolution");

  let currentRecording = null;
  let imuData = null;
  let accelChart = null;
  let gyroChart = null;
  let animFrame = null;

  // ── S3 Browsing ──

  async function browse() {
    fileTree.innerHTML = '<div class="tree-loading">Loading</div>';

    try {
      const res = await fetch(`/api/browse?prefix=`);
      const data = await res.json();
      renderTree(data);
    } catch (err) {
      fileTree.innerHTML = `<div class="tree-empty">Error: ${err.message}</div>`;
    }
  }

  function renderTree(data) {
    const { folders } = data;
    if (!folders.length) {
      fileTree.innerHTML = '<div class="tree-empty">No recordings found</div>';
      return;
    }

    let html = "";
    for (const folder of folders) {
      const name = folder.name.replace(/\/$/, "");
      const isActive = currentRecording === folder.prefix;
      html += `<div class="tree-item${isActive ? " active" : ""}" data-recording="${folder.prefix}">
        <span class="tree-icon mcap">&#9654;</span>
        <span class="tree-name">${name}</span>
      </div>`;
    }

    fileTree.innerHTML = html;

    fileTree.querySelectorAll(".tree-item").forEach((el) => {
      el.addEventListener("click", () => {
        loadRecording(el.dataset.recording);
        fileTree.querySelectorAll(".tree-item").forEach((e) => e.classList.remove("active"));
        el.classList.add("active");
      });
    });
  }

  // ── Recording Loading ──

  async function loadRecording(recordingId) {
    currentRecording = recordingId;
    emptyState.style.display = "none";
    recordingView.style.display = "flex";

    // Show info
    const shortId = recordingId.split("/").pop() || recordingId;
    infoId.textContent = shortId;
    infoDuration.textContent = "...";
    infoFps.textContent = "";
    infoResolution.textContent = "";

    // Fetch metadata
    fetchMetadata(recordingId);

    // Start processing
    try {
      const res = await fetch(`/api/process/${encodeURIComponent(recordingId)}`, { method: "POST" });
      const data = await res.json();

      if (data.status === "ready") {
        onReady(recordingId);
      } else if (data.status === "processing") {
        showProgress();
        watchJob(data.job_id, recordingId);
      }
    } catch (err) {
      progressStage.textContent = "ERROR";
      progressDetail.textContent = err.message;
    }
  }

  async function fetchMetadata(recordingId) {
    try {
      const res = await fetch(`/api/metadata/${encodeURIComponent(recordingId)}`);
      if (!res.ok) return;
      const meta = await res.json();

      // Try different metadata shapes
      const config = meta.recording_config || meta.recording || {};
      if (config.camera_fps) infoFps.textContent = config.camera_fps + " fps";
      if (config.resolution) infoResolution.textContent = config.resolution;

      const stats = meta._stats || {};
      if (stats.duration_s) {
        infoDuration.textContent = formatTime(stats.duration_s);
      }
    } catch {
      // Non-critical
    }
  }

  function showProgress() {
    progressOverlay.style.display = "flex";
    progressFill.style.width = "0%";
    progressStage.textContent = "INITIALIZING";
    progressDetail.textContent = "";
  }

  function hideProgress() {
    progressOverlay.style.display = "none";
  }

  function watchJob(jobId, recordingId) {
    const source = new EventSource(`/api/jobs/${jobId}`);
    let overallProgress = 0;

    source.onmessage = (event) => {
      const data = JSON.parse(event.data);
      const { stage, progress, detail } = data;

      if (stage === "heartbeat") return;

      // Map stages to overall progress
      const stageWeights = { download: 0.15, rgb: 0.45, depth: 0.35, imu: 0.05 };
      const stageOffsets = { download: 0, rgb: 0.15, depth: 0.6, imu: 0.95 };

      if (stage in stageWeights) {
        overallProgress = stageOffsets[stage] + stageWeights[stage] * progress;
      }

      progressStage.textContent = stage.toUpperCase();
      progressFill.style.width = Math.round(overallProgress * 100) + "%";
      progressDetail.textContent = detail;

      if (stage === "done") {
        source.close();
        hideProgress();
        onReady(recordingId);
      } else if (stage === "error") {
        source.close();
        progressStage.textContent = "ERROR";
        progressDetail.textContent = detail;
      }
    };

    source.onerror = () => {
      source.close();
      progressStage.textContent = "CONNECTION LOST";
    };
  }

  function onReady(recordingId) {
    hideProgress();

    // Load video
    const rgbUrl = `/api/video/rgb/${encodeURIComponent(recordingId)}`;
    videoRgb.src = rgbUrl;

    // Load IMU
    loadImu(recordingId);

    // Reset transport
    updatePlayIcon();
  }

  // ── Video Sync ──

  btnPlay.addEventListener("click", togglePlay);

  function togglePlay() {
    if (videoRgb.paused) {
      videoRgb.play();
    } else {
      videoRgb.pause();
    }
    updatePlayIcon();
  }

  function updatePlayIcon() {
    const playing = !videoRgb.paused;
    iconPlay.style.display = playing ? "none" : "block";
    iconPause.style.display = playing ? "block" : "none";
  }

  videoRgb.addEventListener("play", () => {
    updatePlayIcon();
    startSyncLoop();
  });

  videoRgb.addEventListener("pause", () => {
    updatePlayIcon();
    stopSyncLoop();
  });

  videoRgb.addEventListener("timeupdate", onTimeUpdate);

  videoRgb.addEventListener("loadedmetadata", () => {
    timeTotal.textContent = formatTime(videoRgb.duration);
    if (!infoDuration.textContent || infoDuration.textContent === "...") {
      infoDuration.textContent = formatTime(videoRgb.duration);
    }
  });

  function onTimeUpdate() {
    const t = videoRgb.currentTime;
    const d = videoRgb.duration || 1;
    timeCurrent.textContent = formatTime(t);
    const pct = (t / d) * 100;
    seekBar.value = Math.round((t / d) * 1000);
    seekFillEl.style.width = pct + "%";
    updateChartCursors(t);
  }

  // Seek bar
  let seeking = false;
  seekBar.addEventListener("input", () => {
    seeking = true;
    const d = videoRgb.duration || 1;
    const t = (seekBar.value / 1000) * d;
    videoRgb.currentTime = t;
    timeCurrent.textContent = formatTime(t);
    seekFillEl.style.width = (seekBar.value / 10) + "%";
    updateChartCursors(t);
  });
  seekBar.addEventListener("change", () => {
    seeking = false;
  });

  // High-frequency sync loop for smooth chart cursor
  function startSyncLoop() {
    function tick() {
      if (!videoRgb.paused) {
        updateChartCursors(videoRgb.currentTime);
        animFrame = requestAnimationFrame(tick);
      }
    }
    animFrame = requestAnimationFrame(tick);
  }

  function stopSyncLoop() {
    if (animFrame) {
      cancelAnimationFrame(animFrame);
      animFrame = null;
    }
  }

  // Keyboard shortcuts
  document.addEventListener("keydown", (e) => {
    if (e.target.tagName === "INPUT") return;
    if (e.code === "Space") {
      e.preventDefault();
      togglePlay();
    } else if (e.code === "ArrowLeft") {
      videoRgb.currentTime = Math.max(0, videoRgb.currentTime - 5);
    } else if (e.code === "ArrowRight") {
      videoRgb.currentTime = Math.min(videoRgb.duration, videoRgb.currentTime + 5);
    }
  });

  // ── IMU Charts ──

  async function loadImu(recordingId) {
    try {
      const res = await fetch(`/api/imu/${encodeURIComponent(recordingId)}`);
      if (!res.ok) return;
      imuData = await res.json();
      renderCharts();
    } catch {
      // Non-critical
    }
  }

  function videoCursorPlugin(getVideoTime, onChartClick) {
    return {
      hooks: {
        draw: [
          (u) => {
            const t = getVideoTime();
            if (t == null || !u.data || !u.data[0] || u.data[0].length === 0) return;

            const cx = u.valToPos(t, "x", true);
            if (cx < u.bbox.left / devicePixelRatio || cx > (u.bbox.left + u.bbox.width) / devicePixelRatio) return;

            const ctx = u.ctx;
            ctx.save();
            ctx.beginPath();
            ctx.strokeStyle = "rgba(155, 153, 209, 0.7)";
            ctx.lineWidth = 1.5;
            const top = u.bbox.top / devicePixelRatio;
            const bot = (u.bbox.top + u.bbox.height) / devicePixelRatio;
            ctx.moveTo(cx, top);
            ctx.lineTo(cx, bot);
            ctx.stroke();
            ctx.restore();
          },
        ],
      },
    };
  }

  function renderCharts() {
    if (!imuData) return;

    // Destroy existing
    if (accelChart) { accelChart.destroy(); accelChart = null; }
    if (gyroChart) { gyroChart.destroy(); gyroChart = null; }

    const ts = new Float64Array(imuData.timestamps);

    const getVideoTime = () => videoRgb.currentTime || 0;
    const onChartClick = (t) => {
      videoRgb.currentTime = t;
    };

    const accelEl = $("#chart-accel");
    const gyroEl = $("#chart-gyro");

    const accelH = accelEl.clientHeight || 160;
    const accelW = accelEl.clientWidth || 400;
    const gyroH = gyroEl.clientHeight || 160;
    const gyroW = gyroEl.clientWidth || 400;

    const commonOpts = {
      cursor: { show: false },
      select: { show: false },
      legend: { show: false },
      plugins: [videoCursorPlugin(getVideoTime, onChartClick)],
      scales: {
        x: { time: false },
      },
      axes: [
        {
          stroke: "rgba(255,255,255,0.2)",
          grid: { stroke: "rgba(255,255,255,0.04)", width: 1 },
          ticks: { stroke: "rgba(255,255,255,0.06)", width: 1 },
          font: "10px Inter, sans-serif",
          labelFont: "10px Inter, sans-serif",
        },
        {
          stroke: "rgba(255,255,255,0.2)",
          grid: { stroke: "rgba(255,255,255,0.04)", width: 1 },
          ticks: { stroke: "rgba(255,255,255,0.06)", width: 1 },
          font: "10px Inter, sans-serif",
          labelFont: "10px Inter, sans-serif",
        },
      ],
    };

    const seriesColors = {
      x: "#ef4444",
      y: "#22c55e",
      z: "#60a5fa",
    };

    accelChart = new uPlot(
      {
        ...commonOpts,
        width: accelW,
        height: accelH,
        series: [
          {},
          { label: "X", stroke: seriesColors.x, width: 1 },
          { label: "Y", stroke: seriesColors.y, width: 1 },
          { label: "Z", stroke: seriesColors.z, width: 1 },
        ],
      },
      [ts, new Float64Array(imuData.accel.x), new Float64Array(imuData.accel.y), new Float64Array(imuData.accel.z)],
      accelEl
    );

    gyroChart = new uPlot(
      {
        ...commonOpts,
        width: gyroW,
        height: gyroH,
        series: [
          {},
          { label: "X", stroke: seriesColors.x, width: 1 },
          { label: "Y", stroke: seriesColors.y, width: 1 },
          { label: "Z", stroke: seriesColors.z, width: 1 },
        ],
      },
      [ts, new Float64Array(imuData.gyro.x), new Float64Array(imuData.gyro.y), new Float64Array(imuData.gyro.z)],
      gyroEl
    );

    // Click to seek
    accelEl.addEventListener("click", (e) => chartClickSeek(e, accelChart));
    gyroEl.addEventListener("click", (e) => chartClickSeek(e, gyroChart));
  }

  function chartClickSeek(e, chart) {
    if (!chart) return;
    const rect = chart.root.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const t = chart.posToVal(x, "x");
    if (t >= 0 && t <= (videoRgb.duration || Infinity)) {
      videoRgb.currentTime = t;
    }
  }

  function updateChartCursors(t) {
    // Trigger redraw by calling chart.redraw() which fires the draw hooks
    if (accelChart) accelChart.redraw(false);
    if (gyroChart) gyroChart.redraw(false);
  }

  // Resize charts on window resize
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      if (accelChart) {
        const el = $("#chart-accel");
        accelChart.setSize({ width: el.clientWidth, height: el.clientHeight });
      }
      if (gyroChart) {
        const el = $("#chart-gyro");
        gyroChart.setSize({ width: el.clientWidth, height: el.clientHeight });
      }
    }, 200);
  });

  // ── Helpers ──

  function formatTime(s) {
    if (!s || !isFinite(s)) return "0:00";
    const mins = Math.floor(s / 60);
    const secs = Math.floor(s % 60);
    return mins + ":" + String(secs).padStart(2, "0");
  }

  // ── Init ──
  browse("");
})();
