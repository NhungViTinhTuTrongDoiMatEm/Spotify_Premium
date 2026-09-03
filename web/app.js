/**
 * Spotify Premium Analytics & Music Explorer - Frontend Controller
 * Xử lý dữ liệu, chuyển tab, tìm kiếm siêu tốc và phát thông tin bài hát.
 */

let appData = null;
let searchDebounceTimer = null;

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initSearch();
  initSync();
  fetchAnalyticsData();
});

// ==============================================================================
// 1. DATA FETCHING & STATE MANAGEMENT
// ==============================================================================

async function fetchAnalyticsData() {
  try {
    const res = await fetch("/api/stats");
    if (!res.ok) throw new Error("Không thể nạp dữ liệu thống kê");
    appData = await res.json();
    renderAllViews(appData);
  } catch (err) {
    console.error("Lỗi nạp dữ liệu:", err);
  }
}

function renderAllViews(data) {
  renderOverview(data);
  renderTopTracksTable(data.top_tracks);
  renderTopArtistsGrid(data.top_artists);
  renderWrapped(data);
  renderRecentStreams(data.recent_streams);

  // Cập nhật nhãn đồng bộ
  if (data.latest_sync) {
    const dateObj = new Date(data.latest_sync);
    document.getElementById("latest-sync-label").textContent = dateObj.toLocaleDateString("vi-VN", {
      hour: "2-digit",
      minute: "2-digit",
      day: "2-digit",
      month: "2-digit"
    });
  }

  // Mặc định chọn bài đầu tiên đưa vào Player Bar
  if (data.top_tracks && data.top_tracks.length > 0) {
    updatePlayerBar(data.top_tracks[0]);
  }
}

// ==============================================================================
// 2. TAB NAVIGATION
// ==============================================================================

function initTabs() {
  const navButtons = document.querySelectorAll(".nav-btn");
  navButtons.forEach(btn => {
    btn.addEventListener("click", () => {
      const tabId = btn.getAttribute("data-tab");
      switchTab(tabId);
    });
  });
}

function switchTab(tabId) {
  // Cập nhật trạng thái active của buttons
  document.querySelectorAll(".nav-btn").forEach(btn => {
    btn.classList.toggle("active", btn.getAttribute("data-tab") === tabId);
  });

  // Cập nhật pane
  document.querySelectorAll(".tab-pane").forEach(pane => {
    pane.classList.remove("active");
  });

  const targetPane = document.getElementById(`pane-${tabId}`);
  if (targetPane) {
    targetPane.classList.add("active");
  }

  // Tự động focus vào ô tìm kiếm nếu chuyển qua tab search
  if (tabId === "search") {
    document.getElementById("global-search-input").focus();
  }
}

// ==============================================================================
// 3. OVERVIEW TAB RENDERING
// ==============================================================================

function renderOverview(data) {
  // KPI Counters
  document.getElementById("kpi-total-streams").textContent = data.total_streams.toLocaleString();
  document.getElementById("kpi-total-hours").textContent = `${data.total_hours}h (${data.total_minutes}m)`;
  document.getElementById("kpi-unique-tracks").textContent = data.unique_tracks.toLocaleString();
  document.getElementById("kpi-unique-artists").textContent = data.unique_artists.toLocaleString();

  // Persona & Peak Slot
  document.getElementById("persona-text").textContent = `Gu nghe nhạc: ${data.persona}`;
  document.getElementById("peak-slot-label").textContent = data.peak_slot;

  // Render 6 bài hát hot nhất
  const tracksContainer = document.getElementById("quick-top-tracks");
  tracksContainer.innerHTML = "";
  const quickTracks = data.top_tracks.slice(0, 6);

  quickTracks.forEach(t => {
    const card = document.createElement("div");
    card.className = "music-card";
    card.onclick = () => updatePlayerBar(t);

    card.innerHTML = `
      <img src="${t.image_url || 'https://via.placeholder.com/300'}" class="music-card-cover" alt="${t.track_name}">
      <span class="music-card-title">${t.track_name}</span>
      <span class="music-card-artist">${t.artist_names}</span>
      <span class="music-card-badge">🔥 ${t.total_streams} lượt</span>
    `;
    tracksContainer.appendChild(card);
  });

  // Render 6 nghệ sĩ hot nhất
  const artistsContainer = document.getElementById("quick-top-artists");
  artistsContainer.innerHTML = "";
  const quickArtists = data.top_artists.slice(0, 6);

  quickArtists.forEach(a => {
    const card = document.createElement("div");
    card.className = "artist-card";
    card.onclick = () => filterByArtist(a.artist_name);

    card.innerHTML = `
      <img src="${a.sample_image || 'https://via.placeholder.com/150'}" class="artist-avatar" alt="${a.artist_name}">
      <span class="artist-name">${a.artist_name}</span>
      <span class="artist-stats">${a.total_streams} lượt nghe</span>
    `;
    artistsContainer.appendChild(card);
  });
}

// ==============================================================================
// 4. TOP 50 TRACKS TABLE
// ==============================================================================

function renderTopTracksTable(tracks) {
  const tbody = document.getElementById("top-tracks-tbody");
  tbody.innerHTML = "";

  tracks.forEach((t, idx) => {
    const tr = document.createElement("tr");
    const rankClass = idx === 0 ? "rank-1" : (idx === 1 ? "rank-2" : (idx === 2 ? "rank-3" : ""));

    const dateFormatted = t.last_listened_at ? new Date(t.last_listened_at).toLocaleDateString("vi-VN") : "--";

    tr.innerHTML = `
      <td><span class="rank-badge ${rankClass}">#${idx + 1}</span></td>
      <td>
        <div class="table-track-cell">
          <img src="${t.image_url || 'https://via.placeholder.com/64'}" class="table-thumb" alt="${t.track_name}">
          <div>
            <div style="font-weight: 700; color: #fff;">${t.track_name}</div>
            <div style="font-size: 12px; color: var(--text-secondary);">${t.artist_names}</div>
          </div>
        </div>
      </td>
      <td style="color: var(--text-secondary);">${t.album_name}</td>
      <td style="font-weight: 700; color: var(--spotify-green);">${t.total_streams}</td>
      <td>${t.total_minutes} phút</td>
      <td style="color: var(--text-muted);">${dateFormatted}</td>
      <td>
        ${t.spotify_url ? `<a href="${t.spotify_url}" target="_blank" class="btn-open-spotify">Mở ➔</a>` : '--'}
      </td>
    `;
    tr.onclick = () => updatePlayerBar(t);
    tbody.appendChild(tr);
  });
}

// ==============================================================================
// 5. TOP 50 ARTISTS GRID
// ==============================================================================

function renderTopArtistsGrid(artists) {
  const container = document.getElementById("full-top-artists");
  container.innerHTML = "";

  artists.forEach((a, idx) => {
    const card = document.createElement("div");
    card.className = "artist-card";
    card.onclick = () => filterByArtist(a.artist_name);

    card.innerHTML = `
      <img src="${a.sample_image || 'https://via.placeholder.com/150'}" class="artist-avatar" alt="${a.artist_name}">
      <span class="artist-name">#${idx + 1} ${a.artist_name}</span>
      <span class="artist-stats" style="color: var(--spotify-green); font-weight: 700;">${a.total_streams} lượt nghe</span>
      <span style="font-size: 11px; color: var(--text-muted); margin-top: 4px;">${a.total_minutes} phút</span>
    `;
    container.appendChild(card);
  });
}

// ==============================================================================
// 6. WRAPPED ANALYTICS TAB
// ==============================================================================

function renderWrapped(data) {
  document.getElementById("wrapped-persona-badge").textContent = data.persona;
  document.getElementById("wrapped-diversity").textContent = `${Math.round(data.diversity_ratio * 100)}%`;
  document.getElementById("wrapped-hours").textContent = `${data.total_hours} giờ`;
  document.getElementById("wrapped-top-artist").textContent = data.top_artists[0] ? data.top_artists[0].artist_name : "--";

  // Render Time Slot Distribution Bars
  const container = document.getElementById("time-slots-bars");
  container.innerHTML = "";

  const schedule = data.time_schedule || {};
  const totalSlots = Object.values(schedule).reduce((a, b) => a + b, 0) || 1;

  for (const [slotName, count] of Object.entries(schedule)) {
    const percent = Math.round((count / totalSlots) * 100);
    const item = document.createElement("div");
    item.className = "time-slot-item";

    item.innerHTML = `
      <div class="slot-label-row">
        <span>${slotName}</span>
        <span style="color: var(--spotify-green);">${count} lượt (${percent}%)</span>
      </div>
      <div class="slot-bar-bg">
        <div class="slot-bar-fill" style="width: ${percent}%;"></div>
      </div>
    `;
    container.appendChild(item);
  }
}

// ==============================================================================
// 7. RECENT STREAMS TAB
// ==============================================================================

function renderRecentStreams(recentStreams) {
  const container = document.getElementById("recent-streams-list");
  container.innerHTML = "";

  if (!recentStreams || recentStreams.length === 0) {
    container.innerHTML = "<p style='color: var(--text-muted);'>Chưa có dữ liệu gần đây.</p>";
    return;
  }

  recentStreams.forEach(s => {
    const row = document.createElement("div");
    row.className = "track-row";
    row.onclick = () => updatePlayerBar(s);

    const timeStr = new Date(s.played_at).toLocaleTimeString("vi-VN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      day: "2-digit",
      month: "2-digit"
    });

    row.innerHTML = `
      <img src="${s.image_url || 'https://via.placeholder.com/64'}" class="track-row-img" alt="${s.track_name}">
      <div class="track-row-info">
        <div class="track-row-title">${s.track_name}</div>
        <div class="track-row-meta">${s.artist_names} • ${s.album_name}</div>
      </div>
      <div class="track-row-stats">
        <span class="track-row-count">${s.time_slot}</span>
        <span class="track-row-time">${timeStr}</span>
      </div>
    `;
    container.appendChild(row);
  });
}

// ==============================================================================
// 8. SEARCH MODULE
// ==============================================================================

function initSearch() {
  const searchInput = document.getElementById("global-search-input");

  // Keyboard shortcut Ctrl + K
  window.addEventListener("keydown", (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      searchInput.focus();
      switchTab("search");
    }
  });

  searchInput.addEventListener("input", (e) => {
    const q = e.target.value.trim();
    clearTimeout(searchDebounceTimer);
    searchDebounceTimer = setTimeout(() => {
      performSearch(q);
    }, 200);
  });

  searchInput.addEventListener("focus", () => {
    switchTab("search");
  });
}

async function performSearch(query) {
  try {
    const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
    const results = await res.json();

    const tracks = results.matched_tracks || [];
    const artists = results.matched_artists || [];

    document.getElementById("count-matched-tracks").textContent = tracks.length;
    document.getElementById("count-matched-artists").textContent = artists.length;

    // Render tracks
    const tracksContainer = document.getElementById("search-tracks-list");
    tracksContainer.innerHTML = "";
    if (tracks.length === 0) {
      tracksContainer.innerHTML = "<p style='color: var(--text-muted); padding: 12px;'>Không tìm thấy bài hát nào.</p>";
    } else {
      tracks.forEach(t => {
        const row = document.createElement("div");
        row.className = "track-row";
        row.onclick = () => updatePlayerBar(t);
        row.innerHTML = `
          <img src="${t.image_url || 'https://via.placeholder.com/64'}" class="track-row-img" alt="${t.track_name}">
          <div class="track-row-info">
            <div class="track-row-title">${t.track_name}</div>
            <div class="track-row-meta">${t.artist_names} • ${t.album_name}</div>
          </div>
          <div class="track-row-stats">
            <span class="track-row-count">${t.total_streams} lượt</span>
            <span class="track-row-time">${t.total_minutes} phút</span>
          </div>
        `;
        tracksContainer.appendChild(row);
      });
    }

    // Render artists
    const artistsContainer = document.getElementById("search-artists-list");
    artistsContainer.innerHTML = "";
    if (artists.length === 0) {
      artistsContainer.innerHTML = "<p style='color: var(--text-muted); padding: 12px;'>Không tìm thấy nghệ sĩ nào.</p>";
    } else {
      artists.forEach(a => {
        const row = document.createElement("div");
        row.className = "track-row";
        row.onclick = () => filterByArtist(a.artist_name);
        row.innerHTML = `
          <img src="${a.sample_image || 'https://via.placeholder.com/64'}" class="track-row-img" style="border-radius: 50%;" alt="${a.artist_name}">
          <div class="track-row-info">
            <div class="track-row-title">${a.artist_name}</div>
            <div class="track-row-meta">Đã nghe ${a.total_streams} bài • ${a.total_minutes} phút</div>
          </div>
        `;
        artistsContainer.appendChild(row);
      });
    }
  } catch (err) {
    console.error("Lỗi tìm kiếm:", err);
  }
}

function filterByArtist(artistName) {
  const searchInput = document.getElementById("global-search-input");
  searchInput.value = artistName;
  switchTab("search");
  performSearch(artistName);
}

// ==============================================================================
// 9. PLAYER BAR CONTROLLER
// ==============================================================================

function updatePlayerBar(track) {
  if (!track) return;
  document.getElementById("player-title").textContent = track.track_name || "Unknown Track";
  document.getElementById("player-artist").textContent = track.artist_names || "Unknown Artist";
  document.getElementById("player-status").textContent = `Đã nghe ${track.total_streams || 1} lần (${track.total_minutes || Math.round(track.duration_ms / 60000)} phút)`;
  
  if (track.image_url) {
    document.getElementById("player-img").src = track.image_url;
  }

  const linkBtn = document.getElementById("player-spotify-link");
  if (track.spotify_url) {
    linkBtn.href = track.spotify_url;
    linkBtn.style.display = "flex";
  } else {
    linkBtn.style.display = "none";
  }
}

// ==============================================================================
// 10. SYNC TRIGGER
// ==============================================================================

function initSync() {
  const btnSync = document.getElementById("btn-sync");
  btnSync.addEventListener("click", async () => {
    btnSync.classList.add("loading");
    btnSync.querySelector("span").textContent = "Đang đồng bộ...";

    try {
      const res = await fetch("/api/sync", { method: "POST" });
      const result = await res.json();
      if (result.success) {
        alert("🎉 Đồng bộ dữ liệu Spotify thành công!");
        await fetchAnalyticsData();
      } else {
        alert("⚠️ Đồng bộ thất bại: " + (result.error || "Kiểm tra kết nối Spotify"));
      }
    } catch (err) {
      alert("❌ Lỗi gọi API đồng bộ: " + err);
    } finally {
      btnSync.classList.remove("loading");
      btnSync.querySelector("span").textContent = "Đồng Bộ Ngay";
    }
  });
}
