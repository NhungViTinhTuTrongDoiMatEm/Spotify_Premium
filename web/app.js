/**
 * Spotify Premium Analytics & Music Explorer - Frontend Controller
 * Xử lý dữ liệu, chuyển tab, tìm kiếm siêu tốc và phát thông tin bài hát.
 */

let appData = null;
let searchDebounceTimer = null;

// Placeholder SVG chuẩn Spotify Dark Theme khi thiếu ảnh hoặc ảnh tải chậm
const DEFAULT_TRACK_IMG = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 24 24' fill='%231db954'%3E%3Crect width='24' height='24' fill='%23181818'/%3E%3Cpath d='M12 3v10.55c-.59-.34-1.27-.55-2-.55-2.21 0-4 1.79-4 4s1.79 4 4 4 4-1.79 4-4V7h4V3h-6z'/%3E%3C/svg%3E";
const DEFAULT_ARTIST_IMG = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 24 24' fill='%231db954'%3E%3Crect width='24' height='24' rx='12' fill='%23222'/%3E%3Cpath d='M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z'/%3E%3C/svg%3E";

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  initSearch();
  initSync();
  initAlbumModal();
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
  renderDiscovery(data.albums || [], data.unheard_recommendations || []);

  // Cập nhật trạng thái nguồn dữ liệu Databricks Lakehouse
  const statusPill = document.getElementById("lakehouse-status-pill");
  const statusText = document.getElementById("lakehouse-status-text");
  const modeTag = document.getElementById("lakehouse-mode-tag");
  if (statusPill && statusText && modeTag) {
    if (data.is_live) {
      statusPill.className = "lakehouse-status-pill live";
      statusText.textContent = "Databricks Lakehouse";
      modeTag.textContent = "LIVE";
      statusPill.title = `Dữ liệu thời gian thực từ ${data.warehouse_name || "Databricks SQL Warehouse"}`;
    } else {
      statusPill.className = "lakehouse-status-pill offline";
      statusText.textContent = "Local Bronze JSON";
      modeTag.textContent = "OFFLINE";
      statusPill.title = "Đang chạy chế độ offline từ file JSON local";
    }
  }

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
      <img src="${t.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="music-card-cover" alt="${t.track_name}">
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
      <img src="${a.sample_image || DEFAULT_ARTIST_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_ARTIST_IMG}';" class="artist-avatar" alt="${a.artist_name}">
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
          <img src="${t.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="table-thumb" alt="${t.track_name}">
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
      <img src="${a.sample_image || DEFAULT_ARTIST_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_ARTIST_IMG}';" class="artist-avatar" alt="${a.artist_name}">
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
      <img src="${s.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="track-row-img" alt="${s.track_name}">
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

let currentSearchFilter = "all";

function initSearch() {
  const searchInput = document.getElementById("global-search-input");

  // Keyboard shortcut Ctrl + K
  window.addEventListener("keydown", (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      searchInput.focus();
    }
  });

  searchInput.addEventListener("input", () => {
    clearTimeout(searchDebounceTimer);
    searchDebounceTimer = setTimeout(() => {
      performSearch(searchInput.value.trim(), currentSearchFilter);
    }, 200);
  });

  searchInput.addEventListener("focus", () => {
    switchTab("search");
  });

  // Filter Pills (Tất Cả, Đã Nghe, Gợi Ý Chưa Nghe)
  const pills = document.querySelectorAll(".filter-pill");
  pills.forEach(pill => {
    pill.addEventListener("click", () => {
      pills.forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      currentSearchFilter = pill.getAttribute("data-filter") || "all";
      performSearch(searchInput.value.trim(), currentSearchFilter);
    });
  });
}

async function performSearch(query, filter = currentSearchFilter) {
  try {
    const res = await fetch(`/api/search?q=${encodeURIComponent(query)}&filter=${encodeURIComponent(filter)}`);
    const results = await res.json();

    const tracks = results.matched_tracks || [];
    const artists = results.matched_artists || [];

    document.getElementById("count-matched-tracks").textContent = tracks.length;
    document.getElementById("count-matched-artists").textContent = artists.length;

    // Render tracks
    const tracksContainer = document.getElementById("search-tracks-list");
    tracksContainer.innerHTML = "";
    if (tracks.length === 0) {
      tracksContainer.innerHTML = "<p style='color: var(--text-muted); padding: 12px;'>Không tìm thấy bài hát nào phù hợp.</p>";
    } else {
      tracks.forEach(t => {
        const row = document.createElement("div");
        row.className = "track-row";
        row.onclick = () => updatePlayerBar(t);

        const statusTag = t.is_listened
          ? `<span class="track-tag heard">✔️ Đã nghe (${t.total_streams || 1} lần)</span>`
          : `<span class="track-tag unheard">✨ Gợi ý chưa nghe</span>`;

        row.innerHTML = `
          <img src="${t.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="track-row-img" alt="${t.track_name}">
          <div class="track-row-info">
            <div class="track-row-title">${t.track_name}</div>
            <div class="track-row-meta">${t.artist_names} • ${t.album_name}</div>
          </div>
          <div class="track-row-stats" style="display: flex; flex-direction: column; align-items: flex-end; gap: 4px;">
            ${statusTag}
            <span class="track-row-time">${t.total_minutes ? t.total_minutes + ' phút' : Math.round((t.duration_ms || 180000)/60000) + ' phút'}</span>
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
          <img src="${a.sample_image || DEFAULT_ARTIST_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_ARTIST_IMG}';" class="track-row-img" style="border-radius: 50%;" alt="${a.artist_name}">
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
  performSearch(artistName, currentSearchFilter);
}

// ==============================================================================
// 9. PLAYER BAR CONTROLLER
// ==============================================================================

function updatePlayerBar(track) {
  if (!track) return;
  document.getElementById("player-title").textContent = track.track_name || "Unknown Track";
  document.getElementById("player-artist").textContent = track.artist_names || "Unknown Artist";
  document.getElementById("player-status").textContent = `Đã nghe ${track.total_streams || 1} lần (${track.total_minutes || Math.round(track.duration_ms / 60000)} phút)`;
  
  const playerImg = document.getElementById("player-img");
  if (playerImg) {
    playerImg.src = track.image_url || DEFAULT_TRACK_IMG;
    playerImg.onerror = () => { playerImg.src = DEFAULT_TRACK_IMG; };
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
  if (btnSync) {
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
        btnSync.querySelector("span").textContent = "Đồng Bộ Spotify";
      }
    });
  }

  const btnRefreshLakehouse = document.getElementById("btn-refresh-lakehouse");
  if (btnRefreshLakehouse) {
    btnRefreshLakehouse.addEventListener("click", async () => {
      btnRefreshLakehouse.classList.add("loading");
      btnRefreshLakehouse.querySelector("span").textContent = "Đang tải...";
      try {
        const res = await fetch("/api/refresh-lakehouse", { method: "POST" });
        if (res.ok) {
          await fetchAnalyticsData();
        }
      } catch (err) {
        console.error("Lỗi làm mới Lakehouse:", err);
      } finally {
        btnRefreshLakehouse.classList.remove("loading");
        btnRefreshLakehouse.querySelector("span").textContent = "Làm Mới Lakehouse";
      }
    });
  }
}

// ==============================================================================
// 11. DISCOVERY & UNHEARD GEMS & ALBUM TRACKER
// ==============================================================================

function renderDiscovery(albums, recommendations) {
  // 1. Unheard Gems Carousel
  const gemsCount = document.getElementById("unheard-gems-count");
  if (gemsCount) gemsCount.textContent = `${recommendations.length} bài gợi ý`;

  const gemsContainer = document.getElementById("unheard-gems-list");
  if (gemsContainer) {
    gemsContainer.innerHTML = "";
    if (!recommendations || recommendations.length === 0) {
      gemsContainer.innerHTML = "<p style='color: var(--text-muted); padding: 16px;'>Bạn đã nghe hết tất cả bài hát trong các album được cào!</p>";
    } else {
      // Hiển thị tối đa 30 bài gợi ý ưu tiên
      recommendations.slice(0, 30).forEach(item => {
        const card = document.createElement("div");
        card.className = "unheard-gem-card";
        card.onclick = () => updatePlayerBar(item);

        card.innerHTML = `
          <div class="unheard-gem-cover-wrap">
            <img src="${item.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="unheard-gem-cover" alt="${item.track_name}">
            <span class="badge-gem-tag">✨ Gợi ý</span>
            <div class="gem-play-btn" title="Xem bài hát">
              <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>
            </div>
          </div>
          <div class="unheard-gem-title" title="${item.track_name}">${item.track_name}</div>
          <div class="unheard-gem-artist" title="${item.artist_names}">${item.artist_names}</div>
          <div class="unheard-gem-album" title="${item.album_name}">${item.album_name}</div>
        `;
        gemsContainer.appendChild(card);
      });
    }
  }

  // 2. Album Completion Tracker Grid
  const albumsCount = document.getElementById("albums-tracker-count");
  if (albumsCount) albumsCount.textContent = `${albums.length} album`;

  const albumsContainer = document.getElementById("albums-tracker-grid");
  if (albumsContainer) {
    albumsContainer.innerHTML = "";
    if (!albums || albums.length === 0) {
      albumsContainer.innerHTML = "<p style='color: var(--text-muted); padding: 16px;'>Chưa có dữ liệu catalog album.</p>";
    } else {
      albums.forEach(album => {
        const card = document.createElement("div");
        card.className = "album-tracker-card";
        card.onclick = () => openAlbumModal(album);

        const rate = album.completion_rate || 0;
        const fillClass = rate >= 100 ? "complete" : (rate >= 50 ? "high" : "");
        const year = album.release_date ? album.release_date.split("-")[0] : "";

        card.innerHTML = `
          <div class="album-tracker-top">
            <img src="${album.image_url || DEFAULT_TRACK_IMG}" onerror="this.onerror=null; this.src='${DEFAULT_TRACK_IMG}';" class="album-tracker-cover" alt="${album.album_name}">
            <div class="album-tracker-info">
              <div class="album-tracker-name" title="${album.album_name}">${album.album_name}</div>
              <div class="album-tracker-artist" title="${album.artist_names}">${album.artist_names}</div>
              <div class="album-tracker-meta">${year ? year + ' • ' : ''}${album.total_tracks} bài hát (${album.album_type})</div>
            </div>
          </div>
          <div class="album-tracker-progress">
            <div class="album-progress-label">
              <span class="album-progress-text">Đã nghe: ${album.tracks_listened}/${album.total_tracks} bài</span>
              <span class="album-progress-rate">${rate}%</span>
            </div>
            <div class="album-progress-bar">
              <div class="album-progress-fill ${fillClass}" style="width: ${rate}%;"></div>
            </div>
          </div>
        `;
        albumsContainer.appendChild(card);
      });
    }
  }
}

// ==============================================================================
// 12. ALBUM TRACKLIST MODAL CONTROLLER
// ==============================================================================

function initAlbumModal() {
  const modal = document.getElementById("album-modal");
  const closeBtn = document.getElementById("modal-close-btn");

  if (!modal) return;

  if (closeBtn) {
    closeBtn.onclick = () => { modal.style.display = "none"; };
  }

  modal.addEventListener("click", (e) => {
    if (e.target === modal) {
      modal.style.display = "none";
    }
  });

  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modal.style.display !== "none") {
      modal.style.display = "none";
    }
  });
}

function openAlbumModal(album) {
  const modal = document.getElementById("album-modal");
  if (!modal || !album) return;

  const imgEl = document.getElementById("modal-album-img");
  imgEl.src = album.image_url || DEFAULT_TRACK_IMG;
  imgEl.onerror = () => { imgEl.src = DEFAULT_TRACK_IMG; };

  document.getElementById("modal-album-title").textContent = album.album_name;
  document.getElementById("modal-album-artist").textContent = album.artist_names;
  document.getElementById("modal-album-type").textContent = (album.album_type || "ALBUM").toUpperCase();

  const rate = album.completion_rate || 0;
  document.getElementById("modal-progress-fill").style.width = `${rate}%`;
  document.getElementById("modal-progress-text").textContent = `Đã nghe ${album.tracks_listened}/${album.total_tracks} bài (${rate}%)`;

  const tracklistEl = document.getElementById("modal-tracklist");
  tracklistEl.innerHTML = "";

  (album.tracklist || []).forEach(tr => {
    const row = document.createElement("div");
    row.className = `modal-track-row ${tr.is_listened ? '' : 'unheard-row'}`;
    row.onclick = () => updatePlayerBar(tr);

    const durMins = Math.floor(tr.duration_ms / 60000);
    const durSecs = Math.floor((tr.duration_ms % 60000) / 1000).toString().padStart(2, "0");
    const durStr = `${durMins}:${durSecs}`;

    const tag = tr.is_listened
      ? `<span class="track-tag heard">✔️ Đã nghe</span>`
      : `<span class="track-tag unheard">✨ Chưa nghe</span>`;

    row.innerHTML = `
      <div class="modal-track-left">
        <span class="modal-track-num">${tr.track_number}</span>
        <div class="modal-track-details">
          <div class="modal-track-name">${tr.track_name}</div>
          <div class="modal-track-artists">${tr.artist_names}</div>
        </div>
      </div>
      <div class="modal-track-right">
        ${tag}
        <span class="modal-track-dur">${durStr}</span>
      </div>
    `;
    tracklistEl.appendChild(row);
  });

  modal.style.display = "flex";
}

