(() => {
	function byId(id) { return document.getElementById(id); }
	function qs(sel, el = document) { return el.querySelector(sel); }
	function qsa(sel, el = document) { return Array.from(el.querySelectorAll(sel)); }

	const cfg = window.HOT_CONFIG || { nextOffset: 0, apiUrl: "/api/events", total: 0 };
	const timelineRoot = byId("timeline-root");
    const monthNav = byId("month-nav");
	const sentinel = byId("infinite-sentinel");
	// top sentinel will be looked up dynamically by id because it can be recreated
    let selectedStreamerIds = new Set();
    let selectedTagIds = new Set();
	let loadingNext = false;
	let loadingPrev = false;
	let topObserver = null;
	let bottomObserver = null;
    // Defer loading previous pages for a brief period after deep-link landing
    let deferTopLoadingUntil = 0;

    // If there are no events at all, hide the left sidebar immediately to avoid initial flash
    try {
        if (!cfg || !Number.isFinite(cfg.total) || cfg.total === 0) {
            const left = qs('.sidebar');
            if (left) left.style.display = 'none';
        }
    } catch {}
	function topSentinelVisible() {
		const ts = byId("top-sentinel");
		if (!ts) return false;
		const r = ts.getBoundingClientRect();
		const vh = window.innerHeight || document.documentElement.clientHeight;
		// consider "visible" if within 600px of the top of viewport
		return r.top < 600 && r.bottom >= -50;
	}

	function bottomSentinelVisible() {
		const bs = byId("infinite-sentinel");
		if (!bs) return false;
		const r = bs.getBoundingClientRect();
		const vh = window.innerHeight || document.documentElement.clientHeight;
		// visible if its top is within 250px of bottom of viewport
		return r.top < vh + 250;
	}

	function monthAnchor(year, month) {
		const m = String(month).padStart(2, "0");
		return `y${year}-${m}`;
	}

	function parseHashId(hash) {
		if (!hash) return null;
		const raw = hash.startsWith("#") ? hash.slice(1) : hash;
		if (/^\d+$/.test(raw)) return parseInt(raw, 10);
		return null;
	}
    function parseHashKey(hash) {
        if (!hash) return null;
        const raw = hash.startsWith('#') ? hash.slice(1) : hash;
        if (!raw) return null;
        return raw; // allow slug or id string
    }

    // Avoid browser auto scroll to hash only when we have a numeric deep link; we will handle it.
    try {
        const initId = parseHashId(window.location.hash);
        if (initId && "scrollRestoration" in history) {
            history.scrollRestoration = "manual";
        }
    } catch {}

    function scrollElementIntoCenter(el, behavior = "smooth") {
		if (!el) return;
		const rect = el.getBoundingClientRect();
		const vh = window.innerHeight || document.documentElement.clientHeight;
		const elH = Math.max(rect.height, el.offsetHeight || 0);
        // account for fixed header and fixed footer
        const rootStyle = getComputedStyle(document.documentElement);
        const footerVar = rootStyle.getPropertyValue('--footer-h') || '64px';
        const headerVar = rootStyle.getPropertyValue('--header-h') || '64px';
        const footerH = parseInt(footerVar) || 64;
        const headerH = parseInt(headerVar) || 64;
        // Compute available viewport height excluding only header and footer
        const avail = Math.max(0, vh - footerH - headerH);
        const offsetFromTop = headerH + Math.max(0, (avail - elH) / 2);
		// target scroll so element center aligns within the area between header and footer
		let targetTop = window.scrollY + rect.top - offsetFromTop;
		const maxScroll = Math.max(0, document.documentElement.scrollHeight - vh);
		if (targetTop < 0) targetTop = 0;
		if (targetTop > maxScroll) targetTop = maxScroll;
        // If we're already essentially at the target, avoid any scroll jitter
        if (Math.abs(window.scrollY - targetTop) < 1) return;
		window.scrollTo({ top: targetTop, behavior });
	}

	function ensureMonthSection(year, month, monthName, position = "append") {
		const anchor = monthAnchor(year, month);
		let header = byId(anchor);
		let list = qs(`.timeline-list[data-anchor="${anchor}"]`, timelineRoot);
		if (!header) {
			header = document.createElement("h3");
			header.className = "timeline-month";
			header.id = anchor;
			header.textContent = `${monthName} ${year}`;
			// Insert header in descending chronological order among existing month headers
			const headers = qsa(".timeline-month", timelineRoot);
			let inserted = false;
			for (const h of headers) {
				// Place before the first header with an older (lexicographically smaller) anchor
				if (anchor > h.id) {
					timelineRoot.insertBefore(header, h);
					inserted = true;
					break;
				}
			}
			if (!inserted) {
				timelineRoot.appendChild(header);
			}
			list = document.createElement("div");
			list.className = "timeline-list";
			list.setAttribute("data-anchor", anchor);
			// place list right after header
			if (header.nextSibling) {
				timelineRoot.insertBefore(list, header.nextSibling);
			} else {
				timelineRoot.appendChild(list);
			}
		}
		return list;
	}

    function matchesFiltersForEvent(ev) {
        const sid = String(ev.streamer_id || "");
        const tags = Array.isArray(ev.tag_ids) ? ev.tag_ids.map(String) : [];
        const streamerMatch = (selectedStreamerIds.size === 0) || selectedStreamerIds.has(sid);
        let tagMatch = true;
        if (selectedTagIds.size > 0) {
            tagMatch = tags.some(t => selectedTagIds.has(String(t)));
        }
        return streamerMatch && tagMatch;
    }

    function appendEventToTimeline(ev, position = "append") {
        // Avoid duplicates if the same event is loaded multiple times
        const key = (ev.slug ? String(ev.slug) : String(ev.id));
        if (byId(key)) {
            return;
        }
		const list = ensureMonthSection(ev.year, ev.month, ev.month_name, position);
		const article = document.createElement("article");
		article.className = "timeline-item";
		article.id = key;
        if (ev.streamer_id) article.dataset.streamerId = String(ev.streamer_id);
        if (Array.isArray(ev.tag_ids) && ev.tag_ids.length) article.dataset.tagIds = ev.tag_ids.join(',');
		article.innerHTML = `
            <div class="timeline-dot"></div>
			<div class="timeline-card">
				<header class="timeline-card-header">
					<h3 class="timeline-title">
						<a href="#${key}"></a>
					</h3>
					<div class="timeline-meta">
						<time class="timeline-date">${ev.date_display}</time>
						<div class="timeline-streamer" style="display:none;">
							<span class="streamer-inline">
								<img class="streamer-inline-icon" style="display:none;" />
								<span class="streamer-inline-name"></span>
							</span>
						</div>
					</div>
				</header>
				<div class="timeline-body">
					<div class="timeline-video">${ev.embed_html}</div>
					<p class="timeline-text"></p>
					<div class="body-actions">
						<button class="button button-secondary copy-link" data-key="${key}">Copy Link</button>
					</div>
					<div class="event-tags"></div>
				</div>
			</div>
		`;
		// Fill text safely
		qs(".timeline-title a", article).textContent = ev.title;
		qs(".timeline-text", article).textContent = ev.body.length > 280 ? ev.body.slice(0, 279).trimEnd() + "…" : ev.body;
        // If there is no embed, show placeholder text like SSR
        try {
            const videoWrap = qs(".timeline-video", article);
            if (videoWrap && (!ev.embed_html || String(ev.embed_html).trim() === "")) {
                videoWrap.innerHTML = "<p>No video available.</p>";
            }
        } catch {}
        // Render streamer if provided
        try {
            if (ev.streamer_name) {
                const block = qs(".timeline-streamer", article);
                if (block) {
                    const nameEl = qs(".streamer-inline-name", block);
                    if (nameEl) nameEl.textContent = ev.streamer_name;
                    const icon = qs(".streamer-inline-icon", block);
                    if (icon && ev.streamer_icon_url) {
                        icon.src = ev.streamer_icon_url;
                        icon.style.display = "";
                    }
                    block.style.display = "";
                }
            }
        } catch {}
        // Render tags if available
        try {
            if (Array.isArray(ev.tag_ids) && ev.tag_ids.length && Array.isArray(window.HOT_TAGS)) {
                const map = {};
                for (const t of window.HOT_TAGS) map[String(t.id)] = String(t.name || "").toLowerCase();
                const tagsWrap = qs(".event-tags", article);
                if (tagsWrap) {
                    for (const tid of ev.tag_ids) {
                        const name = map[String(tid)];
                        if (!name) continue;
                        const span = document.createElement("span");
                        span.className = "tag-chip";
                        const inner = document.createElement("span");
                        inner.className = "tag-name";
                        inner.textContent = name;
                        span.appendChild(inner);
                        tagsWrap.appendChild(span);
                    }
                    if (!tagsWrap.firstChild) tagsWrap.remove();
                }
            }
        } catch {}
        // Append original Clip ID inline next to Copy Link if present
        if (ev.original_clip_url) {
            const actions = qs(".body-actions", article);
            if (actions) {
                const span = document.createElement("span");
                span.className = "timeline-original-inline";
            const a = document.createElement("a");
            a.className = "mono-url";
            a.href = ev.original_clip_url;
            a.target = "_blank";
            a.rel = "noopener noreferrer";
            // Prefer server-provided clip ID, otherwise derive from URL
            const id = ev.original_clip_id || (ev.original_clip_url.split('/').pop() || '').split('?')[0];
            a.textContent = id || ev.original_clip_url;
                span.append("Archived directly from Clip ID ");
                span.appendChild(a);
                actions.appendChild(span);
            }
        }
		// Append
		const targetList = qs(`.timeline-list[data-anchor="${ev.month_anchor}"]`, timelineRoot) || ensureMonthSection(ev.year, ev.month, ev.month_name, position);
		if (position === "prepend") {
			targetList.insertBefore(article, targetList.firstChild);
		} else {
			targetList.appendChild(article);
		}
        // Set initial visibility based on current filters; applyFilter will reconcile globally
        try {
            if (!matchesFiltersForEvent(ev)) {
                article.style.display = 'none';
            }
        } catch {}
	}

	function buildSidebarFromMeta(meta) {
		if (!monthNav) return;
		cfg.meta = meta;
        // ensure pagination math can run
        cfg.total = Array.isArray(meta) ? meta.length : 0;
        if (!cfg.limit) cfg.limit = 15;
		monthNav.innerHTML = "";
		let currentAnchor = null;
		let li = null;
		let ul = null;
		let count = 0;
		for (const e of meta) {
			const anchor = monthAnchor(e.year, e.month);
			if (anchor !== currentAnchor) {
				// finalize previous
				if (li) {
					const countEl = qs(".count", li);
					if (countEl) countEl.textContent = String(count);
				}
				// start new month block
				li = document.createElement("li");
				li.setAttribute("data-anchor", anchor);
				li.innerHTML = `
					<span class="month-label">${e.month_name} ${e.year}</span>
					<span class="count">0</span>
					<ul class="event-nav" data-anchor="${anchor}"></ul>
				`;
				monthNav.appendChild(li);
				ul = qs(`.event-nav[data-anchor="${anchor}"]`, li);
				currentAnchor = anchor;
				count = 0;
			}
            const a = document.createElement("a");
            const key = (e.slug ? String(e.slug) : String(e.id));
            a.href = `#${key}`;
            a.textContent = e.title;
            if (e.streamer_id != null) {
                a.setAttribute('data-streamer-id', String(e.streamer_id));
            }
            if (Array.isArray(e.tag_ids) && e.tag_ids.length) {
                a.setAttribute('data-tag-ids', e.tag_ids.map(String).join(','));
            }
			const item = document.createElement("li");
			item.appendChild(a);
			ul.appendChild(item);
			count += 1;
		}
		// finalize last
		if (li) {
			const countEl = qs(".count", li);
			if (countEl) countEl.textContent = String(count);
		}
	}

	function findIndexById(id) {
		if (!cfg.meta) return -1;
        const needle = String(id);
		for (let i = 0; i < cfg.meta.length; i++) {
			if (String(cfg.meta[i].id) === needle) return i;
		}
		return -1;
	}

	// removed deprecated month-index helpers

    function findIndexBySlug(slug) {
        if (!cfg.meta) return -1;
        const needle = String(slug);
        for (let i = 0; i < cfg.meta.length; i++) {
            if (String(cfg.meta[i].slug || "") === needle) return i;
        }
        return -1;
    }

    function getKeyForId(id) {
        const idx = findIndexById(id);
        if (idx >= 0) {
            const e = cfg.meta[idx];
            return String(e.slug || e.id);
        }
        return String(id);
    }

	async function loadMore() { // load next (older) page at the bottom
		if (!cfg || cfg.nextOffset >= cfg.total || loadingNext) return;
		loadingNext = true;
		try {
			const url = new URL(cfg.apiUrl, window.location.origin);
			url.searchParams.set("offset", String(cfg.nextOffset));
			url.searchParams.set("limit", String(cfg.limit || 15));
			const res = await fetch(url.toString(), { headers: { "Accept": "application/json" } });
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const data = await res.json();
            for (const ev of data.events) {
				appendEventToTimeline(ev, "append");
			}
			cfg.nextOffset += data.events.length;
            // Re-evaluate visibility after each batch so filters apply to new items
            applyFilter();
		} catch (e) {
			console.error(e);
		} finally {
			loadingNext = false;
			// If user remains at bottom and there are more pages, keep pulling
			if (cfg.nextOffset < cfg.total && bottomSentinelVisible()) {
				requestAnimationFrame(() => loadMore());
			}
		}
	}

	async function loadPrevious() { // load previous (newer) page at the top
		if (!cfg || cfg.beginOffset <= 0 || loadingPrev) return;
		const newOffset = Math.max(0, (cfg.beginOffset || 0) - (cfg.limit || 15));
		try {
			loadingPrev = true;
			const prevH = document.documentElement.scrollHeight;
			const url = new URL(cfg.apiUrl, window.location.origin);
			url.searchParams.set("offset", String(newOffset));
			url.searchParams.set("limit", String((cfg.limit || 15)));
			const res = await fetch(url.toString(), { headers: { "Accept": "application/json" } });
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const data = await res.json();
			// Insert in reverse order so the final order remains descending
			for (let i = data.events.length - 1; i >= 0; i--) {
				appendEventToTimeline(data.events[i], "prepend");
			}
            cfg.beginOffset = newOffset;
			// keep user's viewport anchored to the same content after prepending
			const newH = document.documentElement.scrollHeight;
			const delta = newH - prevH;
			if (delta > 0) {
				window.scrollBy(0, delta);
			}
            // Re-evaluate visibility after each prepend so filters stay consistent
            applyFilter();
		} catch (e) {
			console.error(e);
		} finally {
			loadingPrev = false;
			// If we're still at the top (sentinel visible), keep loading until we fill above
			if (cfg.beginOffset > 0 && topSentinelVisible()) {
				// Use rAF to allow layout to settle
				requestAnimationFrame(() => loadPrevious());
			}
		}
	}

	async function ensureLoadedAndScrollToId(id, downOnly = false) {
        const key = getKeyForId(id);
		const el = byId(String(key));
		if (el) {
			// First jump instantly (no animation) to avoid partial smooth scroll during cold load,
			// then fine-tune with smooth adjustments after layout settles.
			scrollElementIntoCenter(el, "auto");
			requestAnimationFrame(() => scrollElementIntoCenter(el, "smooth"));
			setTimeout(() => scrollElementIntoCenter(byId(String(key)), "smooth"), 250);
			return;
		}
		const index = findIndexById(id);
		if (index === -1) return;
		// Ensure beginOffset is defined
		if (typeof cfg.beginOffset !== "number") cfg.beginOffset = 0;
		// If target is above current window, load previous pages
		while (!downOnly && index < cfg.beginOffset && cfg.beginOffset > 0) {
			// eslint-disable-next-line no-await-in-loop
			await loadPrevious();
		}
		// If target is below current window, load next pages
		while (cfg.nextOffset <= index && cfg.nextOffset < cfg.total) {
			// eslint-disable-next-line no-await-in-loop
			await loadMore();
		}
		const el2 = byId(String(getKeyForId(id)));
		if (el2) {
			scrollElementIntoCenter(el2, "auto");
			requestAnimationFrame(() => scrollElementIntoCenter(el2, "smooth"));
			setTimeout(() => scrollElementIntoCenter(byId(String(getKeyForId(id))), "smooth"), 250);
		}
	}

	// ensureLoadedForMonth removed (no month-click navigation)

	// Build full sidebar on load
	(async () => {
		try {
			if (!cfg.metaUrl) return;
			const res = await fetch(cfg.metaUrl, { headers: { "Accept": "application/json" } });
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const data = await res.json();
			buildSidebarFromMeta(data.events);
            // Initial sidebar visibility pass (handles SSR no-events case)
            requestAnimationFrame(() => updateLeftSidebarVisibility());
			// If page loaded with #<id or slug>, jump directly to the page containing it
			const initialId = parseHashId(window.location.hash);
            const initialKey = parseHashKey(window.location.hash);
			cfg.limit = cfg.limit || 15;
			if ((initialId || initialKey) && Array.isArray(cfg.meta)) {
                let idx = -1;
                let idToLoad = null;
                if (initialId) { idx = findIndexById(initialId); idToLoad = initialId; }
                else if (initialKey) {
                    idx = findIndexBySlug(initialKey);
                    if (idx >= 0) idToLoad = cfg.meta[idx].id;
                }
				if (idx >= 0 && idToLoad != null) {
					const pageStart = Math.floor(idx / cfg.limit) * cfg.limit;
					// Clear existing timeline content except the top sentinel
					if (timelineRoot) {
						timelineRoot.innerHTML = "";
						const ts = document.createElement("div");
						ts.id = "top-sentinel";
						ts.style.height = "1px";
						timelineRoot.appendChild(ts);
                        // Recreate CTA under the sentinel so it exists after deep-link clears
                        const ctaWrap = document.createElement("div");
                        ctaWrap.className = "timeline-cta";
                        const a = document.createElement("a");
                        a.className = "cta-x";
                        a.href = "https://x.com/uglytwitch";
                        a.target = "_blank";
                        a.rel = "noopener noreferrer";
                        a.setAttribute("aria-label", "X (Twitter)");
                        a.innerHTML = `<span>Support us by following on</span>
<svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor" aria-hidden="true"><path d="M18.244 2H21L13.5 10.09 22 22h-6.557l-4.63-6.065L5.3 22H2.5l7.963-9.1L2 2h6.68l4.186 5.66L18.244 2Zm-1.152 18h1.8L7.01 4h-1.87l11.952 16Z"/></svg>`;
                        ctaWrap.appendChild(a);
                        timelineRoot.appendChild(ctaWrap);
					}
					cfg.beginOffset = pageStart;
					cfg.nextOffset = pageStart;
					// For deep links, do not auto-pull previous page immediately
					cfg.suppressTopLoad = true;
					// Load that page
					const url = new URL(cfg.apiUrl, window.location.origin);
					url.searchParams.set("offset", String(pageStart));
					url.searchParams.set("limit", String(cfg.limit));
					const res2 = await fetch(url.toString(), { headers: { "Accept": "application/json" } });
					if (res2.ok) {
						const data2 = await res2.json();
						for (const ev of data2.events) {
							appendEventToTimeline(ev, "append");
						}
						cfg.nextOffset = pageStart + data2.events.length;
                        // Defensive: ensure total is set
                        if (!Number.isFinite(cfg.total) || cfg.total <= 0) {
                            cfg.total = Array.isArray(cfg.meta) ? cfg.meta.length : cfg.nextOffset;
                        }
                        // Flush DOM and wait for layout to stabilize before attempting to measure/scroll
                        await new Promise(requestAnimationFrame);
                        await new Promise(requestAnimationFrame);
                        // If the target isn't on this page due to any drift, probe further pages until found
						await ensurePageContainsId(idToLoad);
                        // Final: scroll after one more frame and tiny delay
                        requestAnimationFrame(async () => {
                            await new Promise((r) => setTimeout(r, 50));
							const el = byId(String(getKeyForId(idToLoad)));
                            if (el) {
                                scrollElementIntoCenter(el, "auto");
                                requestAnimationFrame(() => scrollElementIntoCenter(el, "smooth"));
                            } else {
                                // Fallback to standard routine
								ensureLoadedAndScrollToId(idToLoad, true);
                            }
                        });
                        // Pause top loading briefly so we don't immediately prepend newer pages
                        deferTopLoadingUntil = Date.now() + 1200;
                        if (topObserver) { try { topObserver.disconnect(); } catch {} }
                        setTimeout(() => { enableTopIO(); }, 1200);
						// Do NOT auto-load previous page here; wait until the user scrolls
					}
				}
				enableTopIO();
                enableBottomIO();
			} else {
				// default: we already SSR'd the first page
				cfg.beginOffset = 0;
				enableTopIO();
                enableBottomIO();
			}
		} catch (e) {
			console.error("Failed to build sidebar", e);
		}
	})();

	// Support manual hash changes (id or slug)
	window.addEventListener("hashchange", () => {
		const key = parseHashKey(window.location.hash);
		if (!key) return;
		const el = byId(String(key));
		if (el) {
			scrollElementIntoCenter(el, "smooth");
			return;
		}
		const id = parseHashId(window.location.hash);
		if (id) ensureLoadedAndScrollToId(id);
	});
    // Also handle browser back/forward (popstate) by retrying after layout
    window.addEventListener("popstate", () => {
        const id = parseHashId(window.location.hash);
        if (id) setTimeout(() => ensureLoadedAndScrollToId(id), 100);
    });

    // Try to locate id by fetching adjacent pages if needed (prefer downward/older first)
    async function ensurePageContainsId(id) {
        if (byId(String(id))) return true;
        const targetIdx = findIndexById(id);
        if (targetIdx === -1) return false;
        // First, if target is below current window, pull down until included
        let safety = 10;
        while (!byId(String(id)) && cfg.nextOffset <= targetIdx && cfg.nextOffset < cfg.total && safety-- > 0) {
            // eslint-disable-next-line no-await-in-loop
            await loadMore();
        }
        // If still not found, pull up (newer) pages cautiously
        safety = 10;
        while (!byId(String(id)) && cfg.beginOffset > 0 && targetIdx < cfg.beginOffset && safety-- > 0) {
            // eslint-disable-next-line no-await-in-loop
            await loadPrevious();
        }
        return !!byId(String(id));
    }

    // Dynamic streamer filter (right sidebar pills) - multi-select toggle
    qsa('.sidebar-right .streamer-pill').forEach((pill) => {
        pill.addEventListener('click', (e) => {
            e.preventDefault();
            const id = pill.dataset.streamerId || null;
            if (!id) return;
            if (pill.classList.contains('active')) {
                pill.classList.remove('active');
                selectedStreamerIds.delete(String(id));
            } else {
                pill.classList.add('active');
                selectedStreamerIds.add(String(id));
            }
            applyFilter();
            updateClearButtons();
        });
    });

    // Tags filter - multi-select toggle
    qsa('.sidebar-right .tag-pill').forEach((pill) => {
        pill.addEventListener('click', (e) => {
            e.preventDefault();
            const id = pill.getAttribute('data-tag-id');
            if (!id) return;
            if (pill.classList.contains('active')) {
                pill.classList.remove('active');
                selectedTagIds.delete(String(id));
            } else {
                pill.classList.add('active');
                selectedTagIds.add(String(id));
            }
            applyFilter();
            updateClearButtons();
        });
    });

    // Clear buttons
    const clearStreamersBtn = qs('.sidebar-right .clear-streamers');
    const clearTagsBtn = qs('.sidebar-right .clear-tags');
    function updateClearButtons() {
        if (clearStreamersBtn) clearStreamersBtn.style.visibility = selectedStreamerIds.size > 0 ? 'visible' : 'hidden';
        if (clearTagsBtn) clearTagsBtn.style.visibility = selectedTagIds.size > 0 ? 'visible' : 'hidden';
    }
    if (clearStreamersBtn) {
        clearStreamersBtn.addEventListener('click', (e) => {
            e.preventDefault();
            selectedStreamerIds.clear();
            qsa('.sidebar-right .streamer-pill.active').forEach((p) => p.classList.remove('active'));
            applyFilter();
            updateClearButtons();
        });
    }
    if (clearTagsBtn) {
        clearTagsBtn.addEventListener('click', (e) => {
            e.preventDefault();
            selectedTagIds.clear();
            qsa('.sidebar-right .tag-pill.active').forEach((p) => p.classList.remove('active'));
            applyFilter();
            updateClearButtons();
        });
    }
    // Initialize visibility on load
    updateClearButtons();

    function clearTimelineAndReset() {
        if (timelineRoot) {
            timelineRoot.innerHTML = "";
            const ts = document.createElement("div");
            ts.id = "top-sentinel";
            ts.style.height = "1px";
            timelineRoot.appendChild(ts);
            // Recreate CTA below sentinel after full clear
            const ctaWrap = document.createElement("div");
            ctaWrap.className = "timeline-cta";
            const a = document.createElement("a");
            a.className = "cta-x";
            a.href = "https://x.com/uglytwitch";
            a.target = "_blank";
            a.rel = "noopener noreferrer";
            a.setAttribute("aria-label", "X (Twitter)");
            a.innerHTML = `<span>Support us by following on</span>
<svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor" aria-hidden="true"><path d="M18.244 2H21L13.5 10.09 22 22h-6.557l-4.63-6.065L5.3 22H2.5l7.963-9.1L2 2h6.68l4.186 5.66L18.244 2Zm-1.152 18h1.8L7.01 4h-1.87l11.952 16Z"/></svg>`;
            ctaWrap.appendChild(a);
            timelineRoot.appendChild(ctaWrap);
        }
        cfg.beginOffset = 0;
        cfg.nextOffset = 0;
        enableTopIO();
        enableBottomIO();
    }

    function applyFilter() {
        const items = qsa('.timeline-item', timelineRoot);
        items.forEach((it) => {
            const sid = it.dataset.streamerId || '';
            const tagCsv = it.dataset.tagIds || '';
            const tagSet = new Set(tagCsv ? tagCsv.split(',') : []);
            const streamerMatch = (selectedStreamerIds.size === 0) || selectedStreamerIds.has(String(sid));
            let tagMatch = true;
            if (selectedTagIds.size > 0) {
                tagMatch = false;
                for (const t of selectedTagIds) { if (tagSet.has(String(t))) { tagMatch = true; break; } }
            }
            it.style.display = (streamerMatch && tagMatch) ? '' : 'none';
        });
        // Hide empty month sections
        qsa('.timeline-list', timelineRoot).forEach((list) => {
            const anyVisible = qsa('.timeline-item', list).some(x => x.style.display !== 'none');
            const header = list.previousElementSibling;
            list.style.display = anyVisible ? '' : 'none';
            if (header && header.classList.contains('timeline-month')) header.style.display = anyVisible ? '' : 'none';
        });

        // Ensure first visible month header has no top margin
        const visibleHeaders = qsa('.timeline-month', timelineRoot).filter(h => h.style.display !== 'none');
        visibleHeaders.forEach((h, i) => {
            h.style.marginTop = i === 0 ? '0' : '12px';
        });

        // Update left sentinel (monthNav) to show only matching events
        if (monthNav) {
            const monthLis = qsa(':scope > li', monthNav);
            monthLis.forEach((li) => {
                const links = qsa('.event-nav a', li);
                let visibleCount = 0;
                links.forEach((a) => {
                    const sid = a.getAttribute('data-streamer-id') || '';
                    const tagCsv = a.getAttribute('data-tag-ids') || '';
                    const tagSet = new Set(tagCsv ? tagCsv.split(',') : []);
                    const streamerOk = (selectedStreamerIds.size === 0 || selectedStreamerIds.has(String(sid)));
                    let tagOk = true;
                    if (selectedTagIds.size > 0) {
                        tagOk = false;
                        for (const t of selectedTagIds) { if (tagSet.has(String(t))) { tagOk = true; break; } }
                    }
                    const show = streamerOk && tagOk;
                    a.parentElement.style.display = show ? '' : 'none';
                    if (show) visibleCount += 1;
                });
                const countEl = qs('.count', li);
                if (countEl) countEl.textContent = String(visibleCount);
                li.style.display = visibleCount > 0 ? '' : 'none';
            });
        }

        // Do not rebuild the timeline on filter changes.
        // We keep existing DOM and simply hide/show items so that
        // clearing all filters restores the pre-filter view instantly.
        updateClearButtons();
        updateLeftSidebarVisibility();
    }

    function updateLeftSidebarVisibility() {
        const left = qs('.sidebar');
        if (!left) return;
        const anyVisibleTimeline = qsa('.timeline-item', timelineRoot).some(x => x.style.display !== 'none');
        let anyVisibleSentinel = false;
        if (monthNav) {
            const links = qsa('.event-nav a', monthNav);
            anyVisibleSentinel = links.some(a => {
                const li = a.parentElement;
                return li && li.style.display !== 'none';
            });
        }
        left.style.display = (anyVisibleTimeline && anyVisibleSentinel) ? '' : 'none';
    }
	// Handle clicks in month sidebar for event links only
	if (monthNav) {
		monthNav.addEventListener("click", async (e) => {
			const a = e.target.closest("a");
			if (!a) return;
			const href = a.getAttribute("href") || "";
			if (!href.startsWith("#")) return;
			e.preventDefault();
            const key = parseHashKey(href);
            if (!key) return;
            history.replaceState(null, "", `#${key}`);
            const el = byId(String(key));
            if (el) { scrollElementIntoCenter(el, "smooth"); return; }
            // resolve to numeric id via meta and load
            if (Array.isArray(cfg.meta)) {
                const idx = findIndexBySlug(key);
                if (idx >= 0) {
                    const id = cfg.meta[idx].id;
                    await ensureLoadedAndScrollToId(id);
                    return;
                }
            }
		});
	}

	// Copy Link button handling (event delegation)
	if (timelineRoot) {
		timelineRoot.addEventListener("click", async (e) => {
            // Intercept clicks on event titles to prevent native hash jump jitter
            const titleLink = e.target.closest(".timeline-title a");
            if (titleLink) {
                e.preventDefault();
                const href = titleLink.getAttribute("href") || "";
                const key = parseHashKey(href);
                if (key) {
                    history.replaceState(null, "", `#${key}`);
                    const el = byId(String(key));
                    if (el) { scrollElementIntoCenter(el, "smooth"); }
                    else {
                        const id = parseHashId(href);
                        if (id) await ensureLoadedAndScrollToId(id);
                    }
                }
                return;
            }
			const btn = e.target.closest(".copy-link");
			if (!btn) return;
			e.preventDefault();
			// Prevent spamming: allow only one feedback flash at a time per button
			if (btn.dataset.animating === "1") return;
			const key = btn.getAttribute("data-key") || btn.getAttribute("data-id");
			if (!key) return;
			const url = `${window.location.origin}${window.location.pathname}#${key}`;
			try {
				if (navigator.clipboard && navigator.clipboard.writeText) {
					await navigator.clipboard.writeText(url);
				} else {
					// Fallback
					const ta = document.createElement("textarea");
					ta.value = url;
					document.body.appendChild(ta);
					ta.select();
					document.execCommand("copy");
					document.body.removeChild(ta);
				}
				// Visual feedback: briefly light up the button
				btn.dataset.animating = "1";
				btn.classList.add("copy-lit");
				const FLASH_MS = 800;
				setTimeout(() => {
					btn.classList.remove("copy-lit");
					btn.dataset.animating = "0";
				}, FLASH_MS);
			} catch (err) {
				console.error("Copy failed", err);
			}
		});
	}

	function enableTopIO() {
		if (!("IntersectionObserver" in window)) return;
        if (deferTopLoadingUntil && Date.now() < deferTopLoadingUntil) return;
		const ts = byId("top-sentinel");
		if (!ts) return;
		if (topObserver) {
			try { topObserver.disconnect(); } catch {}
		}
		topObserver = new IntersectionObserver((entries) => {
			for (const entry of entries) {
				if (entry.isIntersecting) {
					// Skip the very first automatic load when arriving via deep link
					if (cfg && cfg.suppressTopLoad) { cfg.suppressTopLoad = false; return; }
                    if (deferTopLoadingUntil && Date.now() < deferTopLoadingUntil) return;
					loadPrevious();
				}
			}
		}, { rootMargin: "200px 0px 0px 0px" });
		topObserver.observe(ts);
	}

	function enableBottomIO() {
		if (!("IntersectionObserver" in window)) return;
		if (!sentinel) return;
		if (bottomObserver) {
			try { bottomObserver.disconnect(); } catch {}
		}
		bottomObserver = new IntersectionObserver((entries) => {
			for (const entry of entries) {
				if (entry.isIntersecting) {
					loadMore();
				}
			}
		}, { rootMargin: "200px" });
		bottomObserver.observe(sentinel);
	}

	// Fallback: also trigger top loading when user scrolls near the top of page
	let topScrollTicking = false;
	window.addEventListener("scroll", () => {
		if (topScrollTicking) return;
		topScrollTicking = true;
		requestAnimationFrame(() => {
			topScrollTicking = false;
			if (window.scrollY <= 200) {
				// Respect suppression flag for first deep-link landing
				if (cfg && cfg.suppressTopLoad) { cfg.suppressTopLoad = false; return; }
                if (deferTopLoadingUntil && Date.now() < deferTopLoadingUntil) return;
				loadPrevious();
			}
		});
	}, { passive: true });

 // Sidebar max-height calculation to keep bottoms above the fixed footer
 function applySidebarMaxHeights() {
     const rootStyle = getComputedStyle(document.documentElement);
     const footerVar = rootStyle.getPropertyValue('--footer-h') || '64px';
     const footerH = parseInt(footerVar, 10) || 64;
     const gap = 16; // small breathing room above footer
     const fortyVh = Math.floor(window.innerHeight * 0.4) - 8; // streamers cap
     const sixtyVh = Math.floor(window.innerHeight * 0.6) - 8; // tags cap
     // Left sidebar (Sentinel) - use full available height
     const leftInner = qs('.sidebar .sidebar-inner');
     if (leftInner) {
         const top = leftInner.getBoundingClientRect().top;
         const avail = window.innerHeight - footerH - top - gap;
         const maxH = Math.max(80, avail);
         leftInner.style.maxHeight = `${maxH}px`;
         leftInner.style.overflow = 'auto';
     }
     // Right sidebar sections (Streamers / Tags) - cap to half viewport each
     const rightInners = qsa('.sidebar-right .sidebar-inner');
     rightInners.forEach((el, idx) => {
         const top = el.getBoundingClientRect().top;
         const avail = window.innerHeight - footerH - top - gap;
         const cap = (idx === 0 ? fortyVh : sixtyVh);
         const maxH = Math.max(80, Math.min(avail, cap));
         el.style.maxHeight = `${maxH}px`;
         el.style.overflow = 'auto';
     });
 }

 // Run on load and on resize to be responsive across resolutions
	window.addEventListener('load', applySidebarMaxHeights);
 window.addEventListener('resize', applySidebarMaxHeights);

 // Simple modal system
 function setupModals() {
     const openers = qsa('[data-modal]');
     const body = document.body;
     function open(id) {
         const el = byId(`modal-${id}`);
         if (!el) return;
         el.classList.add('show');
         el.setAttribute('aria-hidden', 'false');
     }
     function close(el) {
         el.classList.remove('show');
         el.setAttribute('aria-hidden', 'true');
     }
     openers.forEach((a) => {
         a.addEventListener('click', (e) => {
             e.preventDefault();
             const id = a.getAttribute('data-modal');
             if (id) open(id);
         });
     });
     document.addEventListener('click', (e) => {
         const target = e.target;
         const modal = target.closest('.modal');
         if (target.matches('[data-close]') && modal) {
             e.preventDefault();
             close(modal);
         } else if (target.classList && target.classList.contains('modal')) {
             close(target);
         }
     });
     document.addEventListener('keydown', (e) => {
         if (e.key === 'Escape') {
             qsa('.modal.show').forEach((m) => m.classList.remove('show'));
         }
     });
 }
 setupModals();
})();



