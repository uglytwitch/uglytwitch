## Ugly Side of Twitch — Overview

### Goal
Track and publish a timeline of incidents from Twitch streams with a title, body, and attached video (from downloaded Twitch Clips on Backblaze B2). Provide a simple backend to add, edit, and delete events.

### Tech Stack
- Backend: Flask (Python), SQLite (no ORM), yt-dlp, ffmpeg, Backblaze B2 (S3 via boto3)
- Frontend: Jinja templates + CSS + vanilla JS (infinite scroll, filters, deep-linking)

### Key Routes
- `GET /`: scrollable timeline (one-pager)
- `/#<slug-or-id>`: deep-link anchor, centers the event on cold load
- `GET /admin` → redirects to `/admin/events`
- Events: `/admin/events`, `/admin/events/new`, `/admin/events/<id>/edit`
- Streamers: `/admin/streamers`, `/admin/streamers/new`, `/admin/streamers/<id>/edit`
- Tags: `/admin/tags`, `/admin/tags/new`, `/admin/tags/<id>/edit`
- Pages: `/admin/pages`, `/admin/pages/new`, `/admin/pages/<id>/edit`

### Data
- `events(id, slug, title, body, video_url, original_clip_url, thumbnail_url, created_at)`
- `event_videos(id, event_id, quality_label, mime, filesize, duration_s, b2_key, public_url, created_at)`
- `streamers(id, name UNIQUE, icon_url, created_at)` (icons: PNG, square 32–128px, uploaded to `/assets/icons/streamer_<id>.png`)
- `tags(id, name UNIQUE, created_at)`; `event_tags(event_id, tag_id)`
- `pages(id, title, content, position, visible)`

### Video
- HTML5 `<video>` with multiple `<source>` variants (e.g., 1080p/720p), served from B2/CDN.
- Variant and thumbnail organization per event folder `events/<event_id>/`.

### Local Development
1. Create venv, `pip install -r requirements.txt`
2. Optional: `python scripts/setup_ffmpeg.py`
3. Add `env.txt` with B2 creds and base URLs (see README)
4. `python app.py`, visit `http://127.0.0.1:5000/`
5. Seeding: `python scripts/reset_and_seed.py` (wipes Events/Streamers/Tags + B2, preserves Pages)
6. Wipe only: `python scripts/wipe_dummy_data.py`

### Behavior notes
- Deep-link cold loads: the app fetches only the page containing the target event, centers it, and defers other loads until scroll.
- Infinite scroll is bidirectional; list order and month breaks are kept stable.
- Sidebars auto-hide when no results (empty dataset or filtered state).

### Future Enhancements
- Authentication for admin
- Search
- Background worker for long-running ingests