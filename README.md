## Ugly Side of Twitch — Timeline + Admin (Flask)

Scrollable timeline of incidents with deep links, bidirectional infinite scroll, and a lightweight admin for Events, Streamers, Tags, and Pages.

### Features
- Timeline on `/` with HTML5 videos (Backblaze B2 hosted)
- In-page deep links via `/#<slug-or-id>` with reliable cold‑load centering
- Sticky month headers; bidirectional infinite scroll
- Left “Sentinel” (months/events) and right Streamers/Tags filters; sidebars auto‑hide when empty/no results
- Admin (no auth) for Events/Streamers/Tags/Pages
- SQLite database; self‑contained local setup

### Video pipeline
- Events use HTML5 `<video>` with multiple `<source>` variants.
- Admin “New Event” supports two inputs (either/or):
  - Twitch Clip URL: downloads all heights via `yt-dlp`, remuxes to MP4 with `ffmpeg`, uploads to Backblaze B2 (S3 API). Fails safe if no variant uploads.
  - Direct video upload: saved to `events/<event_id>/<event_id>.mp4`; a thumbnail is generated.

### Environment
Set these environment variables (use `env.txt` locally):

```
B2_ACCESS_KEY_ID=...
B2_SECRET_ACCESS_KEY=...
B2_BUCKET=...
B2_REGION=us-west-004
B2_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
B2_BASE_URL=https://<bucket>.s3.us-west-004.backblazeb2.com  # or your CDN URL
# Optional
FFMPEG_PATH=C:\path\to\ffmpeg.exe
YTDLP_PATH=C:\path\to\yt-dlp.exe
SECRET_KEY=dev-secret-change-me
```

### Prerequisites
- Python 3.10+ recommended (Windows 11 compatible)

### Setup (Windows)

Using PowerShell:

```powershell
py -m venv .venv
.venv\Scripts\Activate
pip install -r requirements.txt
python scripts/setup_ffmpeg.py
py app.py
```

Using Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
python scripts/setup_ffmpeg.py
python app.py
```

Then open `http://127.0.0.1:5000/` in your browser.

### Admin routes
- `/admin` → redirects to `/admin/events`
- Events: `/admin/events`, `/admin/events/new`, `/admin/events/<id>/edit`
- Streamers: `/admin/streamers`, `/admin/streamers/new`, `/admin/streamers/<id>/edit`
- Tags: `/admin/tags`, `/admin/tags/new`, `/admin/tags/<id>/edit`
- Pages: `/admin/pages`, `/admin/pages/new`, `/admin/pages/<id>/edit`

### Streamer icons
- PNG only, square, between 32×32 and 128×128. Larger images are downscaled to fit.
- Stored at `/assets/icons/streamer_<id>.png` in B2 (UI resolves to canonical path).

### Scripts
- `scripts/setup_ffmpeg.py` — fetch a static ffmpeg (optional)

### Notes
- Ensure your B2 bucket allows GET from your origin (CORS). Prefer virtual‑host style `B2_BASE_URL` like `https://<bucket>.s3.us-west-004.backblazeb2.com`.
- Deep links use `#<slug-or-id>`; slugs are preferred when present.

### Structure

```
app.py
templates/
  base.html
  index.html
  admin_*.html
static/
  styles.css
  app.js
requirements.txt
scripts/
  setup_ffmpeg.py
docs/
  overview.md
```

### Data Model

`events`:
- `id` INTEGER PRIMARY KEY
- `slug` TEXT UNIQUE
- `title` TEXT
- `body` TEXT
- `video_url` TEXT (backward compat; first variant URL)
- `original_clip_url` TEXT (Twitch Clip URL used for ingestion)
- `thumbnail_url` TEXT
- `created_at` TEXT (UTC)

`event_videos`:
- `id` INTEGER PRIMARY KEY
- `event_id` INTEGER (FK)
- `quality_label` TEXT (e.g., 1080p, 720p)
- `mime` TEXT (e.g., video/mp4)
- `filesize` INTEGER
- `duration_s` REAL
- `b2_key` TEXT (B2 object key)
- `public_url` TEXT (B2/CDN URL)
- `created_at` TEXT

`streamers(id, name UNIQUE, icon_url, created_at)` — optional square icon

`tags(id, name UNIQUE, created_at)` and `event_tags(event_id, tag_id)`

`pages(id, title, content, position, visible)` — feeds footer modals

### License
MIT


