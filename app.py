import os
import re
import sqlite3
from datetime import datetime
from typing import Optional
import uuid
import tempfile
import shutil
import subprocess
from urllib.request import urlretrieve

import boto3
from botocore.config import Config as BotoConfig
from yt_dlp import YoutubeDL
import logging
from PIL import Image

from flask import Flask, render_template, request, redirect, url_for, flash, abort, jsonify, get_flashed_messages, session, Response
from markupsafe import Markup
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash


DATABASE_PATH = os.environ.get("DATABASE_PATH") or os.path.join(os.path.dirname(__file__), "database.db")


def create_app() -> Flask:
	app = Flask(__name__)
	# Load env from local files if present (env.txt or .env) for local dev
	load_env_from_files(["env.txt", ".env"])
	app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
	app.config["DATABASE"] = DATABASE_PATH
	# Be proxy-aware and prefer https when building external URLs
	app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
	app.config["PREFERRED_URL_SCHEME"] = "https"
	# Harden cookies when running on a platform like Render
	if os.environ.get("RENDER"):
		app.config.update(
			SESSION_COOKIE_SECURE=True,
			SESSION_COOKIE_HTTPONLY=True,
			SESSION_COOKIE_SAMESITE="Lax",
		)
	# basic logging setup
	log_dir = os.path.join(os.path.dirname(__file__), "logs")
	os.makedirs(log_dir, exist_ok=True)
	log_path = os.path.join(log_dir, "app.log")
	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.FileHandler(log_path), logging.StreamHandler()])
	app.logger = logging.getLogger("uglytwitch")

	ensure_database_initialized(app.config["DATABASE"])

	# Protect all /admin routes with HTTP Basic Auth
	@app.before_request
	def _protect_admin():
		path = request.path or ""
		if not path.startswith("/admin"):
			return
		admin_user = os.environ.get("ADMIN_USER")
		admin_pw_hash = os.environ.get("ADMIN_PASSWORD_HASH")
		# Fail closed if credentials are not configured in production
		if not admin_user or not admin_pw_hash:
			return Response("Admin is locked", 503)
		auth = request.authorization
		if not auth or auth.username != admin_user or not check_password_hash(admin_pw_hash, auth.password or ""):
			return Response("Auth required", 401, {"WWW-Authenticate": 'Basic realm="Admin"'})

	# Health endpoint for Render
	@app.route("/healthz")
	def healthz():
		return jsonify({"ok": True}), 200

	@app.context_processor
	def inject_helpers():
		return {
			"format_datetime": format_datetime,
			"render_video_player": render_video_player,
			"excerpt": excerpt,
			"format_date_input": format_date_input,
		}

	@app.route("/")
	def index():
		# Optional streamer filter (?streamer=1,2)
		streamer_ids_q = request.args.get("streamer", "").strip()
		# Load a limited number initially for performance; older events are fetched via API
		if streamer_ids_q:
			try:
				ids = [int(s) for s in streamer_ids_q.split(",") if s.strip().isdigit()]
			except Exception:
				ids = []
			if ids:
				conn = get_db_connection(DATABASE_PATH)
				try:
					qmarks = ",".join(["?"] * len(ids))
					cur = conn.execute(
						f"""
						SELECT e.* FROM events e
						JOIN event_streamers es ON es.event_id = e.id
						WHERE es.streamer_id IN ({qmarks})
						GROUP BY e.id
						ORDER BY created_at DESC, id DESC
						""",
						tuple(ids),
					)
					events = cur.fetchall()
				finally:
					conn.close()
			else:
				events = fetch_all_events(order_by="-created_at", limit=15, offset=0)
		else:
			events = fetch_all_events(order_by="-created_at", limit=15, offset=0)
		groups = group_events_by_month(events)
		total = count_events()
		videos_map = fetch_event_videos_map([e["id"] for e in events])
		# Show only streamers/tags that are actually used by at least one event
		streamers = fetch_streamers_with_events()
		event_streamers = fetch_event_primary_streamers_map([e["id"] for e in events])
		tags = fetch_tags_with_events()
		event_tags_map = fetch_event_tags_map([e["id"] for e in events])
		tags_json = [{"id": int(t["id"]), "name": t["name"]} for t in tags]
		return render_template("index.html", events=events, groups=groups, total=total, videos_map=videos_map, streamers=streamers, selected_streamers=streamer_ids_q, event_streamers=event_streamers, tags=tags, event_tags_map=event_tags_map, tags_json=tags_json, pages=fetch_pages(visible_only=True))
	
	@app.route("/api/events")
	def api_events():
		try:
			offset = int(request.args.get("offset", "0"))
			limit = int(request.args.get("limit", "15"))
		except Exception:
			return jsonify({"error": "invalid pagination"}), 400
		events = fetch_all_events(order_by="-created_at", limit=limit, offset=offset)
		total = count_events()
		videos_map = fetch_event_videos_map([e["id"] for e in events])
		event_streamers = fetch_event_primary_streamers_map([e["id"] for e in events])
		event_tags = fetch_event_tags_map([e["id"] for e in events])
		payload = []
		for ev in events:
			dt = parse_datetime(ev["created_at"])
			year = dt.year if dt else None
			month = dt.month if dt else None
			month_name = dt.strftime("%B") if dt else ""
			anchor = f"y{year}-{month:02d}" if (year and month) else ""
			# Build embed HTML from stored variants only
			vids = videos_map.get(ev["id"]) if videos_map else None
			embed_html = str(render_video_player(vids)) if vids else ""
			payload.append({
				"id": ev["id"],
				"slug": ev.get("slug"),
				"title": ev["title"],
				"body": ev["body"],
				"video_url": ev["video_url"],
				"original_clip_url": ev.get("original_clip_url") if isinstance(ev, dict) else ev["original_clip_url"],
				"original_clip_id": (ev.get("original_clip_url") if isinstance(ev, dict) else ev["original_clip_url"]).rsplit("/", 1)[-1].split("?")[0] if (ev.get("original_clip_url") if isinstance(ev, dict) else ev["original_clip_url"]) else None,
				"created_at": ev["created_at"],
				"date_display": format_datetime(ev["created_at"]),
				"month_anchor": anchor,
				"month": month,
				"month_name": month_name,
				"year": year,
				"embed_html": embed_html,
				"streamer_id": (event_streamers.get(ev["id"], {}) or {}).get("id"),
				"streamer_name": (event_streamers.get(ev["id"], {}) or {}).get("name"),
				"streamer_icon_url": (event_streamers.get(ev["id"], {}) or {}).get("icon_url"),
			"tag_ids": event_tags.get(ev["id"], []) or [],
			})
		return jsonify({
			"events": payload,
			"offset": offset,
			"limit": limit,
			"total": total,
			"hasMore": offset + len(events) < total,
		})
	
	@app.route("/api/events/meta")
	def api_events_meta():
		events = fetch_all_events(order_by="-created_at", limit=None, offset=0)
		payload = []
		# include primary streamer id and tags for filtering in sidebar
		streamers_map = fetch_event_primary_streamers_map([ev["id"] for ev in events]) if events else {}
		tags_map = fetch_event_tags_map([ev["id"] for ev in events]) if events else {}
		for ev in events:
			dt = parse_datetime(ev["created_at"])
			if not dt:
				continue
			year = dt.year
			month = dt.month
			month_name = dt.strftime("%B")
			anchor = f"y{year}-{month:02d}"
			payload.append({
				"id": ev["id"],
				"slug": ev.get("slug"),
				"title": ev["title"],
				"created_at": ev["created_at"],
				"date_display": format_datetime(ev["created_at"]),
				"month_anchor": anchor,
				"month": month,
				"month_name": month_name,
				"year": year,
				"streamer_id": (streamers_map.get(ev["id"], {}) or {}).get("id"),
				"tag_ids": tags_map.get(ev["id"], []) or [],
			})
		return jsonify({"events": payload, "total": len(payload)})

	# --- Admin (no auth for simplicity) ---
	@app.route("/admin/events")
	def admin_events_list():
		events = fetch_all_events(order_by="-created_at")
		event_ids = [e["id"] for e in events] if events else []
		es_map = fetch_event_primary_streamers_map(event_ids) if event_ids else {}
		tags_map = fetch_event_tags_map(event_ids) if event_ids else {}
		# Build a simple id->name map for tags
		all_tags = fetch_all_tags()
		tags_by_id = {int(t["id"]): t["name"] for t in all_tags} if all_tags else {}
		return render_template(
			"admin_events_list.html",
			events=events,
			event_streamers=es_map,
			event_tags_map=tags_map,
			tags_by_id=tags_by_id,
			pages=fetch_pages(visible_only=True),
			admin_mode=True,
		)
	@app.route("/admin")
	def admin_root():
		return redirect(url_for("admin_events_list"))

	# Pages admin
	@app.route("/admin/pages")
	def admin_pages():
		pages = fetch_pages(visible_only=False)
		return render_template("admin_pages_list.html", pages=pages, admin_mode=True)

	@app.route("/admin/pages/new", methods=["GET", "POST"])
	def admin_pages_new():
		if request.method == "POST":
			title = request.form.get("title", "").strip()
			content = request.form.get("content", "").strip()
			# position constraints: 1..(max+1); shift others at/after chosen position
			try:
				position = int(request.form.get("position", "1") or 1)
			except Exception:
				position = 1
			visible = 1 if request.form.get("visible") == "on" else 0
			if not title:
				flash("Title is required.", "error")
			else:
				conn = get_db_connection(DATABASE_PATH)
				try:
					# Determine bounds and clamp
					max_pos = conn.execute("SELECT COALESCE(MAX(position), 0) FROM pages").fetchone()[0]
					if position < 1:
						position = 1
					if position > (max_pos + 1):
						position = max_pos + 1
					# Shift existing pages at/after position up by one
					conn.execute("UPDATE pages SET position = position + 1 WHERE position >= ?", (position,))
					# Insert new page
					conn.execute("INSERT INTO pages (title, content, position, visible) VALUES (?, ?, ?, ?)", (title, content, position, visible))
					conn.commit()
					flash("Page created.", "success")
					return redirect(url_for("admin_pages"))
				except Exception as e:
					flash(f"Page creation failed: <code class='mono'>{e}</code>", "error")
				finally:
					conn.close()
		# GET
		conn = get_db_connection(DATABASE_PATH)
		try:
			max_pos = conn.execute("SELECT COALESCE(MAX(position), 0) FROM pages").fetchone()[0]
		finally:
			conn.close()
		return render_template("admin_pages_form.html", mode="new", page=None, pages=fetch_pages(visible_only=True), admin_mode=True, max_position=max_pos)

	@app.route("/admin/pages/<int:page_id>/edit", methods=["GET", "POST"])
	def admin_pages_edit(page_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			pg = conn.execute("SELECT * FROM pages WHERE id = ?", (page_id,)).fetchone()
		finally:
			conn.close()
		if not pg:
			abort(404)
		if request.method == "POST":
			title = request.form.get("title", "").strip()
			content = request.form.get("content", "").strip()
			try:
				position = int(request.form.get("position", "1") or 1)
			except Exception:
				position = 1
			visible = 1 if request.form.get("visible") == "on" else 0
			if not title:
				flash("Title is required.", "error")
			else:
				conn = get_db_connection(DATABASE_PATH)
				try:
					# Clamp target bounds based on current max
					max_pos = conn.execute("SELECT COALESCE(MAX(position), 0) FROM pages").fetchone()[0]
					if position < 1:
						position = 1
					if position > (max_pos + 1):
						position = max_pos + 1
					# Get current position
					cur_pos = int(pg["position"])
					if position != cur_pos:
						if position < cur_pos:
							# Move up: shift pages in [position, cur_pos-1] down by 1
							conn.execute("UPDATE pages SET position = position + 1 WHERE position >= ? AND position < ? AND id <> ?", (position, cur_pos, page_id))
						else:
							# Move down: shift pages in (cur_pos, position] up by 1
							conn.execute("UPDATE pages SET position = position - 1 WHERE position <= ? AND position > ? AND id <> ?", (position, cur_pos, page_id))
					# Update this page
					conn.execute("UPDATE pages SET title = ?, content = ?, position = ?, visible = ? WHERE id = ?", (title, content, position, visible, page_id))
					conn.commit()
					flash("Page updated.", "success")
					return redirect(url_for("admin_pages"))
				finally:
					conn.close()
		# GET
		conn2 = get_db_connection(DATABASE_PATH)
		try:
			max_pos = conn2.execute("SELECT COALESCE(MAX(position), 0) FROM pages").fetchone()[0]
		finally:
			conn2.close()
		return render_template("admin_pages_form.html", mode="edit", page=pg, pages=fetch_pages(visible_only=True), admin_mode=True, max_position=max_pos)

	@app.route("/admin/pages/<int:page_id>/delete", methods=["POST"])
	def admin_pages_delete(page_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			conn.execute("DELETE FROM pages WHERE id = ?", (page_id,))
			conn.commit()
			flash("Page deleted.", "success")
		finally:
			conn.close()
		return redirect(url_for("admin_pages"))

	@app.route("/admin/streamers")
	def admin_streamers():
		streamers = fetch_all_streamers()
		return render_template("admin_streamers_list.html", streamers=streamers, pages=fetch_pages(visible_only=True), admin_mode=True)

	@app.route("/admin/tags")
	def admin_tags():
		tags = fetch_all_tags()
		return render_template("admin_tags_list.html", tags=tags, pages=fetch_pages(visible_only=True), admin_mode=True)

	@app.route("/admin/tags/new", methods=["GET", "POST"])
	def admin_tags_new():
		if request.method == "POST":
			name = request.form.get("name", "").strip()
			if not name:
				flash("Name is required.", "error")
			else:
				conn = get_db_connection(DATABASE_PATH)
				try:
					conn.execute("INSERT INTO tags (name) VALUES (?)", (name,))
					conn.commit()
					flash("Tag created.", "success")
					return redirect(url_for("admin_tags"))
				except Exception as e:
					flash(f"Tag creation failed: <code class='mono'>{e}</code>", "error")
				finally:
					conn.close()
		return render_template("admin_tags_form.html", mode="new", tag=None, pages=fetch_pages(visible_only=True), admin_mode=True)

	@app.route("/admin/tags/<int:tag_id>/edit", methods=["GET", "POST"])
	def admin_tags_edit(tag_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			tag = conn.execute("SELECT * FROM tags WHERE id = ?", (tag_id,)).fetchone()
		finally:
			conn.close()
		if not tag:
			abort(404)
		if request.method == "POST":
			name = request.form.get("name", "").strip()
			if not name:
				flash("Name is required.", "error")
			else:
				conn = get_db_connection(DATABASE_PATH)
				try:
					conn.execute("UPDATE tags SET name = ? WHERE id = ?", (name, tag_id))
					conn.commit()
					flash("Tag updated.", "success")
					return redirect(url_for("admin_tags"))
				finally:
					conn.close()
		return render_template("admin_tags_form.html", mode="edit", tag=tag, pages=fetch_pages(visible_only=True), admin_mode=True)

	@app.route("/admin/tags/<int:tag_id>/delete", methods=["POST"])
	def admin_tags_delete(tag_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
			conn.commit()
			flash("Tag deleted.", "success")
		finally:
			conn.close()
		return redirect(url_for("admin_tags"))

	@app.route("/admin/streamers/new", methods=["GET", "POST"])
	def admin_streamers_new():
		if request.method == "POST":
			name = request.form.get("name", "").strip()
			icon_file = request.files.get("icon_file")
			if not name:
				flash("Name is required.", "error")
			else:
				try:
					conn = get_db_connection(DATABASE_PATH)
					try:
						# Insert first to obtain streamer id
						cur = conn.execute("INSERT INTO streamers (name, icon_url) VALUES (?, ?)", (name, None))
						conn.commit()
						new_id = int(cur.lastrowid)
					finally:
						conn.close()
					icon_url = None
					if icon_file and icon_file.filename:
						icon_url = upload_streamer_icon(icon_file, new_id)
						conn2 = get_db_connection(DATABASE_PATH)
						try:
							conn2.execute("UPDATE streamers SET icon_url = ? WHERE id = ?", (icon_url, new_id))
							conn2.commit()
						finally:
							conn2.close()
					flash("Streamer created.", "success")
					return redirect(url_for("admin_streamers"))
				except Exception as e:
					logging.exception("Streamer create failed")
					flash(f"Streamer creation failed: <code class='mono'>{e}</code>", "error")
		return render_template("admin_streamers_form.html", mode="new", streamer=None, admin_mode=True)

	@app.route("/admin/streamers/<int:streamer_id>/edit", methods=["GET", "POST"])
	def admin_streamers_edit(streamer_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			row = conn.execute("SELECT * FROM streamers WHERE id = ?", (streamer_id,)).fetchone()
		finally:
			conn.close()
		if not row:
			abort(404)
		if request.method == "POST":
			name = request.form.get("name", "").strip()
			icon_file = request.files.get("icon_file")
			try:
				icon_url = row["icon_url"]
				if icon_file and icon_file.filename:
					icon_url = upload_streamer_icon(icon_file, streamer_id)
					# optional: we could delete old icon object, skipped
				conn = get_db_connection(DATABASE_PATH)
				try:
					conn.execute("UPDATE streamers SET name = ?, icon_url = ? WHERE id = ?", (name or row["name"], icon_url, streamer_id))
					conn.commit()
				finally:
					conn.close()
				flash("Streamer updated.", "success")
				return redirect(url_for("admin_streamers"))
			except Exception as e:
				logging.exception("Streamer update failed")
				flash(f"Streamer update failed: <code class='mono'>{e}</code>", "error")
		return render_template("admin_streamers_form.html", mode="edit", streamer=row, admin_mode=True)

	@app.route("/admin/streamers/<int:streamer_id>/delete", methods=["POST"])
	def admin_streamers_delete(streamer_id: int):
		conn = get_db_connection(DATABASE_PATH)
		try:
			conn.execute("DELETE FROM event_streamers WHERE streamer_id = ?", (streamer_id,))
			conn.execute("DELETE FROM streamers WHERE id = ?", (streamer_id,))
			conn.commit()
		finally:
			conn.close()
		flash("Streamer deleted.", "success")
		return redirect(url_for("admin_streamers"))

	@app.route("/admin/events/new", methods=["GET", "POST"])
	def admin_events_new():
		# Defensive defaults to avoid UnboundLocalError on GET before POST locals are set
		title = body = slug = clip_url = event_date = created_at = ""
		if request.method == "POST":
			app.logger.info("admin_events_new POST received")
			title = request.form.get("title", "").strip()
			body = request.form.get("body", "").strip()
			slug = request.form.get("slug", "").strip()
			clip_url = request.form.get("clip_url", "").strip()
			clip_file = request.files.get("clip_file")
			event_date = request.form.get("event_date", "").strip()
			streamer_id = request.form.get('streamer_id')
			app.logger.info("form: title=%s slug=%s event_date=%s has_file=%s has_url=%s streamer_id=%s", bool(title), slug, event_date, bool(clip_file and clip_file.filename), bool(clip_url), streamer_id)
			# Require either a URL or an uploaded file and all core fields
			if not title or not body or not event_date or not slug or (not clip_url and (not clip_file or clip_file.filename == "")):
				flash("Title, body, slug, date, and either a Twitch Clip URL or an uploaded file are required.", "error")
				app.logger.info("validation failed: missing required fields")
				return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
			if not streamer_id or not streamer_id.isdigit():
				flash("Please select a streamer.", "error")
				app.logger.info("validation failed: streamer missing or invalid")
				return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
			# Normalize to start-of-day string for consistent sorting
			created_at = normalize_date_to_created_at(event_date)
			if not created_at:
				flash("Invalid date format.", "error")
				app.logger.info("validation failed: bad date")
				return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
			if clip_file and clip_file.filename:
				app.logger.info("path: manual upload")
				public_base = (os.environ.get("B2_BASE_URL") or "").rstrip("/")
				s3 = get_s3_client()
				bucket = os.environ.get("B2_BUCKET")
				# Create event first to get id
				event_id = create_event(title=title, body=body, video_url="", created_at=created_at, slug=slug)
				# Save temp file
				tmpdir = tempfile.mkdtemp(prefix="upload_clip_")
				local_path = os.path.join(tmpdir, clip_file.filename)
				clip_file.save(local_path)
				# Probe height via ffprobe if available
				ffbin = (os.environ.get("FFMPEG_PATH") or "ffprobe")
				label = "source"
				try:
					res = subprocess.run([ffbin.replace("ffmpeg", "ffprobe"), "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height", "-of", "csv=p=0", local_path], capture_output=True, text=True, check=True)
					h = int(res.stdout.strip()) if res.stdout.strip() else 0
					if h:
						label = f"{h}p"
				except Exception:
					pass
				# Upload file under clips/<event_id>/ with deterministic name
				key = f"clips/{event_id}/{event_id}.mp4"
				s3.upload_file(local_path, bucket, key, ExtraArgs={"ContentType": "video/mp4", "CacheControl": "public, max-age=31536000, immutable"})
				public_url = f"{public_base}/{key}"
				# Generate a thumbnail for this upload (used as poster)
				thumb_local = os.path.join(tmpdir, "thumb.jpg")
				ffmpeg_bin = (os.environ.get("FFMPEG_PATH") or "ffmpeg")
				try:
					subprocess.run([ffmpeg_bin, "-y", "-ss", "1", "-i", local_path, "-frames:v", "1", "-q:v", "2", thumb_local], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
					thumb_key = f"clips/{event_id}/thumb.jpg"
					s3.upload_file(thumb_local, bucket, thumb_key, ExtraArgs={"ContentType": "image/jpeg", "CacheControl": "public, max-age=31536000, immutable"})
					thumb_public = f"{public_base}/{thumb_key}"
				except Exception:
					thumb_public = None
				# Update event
				conn = get_db_connection(DATABASE_PATH)
				try:
					if thumb_public:
						conn.execute("UPDATE events SET video_url = ?, thumbnail_url = ? WHERE id = ?", (public_url, thumb_public, event_id))
					else:
						conn.execute("UPDATE events SET video_url = ? WHERE id = ?", (public_url, event_id))
					conn.commit()
				finally:
					conn.close()
				add_event_video(event_id=event_id, quality_label=label, mime="video/mp4", filesize=os.path.getsize(local_path), duration_s=0.0, b2_key=key, public_url=public_url)
				# Associations (single streamer)
				set_event_streamers(event_id, [int(streamer_id)])
				tag_ids = [int(x) for x in request.form.getlist('tag_ids') if x.isdigit()]
				if tag_ids:
					set_event_tags(event_id, tag_ids)
				shutil.rmtree(tmpdir, ignore_errors=True)
				flash("Event created and video uploaded.", "success")
				return redirect(url_for("admin_events_list"))
			elif clip_url:
				app.logger.info("path: clip url ingest")
				# Ingest clip URL: upload directly to clips/<event_id>/
				try:
					event_id = create_event(title=title, body=body, video_url="", created_at=created_at, slug=slug)
					clip_id, variants = ingest_twitch_clip_to_b2(clip_url, dest_event_id=event_id)
				except Exception as e:
					app.logger.exception("ingest exception")
					flash(f"Clip ingestion failed: <code class='mono'>{e}</code>", "error")
					# Clean up any placeholder event and uploaded objects
					try:
						delete_event(event_id)
						s3 = get_s3_client()
						bucket = os.environ.get("B2_BUCKET")
						for key in s3_list_keys_with_prefix(s3, bucket, f"clips/{event_id}/"):
							s3.delete_object(Bucket=bucket, Key=key)
					except Exception:
						pass
					return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
				if not variants:
					flash("No video variants uploaded; event not created.", "error")
					app.logger.info("ingest produced zero variants")
					try:
						delete_event(event_id)
						s3 = get_s3_client()
						bucket = os.environ.get("B2_BUCKET")
						for key in s3_list_keys_with_prefix(s3, bucket, f"clips/{event_id}/"):
							s3.delete_object(Bucket=bucket, Key=key)
					except Exception:
						pass
					return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
				# Update event with original URL, thumb and primary video URL
				public_base = (os.environ.get("B2_BASE_URL") or "").rstrip("/")
				conn = get_db_connection(DATABASE_PATH)
				try:
					first_url = variants[0]["public_url"]
					thumb_public = variants[0].get("__thumbnail_url__")
					if thumb_public:
						conn.execute("UPDATE events SET original_clip_url = ?, thumbnail_url = ?, video_url = ? WHERE id = ?", (clip_url, thumb_public, first_url, event_id))
					else:
						conn.execute("UPDATE events SET original_clip_url = ?, video_url = ? WHERE id = ?", (clip_url, first_url, event_id))
					conn.commit()
				finally:
					conn.close()
				# Save event associations and variant rows
				streamer_id = request.form.get('streamer_id')
				if streamer_id and streamer_id.isdigit():
					set_event_streamers(event_id, [int(streamer_id)])
				tag_ids = [int(x) for x in request.form.getlist('tag_ids') if x.isdigit()]
				if tag_ids:
					set_event_tags(event_id, tag_ids)
				for v in variants:
					add_event_video(
						event_id=event_id,
						quality_label=v["quality_label"],
						mime=v["mime"],
						filesize=int(v["filesize"]),
						duration_s=float(v["duration_s"]),
						b2_key=v["b2_key"],
						public_url=v["public_url"],
					)
				flash("Event created and video uploaded.", "success")
				return redirect(url_for("admin_events_list"))
		else:
			# GET: render clean form (no validation flash)
			try:
				get_flashed_messages()
				session.pop('_flashes', None)
			except Exception:
				pass
			return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)
		# Safety net: if no branch above returned, render the form
		return render_template("admin_events_form.html", mode="new", event=None, streamers=fetch_all_streamers(), tags=fetch_all_tags(), selected_streamer_ids=[], selected_tag_ids=[], pages=fetch_pages(visible_only=True), admin_mode=True)

	@app.route("/admin/events/<int:event_id>/edit", methods=["GET", "POST"])
	def admin_events_edit(event_id: int):
		event = fetch_event_by_id(event_id)
		if not event:
			abort(404)
		if request.method == "POST":
			title = request.form.get("title", "").strip()
			body = request.form.get("body", "").strip()
			slug = request.form.get("slug", "").strip()
			clip_url = request.form.get("clip_url", "").strip()
			event_date = request.form.get("event_date", "").strip()
			if not title or not body or not event_date or not slug:
				flash("Title, body, slug, date, and either a Twitch Clip URL or an uploaded file are required.", "error")
			else:
				created_at = normalize_date_to_created_at(event_date)
				if not created_at:
					flash("Invalid date format.", "error")
					return render_template(
						"admin_events_form.html",
						mode="edit",
						event=event,
						streamers=fetch_all_streamers(),
						selected_streamer_ids=fetch_event_streamer_ids(event_id),
						pages=fetch_pages(visible_only=True),
						vids=fetch_event_videos_map([event_id]).get(event_id),
						admin_mode=True,
					)
				# Keep existing primary video_url for compatibility
				update_event(event_id=event_id, title=title, body=body, video_url=event["video_url"], created_at=created_at, slug=slug)
				# Update original clip URL if provided (does not re-ingest)
				if clip_url and clip_url != (event["original_clip_url"] or ""):
					conn = get_db_connection(DATABASE_PATH)
					try:
						conn.execute("UPDATE events SET original_clip_url = ? WHERE id = ?", (clip_url, event_id))
						conn.commit()
					finally:
						conn.close()
				# Update event associations: single streamer
				streamer_id = request.form.get('streamer_id')
				if streamer_id and streamer_id.isdigit():
					set_event_streamers(event_id, [int(streamer_id)])
				else:
					set_event_streamers(event_id, [])
				tag_ids = [int(x) for x in request.form.getlist('tag_ids') if x.isdigit()]
				set_event_tags(event_id, tag_ids)
				flash("Event updated.", "success")
				return redirect(url_for("admin_events_list"))
		return render_template(
			"admin_events_form.html",
			mode="edit",
			event=event,
			streamers=fetch_all_streamers(),
			selected_streamer_ids=fetch_event_streamer_ids(event_id),
			tags=fetch_all_tags(),
			selected_tag_ids=fetch_event_tag_ids(event_id),
			pages=fetch_pages(visible_only=True),
			vids=fetch_event_videos_map([event_id]).get(event_id),
			admin_mode=True,
		)

	@app.route("/admin/events/<int:event_id>/delete", methods=["POST"])
	def admin_events_delete(event_id: int):
		# Attempt to remove media from B2 first, while DB rows still exist (for explicit keys)
		bucket = os.environ.get("B2_BUCKET")
		prefix = f"clips/{event_id}/"
		deleted_count = 0
		error_count = 0
		try:
			s3 = get_s3_client()
			# 1) Delete known keys from DB (event_videos + generic thumb)
			try:
				conn = get_db_connection(DATABASE_PATH)
				cur = conn.execute("SELECT b2_key FROM event_videos WHERE event_id = ?", (event_id,))
				rows = cur.fetchall()
				explicit_keys = [row["b2_key"] for row in rows if row["b2_key"]]
			finally:
				conn.close()
			# Ensure we also try the generic thumb
			explicit_keys.append(f"{prefix}thumb.jpg")
			# Also attempt to remove possible directory placeholder objects some UIs create
			explicit_keys.append(f"clips/{event_id}")
			explicit_keys.append(f"clips/{event_id}/")
			for k in explicit_keys:
				try:
					s3.delete_object(Bucket=bucket, Key=k)
					deleted_count += 1
				except Exception:
					error_count += 1
			# 2) Sweep remaining objects by prefix (catches any stragglers)
			keys = s3_list_keys_with_prefix(s3, bucket, prefix)
			for key in keys:
				try:
					s3.delete_object(Bucket=bucket, Key=key)
					deleted_count += 1
				except Exception:
					error_count += 1
			# 3) Purge versioned objects and delete markers so folder doesn't linger with '*'
			dv, ev = s3_delete_all_versions_with_prefix(s3, bucket, prefix)
			deleted_count += dv
			error_count += ev
			# 4) Also purge versions of possible folder marker keys ('clips/<id>' and 'clips/<id>/')
			for marker in (f"clips/{event_id}", f"clips/{event_id}/"):
				dv2, ev2 = s3_hard_delete_key_all_versions(s3, bucket, marker)
				deleted_count += dv2
				error_count += ev2
			# 5) Final attempt to remove folder marker after version purge
			for marker in (f"clips/{event_id}", f"clips/{event_id}/"):
				try:
					s3.delete_object(Bucket=bucket, Key=marker)
				except Exception:
					pass
		except Exception as e:
			logging.exception("Failed to delete some or all B2 objects for event %s: %s", event_id, e)
		# Now remove from DB
		deleted = delete_event(event_id)
		if deleted:
			if error_count > 0:
				flash("Event deleted. Some media could not be removed from storage. Check B2 key permissions.", "warning")
			else:
				flash("Event deleted and folder removed.", "success")
		else:
			flash("Event not found.", "error")
		return redirect(url_for("admin_events_list"))

	# -------------
	# Share/OG page for an event (slug or id). Bots get static OG tags; users are redirected to timeline hash.
	# -------------
	@app.route("/e/<key>")
	def share_event_og(key: str):
		event = _find_event_by_slug_or_id(str(key))
		if not event:
			abort(404)
		# Prefer slug for the share key
		share_key = str(event["slug"] or event["id"])
		# Build canonical URLs
		canonical_main = url_for("index", _external=True) + f"#{share_key}"
		share_url = url_for("share_event_og", key=share_key, _external=True)
		# Choose OG image and optional video
		og_image = event.get("thumbnail_url") if isinstance(event, dict) else event["thumbnail_url"]
		vids_map = fetch_event_videos_map([int(event["id"])])
		vids = vids_map.get(int(event["id"])) if vids_map else None
		og_video = None
		if vids:
			# best-first sorted already by fetch_event_videos_map()
			best = vids[0]
			og_video = best.get("public_url") or ""
		title = event["title"]
		desc = excerpt(event["body"], 200)
		# Render minimal OG page with meta tags and a quick redirect to timeline hash
		return render_template(
			"share_event.html",
			title=title,
			desc=desc,
			og_image=og_image,
			og_video=og_video,
			canonical_main=canonical_main,
			share_url=share_url,
		)

	return app


# ------------------------------
# Database helpers
# ------------------------------
def get_db_connection(db_path: str) -> sqlite3.Connection:
	conn = sqlite3.connect(db_path)
	conn.row_factory = sqlite3.Row
	return conn


def ensure_database_initialized(db_path: str) -> None:
	first_time = not os.path.exists(db_path)
	os.makedirs(os.path.dirname(db_path), exist_ok=True)
	conn = get_db_connection(db_path)
	try:
		# Enable WAL and foreign keys for robustness on single-node deployments
		try:
			conn.execute("PRAGMA journal_mode=WAL")
			conn.execute("PRAGMA foreign_keys=ON")
		except Exception:
			pass
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				slug TEXT,
				title TEXT NOT NULL,
				body TEXT NOT NULL,
				video_url TEXT NOT NULL,
				created_at TEXT NOT NULL DEFAULT (datetime('now'))
			);
			"""
		)
		# Migration: add slug if missing and add unique index
		try:
			conn.execute("ALTER TABLE events ADD COLUMN slug TEXT")
		except Exception:
			pass
		try:
			conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_events_slug ON events(slug)")
		except Exception:
			pass
		# Migration: add original_clip_url if missing
		try:
			conn.execute("ALTER TABLE events ADD COLUMN original_clip_url TEXT")
		except Exception:
			pass
		# New table for uploaded video variants
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS event_videos (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
				quality_label TEXT NOT NULL,
				mime TEXT NOT NULL,
				filesize INTEGER,
				duration_s REAL,
				b2_key TEXT NOT NULL,
				public_url TEXT NOT NULL,
				created_at TEXT NOT NULL DEFAULT (datetime('now'))
			);
			"""
		)
		conn.execute(
			"CREATE INDEX IF NOT EXISTS idx_event_videos_event_id ON event_videos(event_id)"
		)
		# Migration: add thumbnail_url if missing
		try:
			conn.execute("ALTER TABLE events ADD COLUMN thumbnail_url TEXT")
		except Exception:
			pass

		# Streamers table
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS streamers (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL UNIQUE,
				icon_url TEXT,
				created_at TEXT NOT NULL DEFAULT (datetime('now'))
			);
			"""
		)
		# Tags table
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS tags (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL UNIQUE,
				created_at TEXT NOT NULL DEFAULT (datetime('now'))
			);
			"""
		)
		# Join table events<->streamers
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS event_streamers (
				event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
				streamer_id INTEGER NOT NULL REFERENCES streamers(id) ON DELETE CASCADE,
				PRIMARY KEY(event_id, streamer_id)
			);
			"""
		)
		# Join table events<->tags (many-to-many)
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS event_tags (
				event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
				tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
				PRIMARY KEY(event_id, tag_id)
			);
			"""
		)
		# Pages table for footer and modal content
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS pages (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				title TEXT NOT NULL,
				content TEXT NOT NULL,
				position INTEGER NOT NULL DEFAULT 0,
				visible INTEGER NOT NULL DEFAULT 1
			);
			"""
		)
		# Seed About (1) and Disclaimer (2) if missing
		try:
			conn.execute("INSERT OR IGNORE INTO pages (id, title, content, position, visible) VALUES (1, 'About', 'This site documents harmful rhetoric observed on live streams, organized as a timeline for research and accountability purposes.', 1, 1)")
			conn.execute("INSERT OR IGNORE INTO pages (id, title, content, position, visible) VALUES (2, 'Disclaimer', 'Clips are hosted by the site owner for commentary and documentation. All trademarks and content belong to their respective owners.', 2, 1)")
		except Exception:
			pass
		conn.commit()
	finally:
		conn.close()
	# Do not seed sample events; production app ingests real clips or manual uploads only.





def fetch_all_events(order_by: str = "-created_at", limit: Optional[int] = None, offset: int = 0) -> list[sqlite3.Row]:
	# Add stable tiebreaker by id when dates are equal
	if order_by.startswith("-"):
		order_clause = "created_at DESC, id DESC"
	else:
		order_clause = "created_at ASC, id ASC"
	conn = get_db_connection(DATABASE_PATH)
	try:
		sql = f"SELECT * FROM events ORDER BY {order_clause}"
		params: tuple = ()
		if limit is not None:
			sql += " LIMIT ? OFFSET ?"
			params = (int(limit), int(offset))
		cur = conn.execute(sql, params)
		return cur.fetchall()
	finally:
		conn.close()


def count_events() -> int:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT COUNT(*) FROM events")
		return int(cur.fetchone()[0])
	finally:
		conn.close()


def fetch_pages(visible_only: bool = True) -> list[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		if visible_only:
			cur = conn.execute("SELECT * FROM pages WHERE visible = 1 ORDER BY position ASC, id ASC")
		else:
			cur = conn.execute("SELECT * FROM pages ORDER BY position ASC, id ASC")
		return cur.fetchall()
	finally:
		conn.close()
def fetch_all_streamers() -> list[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT * FROM streamers ORDER BY name ASC")
		return cur.fetchall()
	finally:
		conn.close()

def _build_streamer_icon_url(streamer_id: int) -> str:
	base = (os.environ.get("B2_BASE_URL") or "").rstrip("/")
	return f"{base}/assets/icons/streamer_{int(streamer_id)}.png"

def fetch_streamers_with_events() -> list[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute(
			"""
			SELECT
				s.*,
				(
					SELECT COUNT(*)
					FROM event_streamers es
					JOIN events e ON e.id = es.event_id
					WHERE es.streamer_id = s.id
				) AS event_count
			FROM streamers s
			WHERE (
				SELECT COUNT(*)
				FROM event_streamers es
				JOIN events e ON e.id = es.event_id
				WHERE es.streamer_id = s.id
			) > 0
			ORDER BY s.name ASC
			"""
		)
		rows = cur.fetchall()
	finally:
		conn.close()
	# Normalize icon URLs: if a streamer has an icon (icon_url not null), point to the canonical assets path
	out: list[sqlite3.Row] = []
	for r in rows:
		d = dict(r)
		if d.get("icon_url"):
			d["icon_url"] = _build_streamer_icon_url(int(d["id"]))
		out.append(d)
	# Return list of plain dicts; callers/templates access by key
	return out

def fetch_all_tags() -> list[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT * FROM tags ORDER BY name ASC")
		return cur.fetchall()
	finally:
		conn.close()

def fetch_tags_with_events() -> list[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute(
			"""
			SELECT
				t.*,
				(
					SELECT COUNT(*)
					FROM event_tags et
					JOIN events e ON e.id = et.event_id
					WHERE et.tag_id = t.id
				) AS event_count
			FROM tags t
			WHERE (
				SELECT COUNT(*)
				FROM event_tags et
				JOIN events e ON e.id = et.event_id
				WHERE et.tag_id = t.id
			) > 0
			ORDER BY t.name ASC
			"""
		)
		return cur.fetchall()
	finally:
		conn.close()
def fetch_event_tags_map(event_ids: list[int]) -> dict[int, list[int]]:
	if not event_ids:
		return {}
	qmarks = ",".join(["?"] * len(event_ids))
	conn = get_db_connection(DATABASE_PATH)
	try:
		rows = conn.execute(
			f"SELECT event_id, tag_id FROM event_tags WHERE event_id IN ({qmarks})",
			tuple(event_ids),
		).fetchall()
	finally:
		conn.close()
	result: dict[int, list[int]] = {}
	for r in rows:
		result.setdefault(r["event_id"], []).append(r["tag_id"])
	return result

def set_event_streamers(event_id: int, streamer_ids: list[int]) -> None:
	conn = get_db_connection(DATABASE_PATH)
	try:
		conn.execute("DELETE FROM event_streamers WHERE event_id = ?", (event_id,))
		for sid in streamer_ids:
			conn.execute("INSERT OR IGNORE INTO event_streamers (event_id, streamer_id) VALUES (?, ?)", (event_id, int(sid)))
		conn.commit()
	finally:
		conn.close()

def set_event_tags(event_id: int, tag_ids: list[int]) -> None:
	conn = get_db_connection(DATABASE_PATH)
	try:
		conn.execute("DELETE FROM event_tags WHERE event_id = ?", (event_id,))
		for tid in tag_ids:
			conn.execute("INSERT OR IGNORE INTO event_tags (event_id, tag_id) VALUES (?, ?)", (event_id, int(tid)))
		conn.commit()
	finally:
		conn.close()


def fetch_event_streamer_ids(event_id: int) -> list[int]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT streamer_id FROM event_streamers WHERE event_id = ?", (event_id,))
		return [int(r[0]) for r in cur.fetchall()]
	finally:
		conn.close()

def fetch_event_tag_ids(event_id: int) -> list[int]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT tag_id FROM event_tags WHERE event_id = ?", (event_id,))
		return [int(r[0]) for r in cur.fetchall()]
	finally:
		conn.close()


def fetch_event_primary_streamers_map(event_ids: list[int]) -> dict[int, dict]:
	if not event_ids:
		return {}
	conn = get_db_connection(DATABASE_PATH)
	try:
		qmarks = ",".join(["?"] * len(event_ids))
		cur = conn.execute(
			f"""
			SELECT es.event_id, s.id AS streamer_id, s.name, s.icon_url
			FROM event_streamers es
			JOIN streamers s ON s.id = es.streamer_id
			WHERE es.event_id IN ({qmarks})
			GROUP BY es.event_id
			""",
			tuple(event_ids),
		)
		rows = cur.fetchall()
	finally:
		conn.close()
	result: dict[int, dict] = {}
	for r in rows:
		icon_url = None
		if r["icon_url"]:
			icon_url = _build_streamer_icon_url(int(r["streamer_id"]))
		result[r["event_id"]] = {"id": r["streamer_id"], "name": r["name"], "icon_url": icon_url}
	return result

def fetch_event_by_id(event_id: int) -> Optional[sqlite3.Row]:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,))
		row = cur.fetchone()
		return row
	finally:
		conn.close()


def create_event(title: str, body: str, video_url: str, created_at: Optional[str] = None, slug: Optional[str] = None) -> int:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute(
			"INSERT INTO events (slug, title, body, video_url, created_at) VALUES (?, ?, ?, ?, COALESCE(?, datetime('now')))",
			(slug, title, body, video_url, created_at),
		)
		conn.commit()
		return int(cur.lastrowid)
	finally:
		conn.close()

def add_event_video(event_id: int, quality_label: str, mime: str, filesize: int, duration_s: float, b2_key: str, public_url: str) -> int:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute(
			"""
			INSERT INTO event_videos (event_id, quality_label, mime, filesize, duration_s, b2_key, public_url)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			""",
			(event_id, quality_label, mime, filesize, duration_s, b2_key, public_url),
		)
		conn.commit()
		return int(cur.lastrowid)
	finally:
		conn.close()

def fetch_event_videos_map(event_ids: list[int]) -> dict[int, list[sqlite3.Row]]:
	if not event_ids:
		return {}
	conn = get_db_connection(DATABASE_PATH)
	try:
		qmarks = ",".join("?" for _ in event_ids)
		cur = conn.execute(
			f"SELECT * FROM event_videos WHERE event_id IN ({qmarks})",
			tuple(event_ids),
		)
		rows = cur.fetchall()
	finally:
		conn.close()
	# group and sort by quality (descending by numeric resolution if present)
	out: dict[int, list[sqlite3.Row]] = {}
	def quality_key(label: str) -> int:
		m = re.search(r"(\d{3,4})p", label or "")
		return int(m.group(1)) if m else 0
	for r in rows:
		out.setdefault(r["event_id"], []).append(r)
	for k, lst in out.items():
		lst.sort(key=lambda r: quality_key(r["quality_label"]), reverse=True)
	return out

# ------------------------------
# Ingestion: Twitch Clip -> Backblaze B2
# ------------------------------
def get_s3_client():
	endpoint = os.environ.get("B2_ENDPOINT_URL") or os.environ.get("B2_ENDPOINT") or "https://s3.us-west-004.backblazeb2.com"
	region = os.environ.get("B2_REGION") or "us-west-004"
	access_key = os.environ.get("B2_ACCESS_KEY_ID")
	secret_key = os.environ.get("B2_SECRET_ACCESS_KEY")
	session = boto3.session.Session()
	return session.client(
		"s3",
		region_name=region,
		endpoint_url=endpoint,
		aws_access_key_id=access_key,
		aws_secret_access_key=secret_key,
		config=BotoConfig(s3={"addressing_style": "virtual"}),
	)

def s3_list_keys_with_prefix(s3, bucket: str, prefix: str) -> list[str]:
	keys: list[str] = []
	token = None
	while True:
		params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
		if token:
			params["ContinuationToken"] = token
		resp = s3.list_objects_v2(**params)
		for obj in resp.get("Contents", []) or []:
			keys.append(obj["Key"])
		if resp.get("IsTruncated"):
			token = resp.get("NextContinuationToken")
		else:
			break
	return keys



def s3_delete_all_versions_with_prefix(s3, bucket: str, prefix: str) -> tuple[int, int]:
	"""
	Hard-delete all object versions and delete markers under a prefix.
	Returns (deleted_count, error_count).
	"""
	deleted = 0
	errors = 0
	key_marker = None
	version_id_marker = None
	while True:
		params = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
		if key_marker:
			params["KeyMarker"] = key_marker
		if version_id_marker:
			params["VersionIdMarker"] = version_id_marker
		resp = s3.list_object_versions(**params)
		for v in (resp.get("Versions") or []):
			try:
				s3.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
				deleted += 1
			except Exception:
				errors += 1
		for m in (resp.get("DeleteMarkers") or []):
			try:
				s3.delete_object(Bucket=bucket, Key=m["Key"], VersionId=m["VersionId"])
				deleted += 1
			except Exception:
				errors += 1
		if resp.get("IsTruncated"):
			key_marker = resp.get("NextKeyMarker")
			version_id_marker = resp.get("NextVersionIdMarker")
		else:
			break
	return deleted, errors

def s3_hard_delete_key_all_versions(s3, bucket: str, key: str) -> tuple[int, int]:
	"""
	Permanently remove all versions and delete markers for a single key.
	Useful for cleaning up zero-byte 'folder marker' objects like 'clips/<id>' or 'clips/<id>/'.
	"""
	deleted = 0
	errors = 0
	key_marker = None
	version_id_marker = None
	while True:
		params = {"Bucket": bucket, "Prefix": key, "MaxKeys": 1000}
		if key_marker:
			params["KeyMarker"] = key_marker
		if version_id_marker:
			params["VersionIdMarker"] = version_id_marker
		resp = s3.list_object_versions(**params)
		for v in (resp.get("Versions") or []):
			if v.get("Key") != key:
				continue
			try:
				s3.delete_object(Bucket=bucket, Key=key, VersionId=v["VersionId"])
				deleted += 1
			except Exception:
				errors += 1
		for m in (resp.get("DeleteMarkers") or []):
			if m.get("Key") != key:
				continue
			try:
				s3.delete_object(Bucket=bucket, Key=key, VersionId=m["VersionId"])
				deleted += 1
			except Exception:
				errors += 1
		if resp.get("IsTruncated"):
			key_marker = resp.get("NextKeyMarker")
			version_id_marker = resp.get("NextVersionIdMarker")
		else:
			break
	return deleted, errors

def upload_streamer_icon(file_storage, streamer_id: int) -> str:
    """Validate size (32–128px), convert to PNG, upload to B2 at /assets/icons/streamer_<id>.png and return public URL."""
    public_base = (os.environ.get("B2_BASE_URL") or "").rstrip("/")
    bucket = os.environ.get("B2_BUCKET")
    s3 = get_s3_client()
    tmpdir = tempfile.mkdtemp(prefix="icon_")
    try:
        src = os.path.join(tmpdir, file_storage.filename)
        file_storage.save(src)
        im = Image.open(src)
        w, h = im.size
        # Must be perfectly square
        if w != h:
            raise RuntimeError("Icon must be square (width must equal height)")
        if w < 32 or h < 32:
            raise RuntimeError("Icon too small (minimum 32×32)")
        # Downscale if larger than 128 preserving aspect
        max_dim = 128
        if w > max_dim or h > max_dim:
            im.thumbnail((max_dim, max_dim))
        # Ensure PNG
        out = os.path.join(tmpdir, "icon.png")
        im.save(out, format="PNG")
        key = f"assets/icons/streamer_{int(streamer_id)}.png"
        s3.upload_file(out, bucket, key, ExtraArgs={"ContentType": "image/png", "CacheControl": "public, max-age=31536000, immutable"})
        return f"{public_base}/{key}"
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def ingest_twitch_clip_to_b2(clip_url: str, dest_event_id: Optional[int] = None) -> tuple[str, list[dict] | list[dict]]:
	"""
	Download multiple qualities of a Twitch clip and upload to B2.
	If dest_event_id is provided, uploads go directly to clips/<dest_event_id>/.
	Otherwise, uploads go to clips/<clip_id>/.
	Returns (clip_id, variants). Each variant has:
	{ quality_label, mime, filesize, duration_s, b2_key, public_url }
	Raises Exception if no variant can be uploaded.
	"""
	bucket = os.environ.get("B2_BUCKET")
	public_base = os.environ.get("B2_BASE_URL")  # e.g. https://f000.backblazeb2.com/file/your-bucket or CDN
	if not bucket or not public_base:
		raise RuntimeError("B2_BUCKET and B2_BASE_URL are required")

	tmpdir = tempfile.mkdtemp(prefix="ingest_clip_")
	variants: list[dict] = []
	s3 = get_s3_client()
	try:
		# Discover clip metadata first (id/duration), then download all video formats with height>0
		probe_opts = {
			"quiet": True,
			"skip_download": True,
			"http_headers": {
				"Referer": "https://www.twitch.tv/",
				"User-Agent": "Mozilla/5.0",
			},
			"extractor_args": {
				"twitch": {"client_id": ["kimne78kx3ncx6brgo4mv6wki5h1ko"]},
			},
		}
		with YoutubeDL(probe_opts) as ydl:
			info = ydl.extract_info(clip_url, download=False)
		clip_id = str(info.get("id") or "clip")
		duration = float(info.get("duration") or 0.0)
		# Try to capture a thumbnail
		thumb_url = info.get("thumbnail")
		if not thumb_url:
			th_list = info.get("thumbnails") or []
			if th_list:
				# pick the largest by height
				thumb_url = sorted(th_list, key=lambda t: t.get("height") or 0, reverse=True)[0].get("url")

		# Download all combined formats with video (height>0). Let yt-dlp produce one file per height.
		ffmpeg_location = os.environ.get("FFMPEG_PATH")
		ytdlp_path = os.environ.get("YTDLP_PATH")
		outtmpl = os.path.join(tmpdir, f"{clip_id}_%(height)sp.%(ext)s")
		dl_opts = {
			"quiet": True,
			"outtmpl": outtmpl,
			# select all formats with video (height>0); if extractor doesn't expose height in one pass, fallback to best
			"format": "all[height>0]/b",
			"merge_output_format": "mp4",
			"postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}],
			"http_headers": {
				"Referer": "https://www.twitch.tv/",
				"User-Agent": "Mozilla/5.0",
			},
			"extractor_args": {
				"twitch": {"client_id": ["kimne78kx3ncx6brgo4mv6wki5h1ko"]},
			},
			"nooverwrites": True,
		}
		if ffmpeg_location:
			dl_opts["ffmpeg_location"] = ffmpeg_location
		if ytdlp_path:
			dl_opts["ydl_exe"] = ytdlp_path
		with YoutubeDL(dl_opts) as ydl:
			ydl.download([clip_url])

		# Collect downloaded files
		candidates = [p for p in os.listdir(tmpdir) if p.startswith(f"{clip_id}_") and not p.endswith(".download")]
		thumbs_info: list[tuple[int, str, str]] = []  # (height, b2_key, local_thumb_path)
		for name in candidates:
			local_path = os.path.join(tmpdir, name)
			if not os.path.isfile(local_path):
				continue
			filesize = os.path.getsize(local_path)
			if filesize <= 0:
				continue
			# Derive height label
			mh = re.search(r"_(\d{3,4})p\.", name)
			hlabel = f"{mh.group(1)}p" if mh else "best"
			hnum = int(mh.group(1)) if mh else 0
			# Ensure .mp4 extension
			if not name.lower().endswith(".mp4"):
				# rename to .mp4 for consistency
				base = os.path.splitext(name)[0] + ".mp4"
				new_path = os.path.join(tmpdir, base)
				os.rename(local_path, new_path)
				local_path = new_path
				name = base
			base_prefix = f"clips/{int(dest_event_id)}/" if dest_event_id is not None else f"clips/{clip_id}/"
			key = f"{base_prefix}{name}"
			extra = {
				"ContentType": "video/mp4",
				"CacheControl": "public, max-age=31536000, immutable",
			}
			s3.upload_file(local_path, bucket, key, ExtraArgs=extra)
			public_url = f"{public_base.rstrip('/')}/{key}"

			# Generate a thumbnail at 1s for this quality
			try:
				thumb_local = os.path.join(tmpdir, f"{clip_id}_thumb_{hlabel}.jpg")
				ffbin = ffmpeg_location or "ffmpeg"
				subprocess.run([ffbin, "-y", "-ss", "1", "-i", local_path, "-frames:v", "1", "-q:v", "2", thumb_local], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
				thumb_key_q = f"{base_prefix}{clip_id}_thumb_{hlabel}.jpg"
				extra_img = {"ContentType": "image/jpeg", "CacheControl": "public, max-age=31536000, immutable"}
				s3.upload_file(thumb_local, bucket, thumb_key_q, ExtraArgs=extra_img)
				thumbs_info.append((hnum, thumb_key_q, thumb_local))
			except Exception:
				pass

			variants.append({
				"quality_label": hlabel,
				"mime": "video/mp4",
				"filesize": filesize,
				"duration_s": duration,
				"b2_key": key,
				"public_url": public_url,
			})

		# Pick highest-quality generated thumbnail (no generic thumb.jpg)
		thumb_public = None
		if thumbs_info:
			try:
				hmax, thumb_key_q, _local = sorted(thumbs_info, key=lambda t: t[0], reverse=True)[0]
				thumb_public = f"{public_base.rstrip('/')}/{thumb_key_q}"
			except Exception:
				thumb_public = None

		if not variants:
			raise RuntimeError("No variants could be uploaded")
		# sort best-first (handle labels without numeric height safely)
		def _qk(v: dict) -> int:
			m = re.search(r"(\\d{3,4})p", v.get("quality_label", "") or "")
			return int(m.group(1)) if m else 0
		variants.sort(key=_qk, reverse=True)
		# Attach best-thumbnail URL to the first variant dict for convenience
		if variants and thumb_public:
			variants[0]["__thumbnail_url__"] = thumb_public
		return clip_id, variants
	finally:
		shutil.rmtree(tmpdir, ignore_errors=True)


def update_event(event_id: int, title: str, body: str, video_url: str, created_at: Optional[str] = None, slug: Optional[str] = None) -> None:
	conn = get_db_connection(DATABASE_PATH)
	try:
		if created_at:
			if slug is not None:
				conn.execute(
					"UPDATE events SET slug = ?, title = ?, body = ?, video_url = ?, created_at = ? WHERE id = ?",
					(slug, title, body, video_url, created_at, event_id),
				)
			else:
				conn.execute(
					"UPDATE events SET title = ?, body = ?, video_url = ?, created_at = ? WHERE id = ?",
					(title, body, video_url, created_at, event_id),
				)
		else:
			if slug is not None:
				conn.execute(
					"UPDATE events SET slug = ?, title = ?, body = ?, video_url = ? WHERE id = ?",
					(slug, title, body, video_url, event_id),
				)
			else:
				conn.execute(
					"UPDATE events SET title = ?, body = ?, video_url = ? WHERE id = ?",
					(title, body, video_url, event_id),
				)
		conn.commit()
	finally:
		conn.close()


def delete_event(event_id: int) -> bool:
	conn = get_db_connection(DATABASE_PATH)
	try:
		cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
		conn.commit()
		return cur.rowcount > 0
	finally:
		conn.close()


# ------------------------------
# Template helpers
# ------------------------------
def format_datetime(dt_str: str) -> str:
	"""
	Format ISO or SQLite datetime string for display.
	"""
	dt = parse_datetime(dt_str)
	if not dt:
		return dt_str
	return dt.strftime("%B %d, %Y")


def excerpt(text: str, max_len: int = 280) -> str:
	if len(text) <= max_len:
		return text
	return text[: max_len - 1].rstrip() + "…"

def _find_event_by_slug_or_id(key: str) -> Optional[sqlite3.Row]:
	"""Find event by slug (preferred) or numeric id string."""
	conn = get_db_connection(DATABASE_PATH)
	try:
		# Try slug first
		cur = conn.execute("SELECT * FROM events WHERE slug = ? LIMIT 1", (key,))
		row = cur.fetchone()
		if row:
			return row
		# Fallback: numeric id
		if key.isdigit():
			cur = conn.execute("SELECT * FROM events WHERE id = ? LIMIT 1", (int(key),))
			row = cur.fetchone()
			return row
		return None
	finally:
		conn.close()


def render_video_player(sources: list[sqlite3.Row], poster: Optional[str] = None) -> Markup:
	"""
	Render an HTML5 video element with multiple <source> variants.
	Sources should be dict-like rows with fields: public_url, mime, quality_label.
	Highest quality should come first.
	"""
	if not sources:
		return Markup("<p>No video available.</p>")
	# Build <source> tags and a current-base poster
	parts = []
	base = (os.environ.get("B2_BASE_URL") or "").rstrip("/")
	first_b2_key = None
	for s in sources:
		# Prefer deriving from b2_key + current base so host changes are reflected without DB rewrites
		if base and ("b2_key" in s.keys()) and s["b2_key"]:
			url = f'{base}/{s["b2_key"]}'
			if not first_b2_key:
				first_b2_key = s["b2_key"]
		else:
			url = s.get("public_url") or ""
		parts.append(f'<source src="{url}" type="{s["mime"]}" data-quality="{s["quality_label"]}">')

	# Derive poster from first source key to ensure it matches current base
	derived_poster = None
	matched_quality_poster = None
	if base and first_b2_key and "/" in first_b2_key:
		dirname = first_b2_key.rsplit("/", 1)[0]
		# Try to choose the thumbnail that matches the highest-quality source we present
		try:
			m = re.search(r"/([^/]+)_(\\d{3,4})p\\.mp4$", first_b2_key)
			if m:
				clip_id = m.group(1)
				height = m.group(2)
				matched_quality_poster = f"{base}/{dirname}/{clip_id}_thumb_{height}p.jpg"
		except Exception:
			matched_quality_poster = None
		# Fallback to generic thumb.jpg if a quality-specific poster is not derivable
		derived_poster = f"{base}/{dirname}/thumb.jpg"

	poster_final = matched_quality_poster or poster or derived_poster or ""
	poster_attr = f' poster="{poster_final}"' if poster_final else ""
	html = f"""
<div class="video-embed">
	<video controls preload="none" width="620"{poster_attr}>
		{''.join(parts)}
		Your browser does not support the video tag.
	</video>
</div>
"""
	return Markup(html)


def parse_datetime(dt_str: str) -> Optional[datetime]:
	"""
	Parse various datetime formats we might store in SQLite for created_at.
	"""
	try:
		return datetime.fromisoformat(dt_str)
	except Exception:
		pass
	try:
		return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
	except Exception:
		pass
	try:
		# Day-only form
		d = datetime.strptime(dt_str, "%Y-%m-%d")
		return d
	except Exception:
		return None


def normalize_date_to_created_at(date_str: str) -> Optional[str]:
	"""
	Accepts YYYY-MM-DD from <input type='date'> and returns "YYYY-MM-DD 00:00:00".
	"""
	try:
		d = datetime.strptime(date_str, "%Y-%m-%d")
		return d.strftime("%Y-%m-%d 00:00:00")
	except Exception:
		return None


def format_date_input(dt_str: str) -> str:
	"""
	Format created_at to YYYY-MM-DD for <input type='date'> value.
	"""
	dt = parse_datetime(dt_str)
	if not dt:
		return ""
	return dt.strftime("%Y-%m-%d")


def group_events_by_month(events: list[sqlite3.Row]) -> list[dict]:
	"""
	Group events by (year, month) in descending chronological order.
	Assumes input events are already sorted newest-first.
	Returns a list of groups:
	[{ 'year': 2025, 'month': 11, 'month_name': 'November', 'anchor': 'y2025-11', 'events': [rows...] }, ...]
	"""
	groups: list[dict] = []
	current_key = None
	current_group = None
	for ev in events:
		dt = parse_datetime(ev["created_at"])
		if not dt:
			continue
		key = (dt.year, dt.month)
		if key != current_key:
			year, month = key
			month_name = dt.strftime("%B")
			anchor = f"y{year}-{month:02d}"
			current_group = {
				"year": year,
				"month": month,
				"month_name": month_name,
				"anchor": anchor,
				"events": [],
			}
			groups.append(current_group)
			current_key = key
		current_group["events"].append(ev)  # type: ignore[index]
	return groups


def load_env_from_files(paths: list[str]) -> None:
	"""
	Simple env loader for local development. Reads KEY=VALUE lines.
	Ignores empty lines and comments (# ...).
	Values from these files OVERRIDE existing environment vars to ensure local consistency.
	"""
	for p in paths:
		fp = p
		# also check alongside this file to avoid CWD issues
		if not os.path.exists(fp):
			fp = os.path.join(os.path.dirname(__file__), p)
		if not os.path.exists(fp):
			continue
		try:
			with open(fp, "r", encoding="utf-8") as f:
				for line in f:
					line = line.strip()
					if not line or line.startswith("#"):
						continue
					if "=" not in line:
						continue
					key, value = line.split("=", 1)
					key = key.strip()
					value = value.strip().strip('"').strip("'")
					os.environ[key] = value
		except Exception:
			# best-effort loader
			pass

if __name__ == "__main__":
	app = create_app()
	app.run(host="127.0.0.1", port=5000, debug=True)


