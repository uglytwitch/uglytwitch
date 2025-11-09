import os
import sys
import argparse
import traceback

# Import helpers from app.py
try:
	from app import (
		load_env_from_files,
		get_s3_client,
		s3_delete_all_versions_with_prefix,
		ensure_database_initialized,
		DATABASE_PATH,
	)
except Exception:
	# Fallback if relative import context differs
	sys.path.append(os.path.dirname(os.path.dirname(__file__)))
	from app import (  # type: ignore
		load_env_from_files,
		get_s3_client,
		s3_delete_all_versions_with_prefix,
		ensure_database_initialized,
		DATABASE_PATH,
	)


def wipe_b2_bucket_contents() -> tuple[int, int]:
	"""
	Delete all objects (including all historical versions and delete markers)
	from the configured B2 bucket using S3 API. Returns (deleted, errors).
	"""
	bucket = os.environ.get("B2_BUCKET")
	if not bucket:
		raise RuntimeError("B2_BUCKET is not set")
	s3 = get_s3_client()
	# Prefix "" to cover entire bucket
	deleted, errors = s3_delete_all_versions_with_prefix(s3, bucket, "")
	return deleted, errors


def wipe_database() -> None:
	"""
	Remove the SQLite database file completely and re-initialize a fresh schema.
	This resets autoincrement so events start again from ID 1.
	"""
	db_path = DATABASE_PATH
	try:
		if os.path.exists(db_path):
			os.remove(db_path)
	except FileNotFoundError:
		pass
	# Recreate the schema fresh
	ensure_database_initialized(db_path)


def main() -> int:
	parser = argparse.ArgumentParser(description="WIPE ALL DATA: B2 bucket contents and local SQLite DB.")
	parser.add_argument("--yes", action="store_true", help="Confirm destructive wipe without interactive prompt")
	args = parser.parse_args()

	# Load local environment first (env.txt > .env) so B2 credentials resolve
	load_env_from_files(["env.txt", ".env"])

	if not args.yes:
		print("Refusing to proceed without --yes. This operation is destructive.")
		return 2

	# Wipe B2 bucket contents (all versions)
	print("Deleting ALL objects and versions from B2 bucket...")
	deleted, errors = (0, 0)
	try:
		deleted, errors = wipe_b2_bucket_contents()
		print(f"B2: deleted={deleted}, errors={errors}")
	except Exception as e:
		print("ERROR wiping B2 bucket:", e, file=sys.stderr)
		traceback.print_exc()
		# Continue to wipe DB even if storage failed

	# Wipe local database file and re-init schema
	print("Deleting local SQLite database and reinitializing...")
	try:
		wipe_database()
		print("Database reset complete.")
	except Exception as e:
		print("ERROR wiping database:", e, file=sys.stderr)
		traceback.print_exc()
		return 1

	print("All done.")
	return 0


if __name__ == "__main__":
	sys.exit(main())


