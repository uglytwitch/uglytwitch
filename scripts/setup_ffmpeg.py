import os
import sys
import shutil
import tarfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools" / "ffmpeg"
TOOLS.mkdir(parents=True, exist_ok=True)


def download(url: str, dest: Path) -> Path:
	dest.parent.mkdir(parents=True, exist_ok=True)
	tmp = dest.with_suffix(".download")
	print(f"Downloading {url} -> {tmp}")
	urlretrieve(url, tmp.as_posix())
	tmp.rename(dest)
	return dest


def extract_zip(zip_path: Path, dest_dir: Path) -> None:
	with zipfile.ZipFile(zip_path, "r") as z:
		z.extractall(dest_dir)


def extract_tar_xz(tar_path: Path, dest_dir: Path) -> None:
	with tarfile.open(tar_path, "r:xz") as t:
		t.extractall(dest_dir)


def ensure_ffmpeg() -> Path:
	platform = sys.platform
	target_bin = TOOLS / ("ffmpeg.exe" if platform.startswith("win") else "ffmpeg")
	if target_bin.exists():
		print(f"ffmpeg already present at {target_bin}")
		return target_bin

	if platform.startswith("win"):
		# Stable essentials zip from gyan.dev (contains bin/ffmpeg.exe)
		url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
		archive = TOOLS / "ffmpeg.zip"
		download(url, archive)
		extract_zip(archive, TOOLS)
		# find ffmpeg.exe under extracted dir
		ffbin = None
		for p in TOOLS.glob("ffmpeg-*/*/ffmpeg.exe"):
			ffbin = p
			break
		if not ffbin:
			# alternate layout: ffmpeg-*/bin/ffmpeg.exe
			for p in TOOLS.glob("ffmpeg-*/bin/ffmpeg.exe"):
				ffbin = p
				break
		if not ffbin or not ffbin.exists():
			raise RuntimeError("Could not locate ffmpeg.exe in the downloaded archive")
		shutil.copy2(ffbin, target_bin)
		return target_bin

	if platform.startswith("linux"):
		# Static Linux build (amd64)
		url = "https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz"
		archive = TOOLS / "ffmpeg.tar.xz"
		download(url, archive)
		extract_tar_xz(archive, TOOLS)
		ffbin = None
		for p in TOOLS.glob("ffmpeg-*static/ffmpeg"):
			ffbin = p
			break
		if not ffbin or not ffbin.exists():
			raise RuntimeError("Could not locate ffmpeg in the downloaded archive")
		shutil.copy2(ffbin, target_bin)
		target_bin.chmod(0o755)
		return target_bin

	if platform == "darwin":
		# macOS universal zip (community build)
		url = "https://evermeet.cx/ffmpeg/ffmpeg-6.1.1.zip"
		archive = TOOLS / "ffmpeg-mac.zip"
		download(url, archive)
		extract_zip(archive, TOOLS)
		ffbin = TOOLS / "ffmpeg"
		if not ffbin.exists():
			# sometimes zip contains nested name
			for p in TOOLS.glob("ffmpeg*"):
				if p.name == "ffmpeg":
					ffbin = p
					break
		if not ffbin.exists():
			raise RuntimeError("Could not locate ffmpeg in the downloaded archive")
		shutil.copy2(ffbin, target_bin)
		target_bin.chmod(0o755)
		return target_bin

	raise RuntimeError(f"Unsupported platform: {platform}")


def append_env(path: Path) -> None:
	env_file = ROOT / "env.txt"
	line = f"FFMPEG_PATH={path.as_posix()}"
	if env_file.exists():
		content = env_file.read_text(encoding="utf-8")
		if "FFMPEG_PATH=" in content:
			print("FFMPEG_PATH already present in env.txt; not modifying.")
			return
		with env_file.open("a", encoding="utf-8") as f:
			f.write(("\n" if not content.endswith("\n") else "") + line + "\n")
		print(f"Appended to env.txt: {line}")
	else:
		env_file.write_text(line + "\n", encoding="utf-8")
		print(f"Created env.txt with: {line}")


if __name__ == "__main__":
	ff = ensure_ffmpeg()
	print(f"ffmpeg ready at {ff}")
	try:
		append_env(ff)
	except Exception as e:
		print(f"Note: could not append to env.txt automatically: {e}")


