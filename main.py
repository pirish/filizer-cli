import os
import argparse
import hashlib
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import shutil
import sys
from pathlib import Path
from typing import Optional
from collections import Counter

# Handle TOML compatibility for Python 3.10 vs 3.11+
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

CONFIG_DIR = Path.home() / ".config" / "filizer"
CONFIG_FILE = CONFIG_DIR / "cli-conf.toml"

def setup_logging(level: str, log_file: Optional[str]) -> None:
    """Configures logging with a dynamic level and optional file output."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers
    )

def init_config():
    """Generates a default TOML config file at ~/.config/filizer/cli-conf.toml"""
    if CONFIG_FILE.exists():
        print(f"Config already exists at {CONFIG_FILE}")
        return

    default_config = (
        "# Filizer CLI Configuration\n"
        f'url = "{os.getenv("FILIZER_URL", "https://api.example.com/v1/files")}"\n'
        'token = ""\n'
        'log = "filizer.log"\n'
        'level = "INFO"\n'
        'force = false\n'
        'exclude = [".git", "node_modules", "__pycache__", ".venv"]\n'
    )

    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(default_config)
        print(f"Created default configuration at: {CONFIG_FILE}")
    except Exception as e:
        print(f"Error creating config: {e}")

def load_config() -> dict:
    """Loads settings from the TOML config file."""
    if CONFIG_FILE.exists() and tomllib:
        try:
            with open(CONFIG_FILE, "rb") as f:
                return tomllib.load(f)
        except Exception as e:
            print(f"Warning: Failed to load config: {e}")
    return {}

def get_md5(file_path: Path) -> Optional[str]:
    """Generates an MD5 hash using chunked reading for memory efficiency."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(4096):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except (PermissionError, OSError) as e:
        logging.debug(f"Could not hash {file_path}: {e}")
        return None

def get_retrying_session() -> requests.Session:
    """Creates a session with exponential backoff retries for 5xx errors."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def execute_action(action: str, args: str, current_path: Path, force: bool) -> bool:
    """Executes local file operations requested by the server."""
    try:
        match action.lower():
            case "cp":
                dest = Path(args)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(current_path, dest)
                logging.info(f"ACTION: Copied {current_path.name} to {dest}")
                return True
            case "mv":
                dest = Path(args)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(current_path), str(dest))
                logging.info(f"ACTION: Moved {current_path.name} to {dest}")
                return True
            case "rm":
                if not force:
                    confirm = input(f"CONFIRM: Delete {current_path}? (y/N): ")
                    if confirm.lower() != 'y':
                        logging.info(f"ACTION: Skipped deletion of {current_path.name}")
                        return False
                current_path.unlink()
                logging.info(f"ACTION: Removed {current_path.name}")
                return True
        return False
    except Exception as e:
        logging.error(f"Action {action} failed for {current_path.name}: {e}")
        return False

def process_directory(target_dir: str, api_url: str, token: Optional[str], 
                      dry_run: bool, force: bool, excludes: list[str]) -> None:
    """Recursively scans directory, validates with API, and posts data."""
    root_path = Path(target_dir).resolve()
    stats = Counter(new=0, duplicate=0, path_match=0, failed=0, actions_taken=0)
    duplicate_parents = set()
    session = get_retrying_session()
    
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    if not root_path.is_dir():
        logging.error(f"Invalid directory: {root_path}")
        return

    logging.info(f"Scanning: {root_path} {'(DRY RUN)' if dry_run else ''}")

    for root, dirs, files in os.walk(root_path):
        if excludes:
            dirs[:] = [d for d in dirs if d not in excludes]

        current_dir = Path(root)
        for filename in files:
            file_path = current_dir / filename
            if not file_path.exists(): continue 
            
            md5_hash = get_md5(file_path)
            if not md5_hash:
                stats['failed'] += 1
                continue

            is_duplicate = False
            is_exact_path_match = False
            remote_action, remote_args = "", ""
            
            # 1. Validation (GET)
            try:
                params = {"name_eq": filename, "parent_dir_eq": current_dir.name, "md5_eq": md5_hash}
                res = session.get(api_url, params=params, headers=headers, timeout=10)
                
                match (res.status_code, res.json()):
                    case (200, list(items)) if items:
                        is_duplicate = True
                        duplicate_parents.add(current_dir.name)
                        remote_action = items[0].get("action", "")
                        remote_args = items[0].get("action_args", "")
                        
                        if any(i.get("full_path") == str(file_path) for i in items):
                            stats['path_match'] += 1
                            logging.info(f"Path match: {filename}")
                            is_exact_path_match = True
                        else:
                            stats['duplicate'] += 1
                    case (200, _):
                        stats['new'] += 1
                    case (401, _):
                        logging.error("Authentication failed. Check your token.")
                        return
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error during validation of {filename}: {e}")
                stats['failed'] += 1
                continue

            # 2. Remote Action Execution
            if is_duplicate and remote_action:
                if dry_run:
                    logging.info(f"[DRY-RUN] Would {remote_action} {filename}")
                elif execute_action(remote_action, remote_args, file_path, force):
                    stats['actions_taken'] += 1

            # 3. Data Submission (POST)
            if dry_run or not file_path.exists() or is_exact_path_match:
                continue

            try:
                payload = {
                    "name": filename, "size": file_path.stat().st_size,
                    "kind": file_path.suffix.lower() or "file", "md5": md5_hash,
                    "parent_dir": current_dir.name, "full_path": str(file_path),
                    "duplicate": is_duplicate
                }
                session.post(api_url, json=payload, headers=headers, timeout=10)
            except requests.exceptions.RequestException:
                stats['failed'] += 1

    # --- Summary Report ---
    summary = [
        "\n" + "="*40, "SCAN SUMMARY REPORT", "="*40,
        f"New Files Posted:      {stats['new']}",
        f"Duplicates Found:      {stats['duplicate']}",
        f"Exact Path Matches:    {stats['path_match']}",
        f"Actions Executed:      {stats['actions_taken']}",
        f"Failed Operations:     {stats['failed']}",
        "-"*40, "Parent Directories with Duplicates:",
        *((f" - {p}" for p in sorted(duplicate_parents)) if duplicate_parents else [" - None"]),
        "="*40
    ]
    for line in summary: logging.info(line)

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(description="Filizer CLI - Secure File Sync")
    parser.add_argument("--init", action="store_true", help="Generate default config")
    parser.add_argument("--url", nargs="?", default=os.getenv("FILIZER_URL", config.get("url")), help="API URL")
    parser.add_argument("--path", nargs="?", default=".", help="Scan path")
    parser.add_argument("--token", default=os.getenv("FILIZER_TOKEN", config.get("token")), help="Bearer Token")
    parser.add_argument("--log", default=config.get("log"), help="Log file path")
    parser.add_argument("--level", default=config.get("level", "INFO"), 
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Log level")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode")
    parser.add_argument("--force", action="store_true", default=config.get("force", False), help="Force 'rm'")
    parser.add_argument("--exclude", nargs="+", default=config.get("exclude", []), help="Excluded folders")
    
    args = parser.parse_args()

    if args.init:
        init_config()
        sys.exit(0)

    if not args.url:
        print("Error: API URL required. Run with --init or set FILIZER_URL.")
        sys.exit(1)

    setup_logging(args.level, args.log)
    process_directory(args.path, args.url, args.token, args.dry_run, args.force, args.exclude)

if __name__ == "__main__":
    main()