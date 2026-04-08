from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent
FRONTEND_DIR = ROOT / "frontend"
PUBLIC_DIR = ROOT / "public"


def main() -> None:
    PUBLIC_DIR.mkdir(exist_ok=True)
    shutil.copytree(FRONTEND_DIR, PUBLIC_DIR, dirs_exist_ok=True)
    print(f"Copied {FRONTEND_DIR} -> {PUBLIC_DIR}")


if __name__ == "__main__":
    main()

