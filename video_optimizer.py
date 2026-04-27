#!/usr/bin/env python3
"""Entrypoint shim for video_optimizer."""

import sys
from pathlib import Path

# Allow running directly without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from optimizer.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
