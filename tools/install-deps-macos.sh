#!/usr/bin/env bash
set -euo pipefail

# Check for Homebrew
if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install from https://brew.sh"
  exit 1
fi

echo "Installing build dependencies..."
brew install pkg-config cmake

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "Installing ffmpeg..."
  brew install ffmpeg
fi

if ! command -v MP4Box >/dev/null 2>&1; then
  echo "Installing gpac (MP4Box)..."
  brew install gpac
fi

if ! command -v mp4edit >/dev/null 2>&1; then
  echo "Installing Bento4..."
  brew install bento4
fi

echo "Done."
