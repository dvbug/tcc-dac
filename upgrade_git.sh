#!/bin/sh

type git >/dev/null 2>&1 || { echo >&2 "I require git but it's not installed.  Aborting.";return; }

git fetch --all
git reset --hard origin/master