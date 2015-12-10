#!/bin/bash

type virtualenv >/dev/null 2>&1 || { echo >&2 "$0 requires virtualenv but it's not installed.  Aborting."; exit 1; }
type pip >/dev/null 2>&1 || { echo >&2 "$0 requires pip but it's not installed.  Aborting."; exit 1; }

if [ ! -f ./.env/bin/python ]; then
    echo "Installing virtualenv..."
    virtualenv .env
    if [ ! $? -eq 0 ]; then
        echo "virtualenv installation failed, exiting"
        exit 1
    fi
fi


./.env/bin/pip install -r requirements.pip
if [ ! $? -eq 0 ]; then
    echo "pip installation failed, exiting"
    exit 1
fi
