#!/usr/bin/env bash

if [ ! -d venv ]; then
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install --upgrade wheel
    if [ -s requirements.txt ]; then
        pip install --upgrade -r requirements.txt | tee setup.txt
    fi
fi

for i in data output work
do
    if [ ! -d ${i} ]; then
        mkdir ${i}
    fi
done

for i in {A..Z}
do
    if [ ! -d image/${i} ]; then
        mkdir -p image/${i}
    fi
done

source venv/bin/activate

echo get ODM data
FILE=ODM-2021-22-ALL-journeys-slim.gpkg
if [ ! -s data/${FILE} ]; then
    curl -o data/${FILE} -L https://automaticknowledge.org/flowdata/${FILE}
fi

echo process ODM data
if [ ! -s journeys-all.gpkg ]; then
    ./odm-path.py
fi

echo process image and markdown files
if [ ! -s station.md ]; then
    ./output-crs.py
    ./format-md.py
fi
