#!/usr/bin/env bash
set -x

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
    if [ ! -d vector/${i} ]; then
        mkdir -p vector/${i}
    fi
done

source venv/bin/activate

echo get network data
FILE=network-model.gpkg
URL=https://github.com/openraildata/network-rail-gis/releases/download/20230317-01/
if [ ! -s data/${FILE} ]; then
    curl -o data/${FILE} -L "${URL}/${FILE}?raw=true"
fi

echo process ODM data
if [ ! -s journeys-all.gpkg ]; then
    ./odm-station.py
    ./odm-path.py
fi

echo process image and markdown files
if [ ! -s station-update.md ]; then
    ./output-crs.py
    for i in {A..Z}
    do    
	./create-git.sh ${i}
    done
    ./format-md.py image station.md
    ./format-md.py image station-update.md --gif
fi
