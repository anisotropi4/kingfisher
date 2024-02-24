#!/usr/bin/env bash
#set -x

LETTER=${1:A}
FILESTUB=$(ls image/${LETTER}/???-rail.png | sed 's/-rail.png//')
for j in ${FILESTUB}
do
    CRS=$(basename ${j})
    FILES=$(ls image/${LETTER}/${CRS}-20*.png)
    echo ${CRS} ${FILES}
    if [ ! -s image/${LETTER}/${CRS}-rail.gif ]; then
	convert -delay 150 ${FILES} -alpha remove -loop 0 image/${LETTER}/${CRS}-rail.gif
    fi
done

