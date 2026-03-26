#!/bin/sh

SED_EXEC="sed -i"
if [ "$(uname)" = "Darwin" ]; then
  SED_EXEC="sed -i ''"
fi

img_file="images_lang=en.ttl"

pbzip2 -dk "$img_file.bz2"

$SED_EXEC 's/\\\\.//g' "$img_file"
$SED_EXEC ':begin s/\(<[^> ]\+\)\s/\1_/g; t begin;' "$img_file"

pbzip2 -c "$img_file"
