#!/bin/sh

awk -F ',,' '{
  gsub("[{}]", "", $2);
  gsub("[{}\[\]()]", "", $3);
  print $2","$3
}'
