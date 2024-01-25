#!/bin/bash

for i in {01..18}; do ssh -f skadi-$i "xbutil reset -d 0000:17:00.1 --force; xbutil reset -d 0000:65:00.1 --force"; done
