#!/bin/bash
chmod +x ./mapper.py
chmod +x ./reducer.py
echo "Das ist ein Test
Das ist ein anderer Test" | ./mapper.py | sort | ./reducer.py 
