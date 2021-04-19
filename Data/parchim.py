#!/usr/bin/env python3
import fileinput
import os

for root, dirs, files in os.walk("./testing"):
    for filename in files:
        if filename.endswith(".json"):
            fullpath=os.path.join(root, filename)
            print(fullpath)
            with fileinput.FileInput(fullpath, inplace=True, backup='.bak') as file:
                for line in file:
                    print(line.replace("Parchim	City", "Parchim City"), end='')