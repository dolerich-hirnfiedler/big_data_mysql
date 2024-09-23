#!/usr/bin/env python3

import os

# for name in os.listdir("./dataset/Data/"):
#     print(name)

file_path = './dataset/labeled_ids.txt'

# Initialize an empty list to store the lines
lines = []

# Open the file and read it
with open(file_path, 'r') as file:
    # Read each line in the file, strip any trailing newline characters, and add to the list
    # lines = file.readlines()
    lines = [line.strip() for line in file]

# Alternatively, to remove newline characters:

# Print the list to see the output
print(lines)
