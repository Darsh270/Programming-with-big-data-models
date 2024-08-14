import csv

file_path = 'ds410hw/testdata/flight-data/data.txt'

with open(file_path, mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
