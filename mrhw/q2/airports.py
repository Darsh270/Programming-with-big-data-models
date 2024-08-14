
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAirportTraffic(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):  # Mapper function
        # Skip the header
        if line.startswith('ITIN_ID,YEAR'):
            return
        # Parsed csv lines into fields
        fields = line.split(',')
        origin = fields[3]
        dest = fields[5]
        passengers = float(fields[7])
        # tuple indicating direction and passenger count
        yield origin, ('depart', passengers)
        yield dest, ('arrive', passengers)

    def combiner(self, airport, values):  # combiner function
        departures = 0.0
        arrivals = 0.0
        # sums up the departure and arrival counts
        for direction, passengers in values:
            if direction == 'depart':
                departures += passengers
            else:
                arrivals += passengers
        # combined count for each airport
        if departures > 0:
            yield airport, ('depart', departures)
        if arrivals > 0:
            yield airport, ('arrive', arrivals)

    def reducer(self, airport, values):  # reducer function
        departures = 0.0
        arrivals = 0.0
        # sums up the departure and arrival counts
        for direction, passengers in values:
            if direction == 'depart':
                departures += passengers
            else:
                arrivals += passengers
        # Filter based on the gap criteria
        if abs(departures - arrivals) >= 9:
            yield airport, (departures, arrivals)
# runs the code
if __name__ == '__main__':
    MRAirportTraffic.run()

