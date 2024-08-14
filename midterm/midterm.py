from mrjob.job import MRJob

class FlightBooking(MRJob):

    def mapper(self, _, line):

        if line.startswith('ITIN_ID'):
            return
        fields = line.split(',')
        origin = fields[3]
        passengers = float(fields[7])
        solo_booking = 1 if passengers == 1 else 0
        yield origin, (1, solo_booking)

    def combiner(self, origin, counts):
        total, solo = map(sum, zip(*counts))
        yield origin, (total, solo)

    def reducer(self, origin, counts):
        total, solo = map(sum, zip(*counts))
        fraction_solo = solo / total if total > 0 else 0
        yield origin, fraction_solo

if __name__ == '__main__':
    FlightBooking.run()
