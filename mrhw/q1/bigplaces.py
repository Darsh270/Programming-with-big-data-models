
from mrjob.job import MRJob
from mrjob.step import MRStep

class Airports(MRJob):
# Mapper function
    def mapper_price(self, _, line):
       # csv file into fields 
        fields = line.split('\t')
        # Skip header
        if len(fields) == 7 and fields[4].isdigit():
            state_abbreviation = fields[1]
            county = fields[3]
            population = int(fields[4])
            # Checks if city is big
            if population >= 80000:
                # Emit (state abbreviation, county) tuple as the key, with value 1
                yield (state_abbreviation, county), 1

    def reducer_price(self, key, values):
        # Sum the values for each (state abbreviation, county) key
        total_big_cities = sum(values)
        # Emit state abbreviation with a value of 1 if this county is big
        if total_big_cities >= 2:
            yield key[0], 1

    def mapper_cid(self, key, value):
        # Pass through the state abbreviation and the count
        yield key, value

    def reducer_cid(self, key, values):
        # Sum the values for each state abbreviation, which gives the number of big counties
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_price, reducer=self.reducer_price),
            MRStep(mapper=self.mapper_cid, reducer=self.reducer_cid)
        ]
# runs the code
if __name__ == '__main__':
    Airports.run()

