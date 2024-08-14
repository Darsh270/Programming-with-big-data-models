
from mrjob.job import MRJob
from mrjob.step import MRStep

class ZipDistribution(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
     
        data = line.split("\t")
        zip_codes = data[5].strip()
        number_zip_codes = len(zip_codes.split()) if zip_codes else 0
        yield number_zip_codes, 1

    def combiner(self, number_zip_codes, counts):
        yield number_zip_codes, sum(counts)

    def reducer(self, number_zip_codes, counts):
        yield number_zip_codes, sum(counts)

if __name__ == '__main__':
    ZipDistribution.run()
