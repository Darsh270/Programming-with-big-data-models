
from mrjob.job import MRJob
from mrjob.step import MRStep  

class CityStats(MRJob): #Class Name
#Mapper function
    def mapper(self, _, line):
        # Check for header and skip
        if "City" in line and "State" in line and "Population" in line:
            return
        columns = line.split("\t") 
        if len(columns) < 6:
            return
        # Extracts required information
        stateCode = columns[2]  
        popStr = columns[4]  
        if popStr.isdigit():
            cityPop = int(popStr)  
        else:
            return
        zipList = columns[5].split(" ") 
        yield stateCode, (1, cityPop, len(zipList))
#Combiner function
    def combiner(self, stateCode, counts):  
        cityTotal = 0  
        popSum = 0  
        maxZip = 0  
        for count, pop, zips in counts:
            cityTotal += count
            popSum += pop
            maxZip = max(maxZip, zips)
        yield stateCode, (cityTotal, popSum, maxZip)

    def reducer(self, stateCode, aggregates):
        totalCities = 0  
        totalPop = 0  
        highestZipCount = 0  
        for count, pop, zips in aggregates:
            totalCities += count
            totalPop += pop
            highestZipCount = max(highestZipCount, zips)
        yield stateCode, (totalCities, totalPop, highestZipCount)
#Steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)
        ]
#Run code
if __name__ == '__main__':
    CityStats.run()

