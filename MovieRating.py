from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageRating(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
	    print(key + values);
	    totalRatings = sum(values);
	    key = [key[0], (sum(key[1])/totalRatings)]
        yield key, totalRatings


if __name__ == '__main__':
    AverageRating.run()