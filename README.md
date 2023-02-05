# atds-ntua-2023
Advanced Topics of Database Systems - Group 55 - NTUA

We are using a [PySpark](https://spark.apache.org/) Standalone Cluster implemented with 2 linux VMs from [Okeanos](https://okeanos-knossos.grnet.gr).
We also use a distributed file system -[Apache Hadoop](https://hadoop.apache.org/).

## Team Members

- [Mikaela Ksilia](https://github.com/mikaelaksil),

- [Nikoletta Saliari](https://github.com/nicolettasal)

## Assignment

We have to implement 5 Queries described below:


| Query | Description                                                                                                                                                                                                                                                                                                                                |
|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Q1    | Find the route with the biggest tip in March with point of arrival "Battery Park".                                                                                                                               |
| Q2    | Find, for each month, the route with the highest amount in tolls. Ignore zero amounts.                                                                                                                                                                                                                                                           |
| Q3    | Find, per 15 days, the average distance and cost for all routes with a point of departure different from the point of arrival.                                                                                                                                                                                                                                            |
| Q4    | Find the three longest (top 3) peak hours per day of the week, meaning the times (eg, 7-8am, 3-4pm, etc) of the day with the largest number passengers in a taxi route. Calculate for every month.                                                                                                                                                         |
| Q5    | Find the top five (top 5) days per month on which the routes had the largest percentage in tip. For example, if the route cost $10 (fare_amount) and the tip was $5, the percentage is 50%.|

For every query we are using SQL-API or Dataframe API. Also, for Q3 we create an additional implementation using RDD-API.
