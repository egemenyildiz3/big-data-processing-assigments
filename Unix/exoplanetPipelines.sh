#!/usr/bin/env bash

echo "Running exoplanetProcessing.sh"

# ---- APACHE ----
cd ./../data/planets || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipline that for all planets that were discovered using Pulsar Timing displays the discovery year, name  and facility separated by spaces.
# Example: 2017 PSR B0329+54 b Multiple Facilities
pulsarTiming=$(grep 'Pulsar Timing' exoplanets | awk -F',' '{print $6, $1, $7}')
# Print pulsarTiming
echo "Pulsar Timing:"
echo "$pulsarTiming"


# -- Q2 --
echo "-- Q2 --"
# Write a pipeline that finds the year in which most exoplanets were discovered. Also provide the number of planets discovered that year.
# Example: 1505 2016
highestYear=$(cut -d ',' -f6 exoplanets | sort | uniq -c | sort -nr | head -n1)
echo "Highest year:"
echo "$highestYear"

# -- Q3 --
echo "-- Q3 --"
# Write a pipeline that counts the number of exoplanets discovered between 1997 and 2006
intervalPlanets=$(awk -F',' '{print $6}' exoplanets | grep -E '(199[7-9]|200[0-6])' | wc -l)
echo "Planets in interval:"
echo "$intervalPlanets"

# -- Q4 --
echo "-- Q4 --"
# Write a pipeline that outputs the names of two exoplanets that have the highest number of stars in the planetary system.
highestStars=$(grep -v "#" exoplanets  | sort -t',' -k3nr | head -n2 | awk -F',' '{print $1}')
echo "Highest stars:"
echo "$highestStars"

cd ../../pipelines/ || exit
