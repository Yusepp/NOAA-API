##### OVERVIEW 
Python 2 & 3 API to get non sparse historical data from the NOAA database from a certain coordinate (latitude, longitude).

The original API [Base code](https://github.com/stewartwatts/noaahist) is obtained from stewartwatts' repo so let's give him some credit too! 

I'm focusing on improving this API so We can obtain a complete dataset without missing fields.

I'll use this data for my Final Degree Thesis

##### DEPENDENCIES 
Both Python2 and Python3 are mandatory. Also a Linux based distro (Windows is not supported).
curl, gunzip, Java Runtime Environment / compiler, pyzipcode (if you pass zip codes instead of latitude,longitude), numpy, pandas, geopy

##### DATA SOURCE 
ftp://ftp.ncdc.noaa.gov/pub/data/noaa/ 