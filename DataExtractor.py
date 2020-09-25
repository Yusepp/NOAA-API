# Import request functions
from data_from_station import *
from explore_stations import stns_with_fld
from noaahist import *
# Python standard libraries
import subprocess
import ujson
import os
import sys



def get_data(d,z,lat,lon,f,o):
    """[summary]

    Args:
        - d ([type]): --date: a single date or a start date and end date in YYYYMMDD format | 19710321 19710323 | 19710321
        - z ([type]): --zips: one or more zip codes | 89109 | 89109 89110
        - lat ([type]): --lats: latitudes | 34.05 | 34.05 34.893
        - lon ([type]): --lons: longitudes (length and order must agree with --lats args) | -118.25 | -118.25 -117.019
        - f ([type]): --flds: keys for the data you would like (see NOAA fields below) | SPD | SPD TEMP
        - p ([type]): automatically detect number of available processors, N, and run requests in parallel on N-1 processors
        - nprocs ([type]): explicitly set how many processors to use (ignored if -p is passed)
        - i ([type]): --infile: to run many requests at once, pass in a formatted text file with one request specified per line
        - o ([type]): --outfile: redirect comma-separated output lines (defaults to stdout) | fllv.csv
        - m ([type]): --metadata: feeback on which stations data was pulled from; if outfile is specified, 
                                  written to outfilename_metadata.txt, else printed to STDOUT
    """
    
    os.mkdir("./fix")
    
    retrieve_data(d,z,lat,lon,f,o)
        
    
    # Then we check if data is sparse
    [columns,rows] = check_data(o)
    
    if len(columns) > 0: # If we found sparsity we need to fix it.
        # To do so we first check for the closest station to our zipcode
        # In order to retrieve the missing data (or try it)
        lat = lat.split(" ")[0] # First Lat
        lon = lon.split(" ")[0] # First Lon
        year1 = int(d[:4]) # First Year
        
        if len(d) > 8:
            year2 = int(d[9:13]) # Second Year
            station1 = stns_with_fld(columns[0], latitude=lat, longitude=lon, year=year1, N=5)
        
        else:
            station1 = stns_with_fld(columns, latitude=lat, longitude=lon, year=year1, N=5) # Station1
        
        
        def retrieve_station(station):
                try:
                    station_data = get_data_from_station("Q1",station,columns[0],d[:8],d[9:17])
                    return station_data,station
                except: 
                    return None
            
        all_stations_data = [retrieve_station(station) for station in station1] 
            
        for data,station in all_stations_data:
            if data != None:
                
                if not os.path.isdir("./fix/"+str(year1)): 
                    os.mkdir("./fix/"+str(year1))
                    
                path  = "./fix/"+str(year1)+"/"+str(station)+".csv"
                f = open(path, "w")
                f.write(data)
                f.close()
                print "Created: "+path
        
    
    
    
    
def retrieve_data(d,z,lat,lon,f,o):
    # Print Info
    print "Extracting Additional data:\n"
    print " - Data: "+d+"\n"
    print " - ZipCode: "+z+"\n"
    print " - Latitude: "+lat+"\n"
    print " - Longitude: "+lon+"\n"
    print " - Fields: "+f+"\n"
    print " - Output file: "+o+"\n"
    print "Extracting..."
    
    # Execute command
    command = "python2 ./noaahist.py -d "+d+" -z "+z+" --lats "+lat+" --lons "+lon+" -f "+f+" -p --outfile "+o+" -m"
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    # CSV generated
    print " Generated "+o+"\n\n"
    

def check_data(o):
    # Check NaN columns and rows
    print "Checking retrieved data..."
    command = "python3 analyzeData.py "+o
    p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    print " Done\n"
    data = ujson.loads(output)
    data[0] = [element.encode('utf-8') for element in data[0]]
    
    # Print Info
    print "Sparse Columns: "
    print data[0]
    print "\n"
    print "Number of affected rows: "
    print len(data[1])
    print "\n\n"
    
    return data

def get_data_from_station(n,i,f,s,e):
    """  Retrieve selected data from a concrete station

    Args:
        - n ([type]): --queryname: for convenient grouping of returned data | DC_weather
        - i ([type]): --stn_id: the USAF ID of the station from which to pull data | 724067-93744
        - f ([type]): --flds: one or more field names | TEMP SPD
        - s ([type]): --startdate: date in YYYYMMDD format | 20131107
        - e ([type]): --enddate: date in YYYYMMDD format | 20131110
    """
    # Print Info
    print "Extracting Additional data:\n"
    print " - Queryname: "+n+"\n"
    print " - Station ID: "+i+"\n"
    print " - Fields: "+f+"\n"
    print " - From "+s+" to "+e+"\n"
    print "Extracting..."
    
    # Execute command
    command_fix_sparse = "python2 ./data_from_station.py -n "+n+" -i "+i+" -f "+f+" -s "+s+" -e "+e
    p = subprocess.Popen(command_fix_sparse, stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    
    # Get Result
    print " Done\n"   
    return output

fields = " ".join(sys.argv[6:len(sys.argv)-1])
get_data(d=sys.argv[1]+" "+sys.argv[2],z=sys.argv[3],lat=sys.argv[4],lon=sys.argv[5],
         f=fields,o=sys.argv[-1])
#get_data(d="19900321" "19900323",z="89109",lat="34.05 34.893",lon="-118.25 -117.019",
#         f="TEMP DIR MAX MIN PCP01",o="tmp.csv")
