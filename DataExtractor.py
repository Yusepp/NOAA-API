# Import request functions
from data_from_station import *
from explore_stations import stns_with_fld,stns_info
from noaahist import *
# Python standard libraries
import subprocess
import ujson
import os
import sys
from itertools import combinations
import numpy as np
from numpy.linalg import norm



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
    
    all_stations = stns_info()
    if len(columns) > 0: # If we found sparsity we need to fix it.
        # To do so we first check for the closest station to our zipcode
        # In order to retrieve the missing data (or try it)
        lat = lat.split(" ")[0] # First Lat
        lon = lon.split(" ")[0] # First Lon
        year = int(d[:4]) # First Year
        
        # We calculate dist to all stations
        for id in all_stations:
            dist = haversine(lat1=float(lat),lon1=float(lon),lat2=float(all_stations[id]['lat']),lon2=float(all_stations[id]['lon']))/0.621371
            all_stations[id]['dist'] = dist
        
        # We want to check if there are a combination of 3 stations that triangulate our current position
        n_search = 30
        is_triangulated = False
        while not is_triangulated:
            close_stations,stns_coord = N_closest_stations_by_distance(lat,lon,year,columns,all_stations,n_search,n_search)
            possible_triangulated = list(combinations(close_stations,3))
            is_triangulated, stns_triang = check_triag({"lat":float(lat),"lon":float(lon)},stns_coord,possible_triangulated,all_stations)
            n_search += 2
            
        # Result
        print "Is triangulated: " + str(is_triangulated)
        print "Triangulated Stations: "+str(stns_triang)
        print "Final rad: "+str(n_search)
        
        # We estimate weight for every station
        weights = barycentric_weights((float(lat),float(lon)),stns_triang,all_stations)
        print "Estimated weights: "+str(weights)+"\n"
        
        def retrieve_station(station,d):
            retrieved = False
            
            while not retrieved:
                try:
                    station_data = get_data_from_station("Q1",station," ".join(columns),d[:8],d[9:17])
                    return station_data,station
                except Exception as err: 
                    print err
         
        
        
        triag_stations_data = [retrieve_station(station,d) for station in stns_triang] 
        triag_stations_data = [x for x in triag_stations_data if x != None]
        
        for pos,triag_data in enumerate(triag_stations_data):
            data,station = triag_data
            if data != None:
                if not os.path.isdir("./fix/"+str(year)): 
                    os.mkdir("./fix/"+str(year))
                    
                if 'Q1' in data:
                    path  = "./fix/"+str(year)+"/"+str(pos+1)+".csv"
                    f = open(path, "w")
                    f.write(data)
                    f.close()
                    print "Created: "+path
                    
        
        print "Interpolating Data..."
        command = "python3 InterpolateData.py ./fix/"+str(year)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        print " Done\n"
        
        print "Creating Dataset using Weigths..."
        command = "python3 createDataset.py ./fix/"+str(year)+" "+weights["u"]+" "+weights["v"]+" "+weights["w"]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        print " Done\n"
        
        
def sign(p1,p2,p3):
        # Calculation
        return (p1["lat"] - p3["lat"]) * (p2["lon"] - p3["lon"]) - (p2["lat"] - p3["lat"]) * (p1["lon"] - p3["lon"])


def test_triag(current_coords,p1,p2,p3):
    d1 = sign(current_coords,p1,p2)
    d2 = sign(current_coords,p2,p3)
    d3 = sign(current_coords,p3,p1)
    
    has_neg = (d1 < 0) or (d2 < 0) or (d3 < 0) 
    has_pos = (d1 > 0) or (d2 > 0) or (d3 > 0)
    
    return not(has_neg and has_pos)


def check_triag(current_coords,stns_coord,possible_triangulated,all_stations):
    # Check all posible triag
    result = [[p1,p2,p3] for p1,p2,p3 in possible_triangulated if test_triag(current_coords,stns_coord[p1],stns_coord[p2],stns_coord[p3])]
    
    # Not Found
    if len(result) == 0:
        return False, result
    
    # Just one found 
    elif len(result) == 1:
        print "1 Found"
        return True, result[0]
    
    else:
        print "Searching closest: "+str(result)
        #  2+ We choose the closest ones
        result = min([((all_stations[p1]['dist']+all_stations[p2]['dist']+all_stations[p3]['dist']),[p1,p2,p3]) for p1,p2,p3 in result])
        return True, result[1]

def get_pos(all_stations,stns_id):
    return np.array((float(all_stations[stns_id]['lat']),float(all_stations[stns_id]['lon'])))

def area(A,B,C):
    return 0.5*norm(np.cross(B-A,C-A))

def barycentric_weights(P,stns_triang,all_stations):
    P = np.array(P)
    A = get_pos(all_stations,stns_triang[0])
    B = get_pos(all_stations,stns_triang[1])
    C = get_pos(all_stations,stns_triang[2])
    
    u = area(C,A,P)/area(A,B,C)
    v = area(A,B,P)/area(A,B,C)
    w = area(B,C,P)/area(A,B,C)
    
    return {"u":u,"v":v,"w":w}


def N_closest_stations_by_distance(lat,lon,year,columns,all_stations,N,d):
    # Retrieve N closest stations with fields at coords
    close_stations_fields = [stns_with_fld(col, latitude=lat, longitude=lon, year=year, N=N) for col in columns] 
    # First set of stations
    close_stations = set(close_stations_fields[0])
    # Check intersection so we get stations with all the infos
    for close_station in close_stations_fields:
        close_stations = close_stations.intersection(set(close_station))
        
    # Then we remove the ones that are too far
    close_stations = [stn for stn in close_stations if all_stations[stn]['dist'] < d] 
    
    # We create a set with coords of the chosen stations   
    stns_coord = {station:{"lat":float(all_stations[station]["lat"]),
                            "lon":float(all_stations[station]["lon"])} 
                    for station in close_stations}
     
    return close_stations,stns_coord 
    
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
    print output
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
