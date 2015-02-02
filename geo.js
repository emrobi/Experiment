var fs = require("fs");
var elasticsearch = require("elasticsearch");

var filePath = "/home/ubuntu/workspace/data/methow.geojson";
var args = process.argv;

if(args.length > 2) {
    filePath = args[2]; // File to process. 
}

var connectionString = "http://localhost:9200";
var defaultIndex = "hydro";

var client = new elasticsearch.Client({
    host: connectionString,
    log: 'error'
});

createIndex(defaultIndex, undefined, processFile);

var buf = '';
var stream = {};

function processFile() { 

    console.log("Processing file: ", filePath);

    stream = fs.createReadStream(filePath, {flags: 'r', encoding: 'utf-8'});

    
    stream.on('data', function(d) {
        buf += d.toString();
        stream.pause();
        pump();
    });
    
}

/* Got a problem with ES closing too soon. 
stream.on('close', function() { 
    if(client) {
        client.close(); 
        // client = undefined;
    }
});
stream.on('end', function() { 
    if(client) {
        client.close(); 
        // client = undefined;
    }
});
*/

function getData() {
    var pos;


    // Do we need data
    if ((pos = buf.indexOf('\n')) < 0) {
        stream.resume();
    }
}

function pump() {

    getData();

    var pos;
    if ((pos = buf.indexOf('\n')) >= 0) { // keep going while there's a newline somewhere in the buffer
        var line = buf.slice(0, pos);
        buf = buf.slice(pos+1); // and slice the processed data off the buffer
        try {
            processLine(line); // hand off the line
        } catch(err) {
            logResponse(err);
            setTimeout(pump, 0);
        }
    }
}

function processLine(line) { // here's where we do something with a line

    if (line[line.length-1] == '\r') line=line.substr(0,line.length-1); // discard CR (0x0D)
    line.trim();
    if (line[line.length-1] == ',') line=line.substr(0,line.length-1); // discard CR (0x0D)

    if (line.length > 0) { // ignore empty lines
        var obj = JSON.parse(line); // parse the JSON
        transform(obj);
        persist(obj);
    }
}

// Need to clean up the geometry before putting it into ElasticSearch
function transform(obj) {
    var geo = obj.geometry;
    
    
    if( geo.type === "MultiLineString") {
        // Convert [[[lon,lat]...]] tp [[lon,lat]...]
        if(geo.hasOwnProperty('coordinates')) {
            geo.coordinates = geo.coordinates[0];
            geo.coordinates = geo.coordinates.map (function(c) {
                if(c.length > 2)
                    c = c.slice(0,2);
                return c;
            });
        }
    }
}

/* obj should be a fully-formed geojson object
*/
function persist(obj) {
    //console.log(JSON.stringify(obj));
    //console.log(obj.properties.GNIS_NAME, obj.geometry.coordinates); // do something with the data here!

    client.index({
        index: defaultIndex,
        type: 'feature',
        id: obj.properties.PERMANENT_IDENTIFIER,
        body: obj
    }, function (error, response){
        logResponse(error, response);
        setTimeout(pump, 0);
    });  
}

function createIndex(value, errorFunc, successFunc) {

    // Only create an index if the current one doesn't exists.
    client.indices.exists({
        index: value
    }, function (error, response) {
        if(error) {
            if(errorFunc) {
                errorFunc(error);
            }
        }

        // check repsonse
        if( response === true) {
            successFunc();
            return;
        }

        client.indices.create({
            index: value,
            body: {
                mappings: {
                    feature: {
                        properties: {
                            geometry : {
                                properties: {
                                    coordinates: {
                                        type: 'geo_point'
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }, function(error, response) {
            if(error) {
                if(errorFunc) 
                    errorFunc(error);
            }
            else {
                if( successFunc )
                    successFunc();
            }
        });
    });   
}

 

function logResponse(error, response) {
    if(error) {
        console.log("Bogus: ", error);
    } else {
        // console.log("Success: ", response);
    }
}