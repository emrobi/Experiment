var fs = require("fs");
var elasticsearch = require("elasticsearch");

var filePath = "/home/ubuntu/workspace/data/methow.geojson";
var args = process.argv;

if(args.length > 2) {
    filePath = args[2]; // File to process. 
}

var connectionString = "http://localhost:9200";
var client = new elasticsearch.Client({
    host: connectionString,
    log: 'trace'
});


console.log("Processing file: ", filePath);

var stream = fs.createReadStream(filePath, {flags: 'r', encoding: 'utf-8'});
var buf = '';

stream.on('data', function(d) {
    buf += d.toString(); // when data is read, stash it in a string buffer
    pump(); // then process the buffer
});

stream.on('close', function() { 
    if(client) {
        client.close(); 
        client = undefined;
    }
});
stream.on('end', function() { 
    if(client) {
        client.close(); 
        client = undefined;
    }
});

function pump() {
    var pos;

    while ((pos = buf.indexOf('\n')) >= 0) { // keep going while there's a newline somewhere in the buffer
        if (pos === 0) { // if there's more than one newline in a row, the buffer will now start with a newline
            buf = buf.slice(1); // discard it
            continue; // so that the next iteration will start with data
        }
        processLine(buf.slice(0,pos)); // hand off the line
        buf = buf.slice(pos+1); // and slice the processed data off the buffer
    }
}

function processLine(line) { // here's where we do something with a line

    if (line[line.length-1] == '\r') line=line.substr(0,line.length-1); // discard CR (0x0D)
    line.trim();
    if (line[line.length-1] == ',') line=line.substr(0,line.length-1); // discard CR (0x0D)

    try {
        if (line.length > 0) { // ignore empty lines
            var obj = JSON.parse(line); // parse the JSON
            transform(obj);
            persist(obj);
        }
    } catch(error) {
        console.log(error)
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
    console.log(JSON.stringify(obj));
//            console.log(obj.properties.GNIS_NAME, obj.geometry.coordinates); // do something with the data here!

    client.index({
        index: 'hydrography',
        type: 'feature',
        id: obj.properties.PERMANENT_IDENTIFIER,
        body: obj
    }, function(error, response){
        if(error) {
            console.log(error);
        } else {
            console.log(response);
        }
    });    
}