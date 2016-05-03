var fs = require("fs");
var elasticsearch = require("elasticsearch");
var path = require("path");

var dataPath = "/Users/eric/dev/data/reachcode"
var filePath = "/Users/eric/dev/data/hydro_waterbody.geojson";
var args = process.argv;

if(args.length > 2) {
    filePath = args[2]; // File to process.
}

var connectionString = "ubuntu:9200";
var defaultIndex = "hydro7";

var client = new elasticsearch.Client({
    apiVersion: '2.1',
    host: connectionString,
    log: 'error',
    sniffOnStart: true,
    sniffOnConnectionFault: true
});

var buf = '';
var stream = {};

createIndex(defaultIndex, undefined, processFile);

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


function pump() {

    var pos;
    var arr = [];

    while ((pos = buf.indexOf('\n')) >= 0) { // keep going while there's a newline somewhere in the buffer
        if( pos === 0 ) {
            buf = buf.slice(1);
            continue;
        }
        var line = buf.slice(0, pos);
        buf = buf.slice(pos+1); // and slice the processed data off the buffer
        try {
            var obj = processLine(line);
            if( obj ) {
                arr.push(obj);
            }
        } catch(err) {
            logResponse(err);
        }
    }

    persist(arr);
}

function processLine(line) { // here's where we do something with a line

    if (line[line.length-1] == '\r') line=line.substr(0,line.length-1); // discard CR (0x0D)
    line.trim();
    if (line[line.length-1] == ',') line=line.substr(0,line.length-1); // discard CR (0x0D)

    if (line.length > 0) { // ignore empty lines
        var obj = JSON.parse(line); // parse the JSON
        if (obj.properties.GNIS_NAME ) {
            transform(obj);
            return obj;
        }
        // console.log("Dropped obj");
        return null;
    }
}

/// Need to clean up the geometry before putting it into ElasticSearch
function transform(obj) {
    var geo = obj.geometry;

    geo.type = geo.type.toLowerCase();
    //if( geo.type === "MultiLineString") {
        //geo.type = "multilinestring";
        // Convert [[[lon,lat]...]] tp [[lon,lat]...]
        //if(geo.hasOwnProperty('coordinates')) {
            // geo.coordinates = geo.coordinates[0];
            //geo.coordinates = geo.coordinates.map (function(c) {
            //    if(c.length > 2)
            //        c = c.slice(0,2);
            //    return c;
            //});
        //}
    //}

    // console.log(obj);
    return obj;
}

/* obj should be a fully-formed geojson object
*/
function persist(arr) {
    //console.log(JSON.stringify(obj));
    //console.log(obj.properties.GNIS_NAME, obj.geometry.coordinates); // do something with the data here!

    var count = arr.length;
    if(count === 0) {
        stream.resume();
        return;
    }

    for (var i = 0; i < arr.length; i++) {
        var obj = arr[i];
/*        client.index({
            index: defaultIndex,
            type: 'feature',
            id: obj.properties.GNIS_ID + "_" + obj.properties.PERMANENT_IDENTIFIER,
            body: obj
        }, function (error, response) {
            logResponse(error, response);
            if(--count <= 0) {
                stream.resume();
            }
        });
*/
        var buf = JSON.stringify(obj);

        var fn = path.join(dataPath, obj.properties.GNIS_NAME + "_" + obj.properties.REACHCODE + ".geojson");
        if( fs.existsSync(fn)) {
          console.log("File exists: " + fn);
        }
        var fd = fs.openSync(fn, 'w');
        fs.writeSync(fd, buf);
        fs.closeSync(fd);
        if(--count <= 0) {
          stream.resume();
        }
    };
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
        else if( response === true) {
            successFunc();
        }
        else {
            client.indices.create({
                index: value,
                body: {
                    mappings: {
                        feature: {
                            properties: {
                                name: {
                                    type: 'string',
                                    index: 'not_analyzed'
                                },
                                geometry : {
                                    type: 'geo_shape',
                                    precision: '3m'
                                },
                                properties: {
                                    properties: {
                                      "FTYPE": {
                                        "type": "long"
                                      },
                                      "NHD_FLOW": {
                                        "type": "string"
                                      },
                                      "SHAPE_Length": {
                                        "type": "double"
                                      },
                                      "FTR_SOURCE_DT": {
                                        "format": "epoch_millis||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                                        "type": "date"
                                      },
                                      "JURIS_NAME": {
                                        "type": "string"
                                      },
                                      "GNIS_NAME": {
                                        "copy_to": [
                                          "name"
                                        ],
                                        "type": "string"
                                      },
                                      "FCODE": {
                                        "type": "long"
                                      },
                                      "FTR_SOURCE": {
                                        "type": "string"
                                      },
                                      "PFC": {
                                        "type": "string"
                                      },
                                      "PFC_AREA_NO": {
                                        "type": "string"
                                      },
                                      "PLANFLOW": {
                                        "type": "string"
                                      },
                                      "PROPERTY_STATUS": {
                                        "type": "string"
                                      },
                                      "PUB_DATE": {
                                        "format": "epoch_millis||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                                        "type": "date"
                                      },
                                      "NHD_FLOW_METADATA": {
                                        "type": "string"
                                      },
                                      "STREAMORDER": {
                                        "type": "string"
                                      },
                                      "LOCAL_NAME": {
                                        "type": "string"
                                      },
                                      "FTR_ORGANIZATION": {
                                        "type": "string"
                                      },
                                      "PFC_DATE": {
                                        "format": "epoch_millis||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                                        "type": "date"
                                      },
                                      "DIST_RCH_DT": {
                                        "format": "epoch_millis||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                                        "type": "date"
                                      },
                                      "FISHBEARING": {
                                        "type": "string"
                                      },
                                      "ELEVATION": {
                                        "type": "double"
                                      },
                                      "GNIS_ID": {
                                        "type": "string"
                                      },
                                      "SUBBASIN": {
                                        "type": "string"
                                      },
                                      "FTR_INTERPRETATION": {
                                        "type": "string"
                                      },
                                      "SHAPE_Area": {
                                        "type": "long"
                                      },
                                      "WBAREA_PERMANENT_IDENTIFIER": {
                                        "type": "string"
                                      },
                                      "DIST_RCH_NO": {
                                        "type": "string"
                                      },
                                      "TMEASURE": {
                                        "type": "double"
                                      },
                                      "CONTINUITY": {
                                        "type": "string"
                                      },
                                      "FMEASURE": {
                                        "type": "double"
                                      },
                                      "PFC_RCH_NO": {
                                        "type": "string"
                                      },
                                      "FLD_VER_DT": {
                                        "format": "epoch_millis||yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                                        "type": "date"
                                      },
                                      "FTR_SOURCESCALE": {
                                        "type": "long"
                                      },
                                      "PERMANENT_IDENTIFIER": {
                                        "index": "not_analyzed",
                                        "type": "string"
                                      },
                                      "REACHCODE": {
                                        "type": "string"
                                      }                                    }
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
                if( successFunc )
                    successFunc();
            });
        }
    });
}



function logResponse(error, response) {
    if(error) {
        console.log("Bogus: ", error);
    } else {
        // console.log("Success: ", response);
    }
}
