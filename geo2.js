var fs = require('fs')
    , elasticsearch = require('elasticsearch')
    // , request = require('request')
    , JSONStream = require('JSONStream')
    , es = require('event-stream')
    , TransformToBulk = require('elasticsearch-streams').TransformToBulk
    , WritableBulk = require('elasticsearch-streams').WritableBulk;

var filePath = "/home/ubuntu/workspace/data/methow.geojson";
var args = process.argv;

if(args.length > 2) {
    filePath = args[2]; // File to process. 
}

var connectionString = "http://localhost:9200";
var defaultIndex = "hydro_test";

var client = new elasticsearch.Client({
    host: connectionString,
    log: 'error',
    sniffOnStart: true,
    sniffOnConnectionFault: true
});

var bulkExec = function(bulkCmds, callback) {
    // console.log("Bulk emit");
    client.bulk({
        index : defaultIndex,
        type  : 'feature',
        body  : bulkCmds
    }, callback);
};
var ws = new WritableBulk(bulkExec);
var toBulk = new TransformToBulk(transform);

createIndex(defaultIndex, undefined, processFile);

function processFile() { 

    console.log("Processing file: ", filePath);

    fs.createReadStream(filePath, {flags: 'r', encoding: 'utf-8'})
        .pipe(JSONStream.parse('features.*'))
        .pipe(toBulk).pipe(ws).on('close', done);    
}

function done() {
    client.close();
    // console.log('Closed connection');
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

    return obj;
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
                            name: {
                                type: 'string',
                                index: 'not_analyzed'
                            },
                            geometry : {
                                properties: {
                                    coordinates: {
                                        type: 'geo_point',
                                        geohash: 'true',
                                        lat_lon: 'true',
                                        fielddata: {
                                            format: 'compressed',
                                            precision: '3m'
                                        }
                                    }
                                }
                            },
                            properties: {
                                properties: { 
                                    GNIS_NAME: {
                                        type: 'string',
                                        copy_to: 'name'

                                    },
                                    PERMANENT_IDENTIFIER: {
                                        type: 'string',
                                        index: 'not_analyzed'

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

