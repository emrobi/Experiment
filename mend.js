var fs = require("fs");
var geomend = require("geojson-mend");
var _ = require("highland")

var data = fs.readFileSync("./data/Potholes.geojson");
var obj = JSON.parse(data);

obj.geometry = geomend.nDecimals(obj.geometry, 4);

// iterator over each point in the geometry and remove duplicate points
obj.geometry.coordinates[0] = _(obj.geometry.coordinates[0]).uniq
console.log(JSON.stringify(obj));