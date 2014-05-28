/*
* Map reduce version of run.php
*/
var conn = new Mongo();
var db = conn.getDB("ukonline");

var mapFunction = function() {
    var group = Math.floor(this.LookupCountyID / 1000);
    emit('G' + group, 1);
};
var reduceFunction = function(key, values) {
    return Array.sum(values);
};

print('Count:' + db.LookupCountys.find().count());
print('Key: G0 = England, G1 = Scotland, G2 = Wales, G3 = NI');

var mr = db.Locations.mapReduce(
    mapFunction,
    reduceFunction,
    {
        out: { inline: 1 }
    }
);
printjson(mr);
print('Completed');



