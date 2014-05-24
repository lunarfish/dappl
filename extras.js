/**
 * Created by rick on 22/05/2014.
 */

// Create some extras data. ~50 values per location id, ranging from 1-50
var extraCountMin = 2,
    extraCountRange = 2,
    extraValMin = 1,
    extraValRange = 49;

var conn = new Mongo();
var db = conn.getDB("ukonline");

var locations = db.Locations.find();//.limit(2);
var doc, extra, count, extraId = 1;

// Remove any current extras
db.Extras.remove();

while (locations.hasNext()) {
    // Get the location object
    doc = locations.next();
    //printjson(doc);

    // How extras for this one?
    count = Math.floor((Math.random() * extraCountRange) + extraCountMin);

    while(count > 0) {
        extra = {
            ExtraID: extraId,
            LocationID: doc.LocationID,
            ExtraValue: Math.floor((Math.random() * extraValRange) + extraValMin)
        };

        db.Extras.save(extra);
        //printjson(extra);

        extraId++;
        count--;
    }

}