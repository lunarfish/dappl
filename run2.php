<?php
/**
 * Prototype test script to emulate a report on a root entity with a predicate on a 1:many related entity
 *
db.CentresLinksLookupTargetAudiences.find({CentreID: {$lt: 100}, LookupTargetAudienceID: {$gt: 5}}).count();
var cursor = db.CentresLinksLookupTargetAudiences.find({CentreID: {$lt: 100}, LookupTargetAudienceID: {$gt: 5}}, {CentreID: true});
while(cursor.hasNext()) { doc = cursor.next(); print(doc.CentreID); };
 *
db.Locations.find({LocationID: {$lt: 100}}).count();
 *
 *
Seed CentresLinksLookupTargetAudiences with some extra data
db.CentresLinksLookupTargetAudiences.find().limit(1).pretty();
for(i = 0; i < 50; i++) { db.CentresLinksLookupTargetAudiences.save({"CentreID" : 50, "LookupTargetAudienceID" : i, "Ordinal" : 1, "Active" : 1}); }
 *
 *
 *
 */

require_once('StorageRequest.php');
require_once('FetchCursor.php');
require_once('StorageManager.php');
require_once('FetchNode.php');

// Input
if ($argc != 3) {
    echo 'Error: Invalid arguments. Usage: php -f ' . __FILE__ . ' <target> <batchsize>' . PHP_EOL;
    exit(1);
}
$targetNation = $argv[1];
$batchSize = (int)$argv[2];

// Profiling
// Tests so far show batch size of 100 to be sensible for this particular request. For some targets increasing to 300 makes requests 5-10% faster
// But consumes 10-20% more memory. Memory usage ranges from 5-8MB at the moment.
$startTime = microtime(true);

// Graph requests
$storageManager = new StorageManager();
$rootRequest = new StorageRequest('Locations');
$taRequest = new StorageRequest('CentresLinksLookupTargetAudiences');
$taRequest->addFilter(array('LookupTargetAudienceID' => array('$gt' => 5)));

// Limit nation search to first 100
//$fewNations = array(1, 2, 3, 4, 5, 12466, 12531, 13083, 13099, 13141, 11938, 12439, 12447, 12448, 12449, 12436, 12437, 12441, 13216, 13345);
$rootRequest->addFilter(array('LocationID' => array('$lt' => 100)));


// Configure nodes
$rootNode = new FetchNode('Locations', null, $rootRequest, $storageManager, $batchSize, false);
$taNode = new FetchNode('CentresLinksLookupTargetAudiences', null, $taRequest, $storageManager, $batchSize, false);
$rootNode->addChild($taNode);


// Run the graph
$result = null;
$total = 0;
do {
    $rootResults = array();
    $result = $rootNode->execute($rootResults);
    // do something with rootResults...
    if (count($rootResults)) {
        //echo 'Answer is:' . PHP_EOL;
        //var_dump($rootResults);
        foreach($rootResults as $row) {
            //echo $row['LocationID'] . PHP_EOL;
        }
        $total += count($rootResults);
    }
} while($result);

// Profile
$endTime = microtime(true);
echo 'Batch size: ' . $batchSize;
echo ' Target: ' . $targetNation;
echo ' Time: ' . round($endTime - $startTime, 2) . " Sec.";
echo ' Total found: ' . $total;
echo " Memory: ".(memory_get_peak_usage(true)/1024/1024)." MB";
echo PHP_EOL;