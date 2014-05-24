<?php
/**
 * Prototype test script to emulate a report on a root entity with a predicate on a distant related entity.
 *
 * Here we are looking for any location where Nation = <something>, but not using the Nation property of Locations!
 * Instead we use the relationship of Location.LookupCountyID to LookupCountys, then LookupNationID to LookupNations,
 * then filter on LookupNations.Nation value
 *
 * Additional criteria is the county name must start with a-f
 *
db.Locations.group({
key: { ord_dt: 1 },
cond: { ord_dt: { $gt: new Date( '01/01/2012' ) } },
reduce: function( curr, result ) {
result.total += curr.item.qty;
},
initial: { total : 0 }
});
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
$countyRequest = new StorageRequest('LookupCountys');
$nationRequest = new StorageRequest('LookupNations');

// Limit nation search to 5 of each nation
$fewNations = array(1, 2, 3, 4, 5, 12466, 12531, 13083, 13099, 13141, 11938, 12439, 12447, 12448, 12449, 12436, 12437, 12441, 13216, 13345);
//$rootRequest->addFilter(array('LocationID' => array('$in' => $fewNations)));

// Add what we are looking for
$nationRequest->addFilter(array('Nation' => $targetNation));
$countyRequest->addFilter(array('Description' => new MongoRegex('/^[a-f].*$/i'))); // Only counties that start with a-f

// Configure nodes
$rootNode = new FetchNode('Locations', null, $rootRequest, $storageManager, $batchSize, false);
$countyNode = new FetchNode('LookupCountys', null, $countyRequest, $storageManager, $batchSize, false);
$rootNode->addChild($countyNode);

$nationNode = new FetchNode('LookupNations', null, $nationRequest, $storageManager, $batchSize, false);
$countyNode->addChild($nationNode);

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