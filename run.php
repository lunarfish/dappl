<?php
/**
 * Prototype test script to emulate a report on a root entity with a predicate on a distant related entity.
 *
 * Here we are looking for any location where Nation = <something>, but not using the Nation property of Locations!
 * Instead we use the relationship of Location.LookupCountyID to LookupCountys, then LookupNationID to LookupNations,
 * then filter on LookupNations.Nation value
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
require_once('EntityMetadata.php');
require_once('MetadataManager.php');
require_once('FetchNode.php');

// Input
$targetNation = 'England';
$batchSize = 50000;

if (isset($argc)) {
    if ($argc != 3) {
        echo 'Error: Invalid arguments. Usage: php -f ' . __FILE__ . ' <target> <batchsize>' . PHP_EOL;
        exit(1);
    }
    $targetNation = $argv[1];
    $batchSize = (int)$argv[2];
}

// Profiling
// Tests so far show batch size of 100 to be sensible for this particular request. For some targets increasing to 300 makes requests 5-10% faster
// But consumes 10-20% more memory. Memory usage ranges from 5-8MB at the moment.
$startTime = microtime(true);

// Setup the managers
$storageManager = new StorageManager(array());
$metadataManager = new MetadataManager(array(
    'metadata_container_name' => 'mongo.metadata',
    'metadata_default_resource_name' => 'Entities'
), $storageManager);

// Graph requests
$rootRequest = new StorageRequest('Locations', $metadataManager->metadataForEntity('Location'));
$countyRequest = new StorageRequest('LookupCountys', $metadataManager->metadataForEntity('LookupCounty'));
$nationRequest = new StorageRequest('LookupNations', $metadataManager->metadataForEntity('LookupNation'));
$extraRequest = new StorageRequest('Extras', $metadataManager->metadataForEntity('Extra'));

// Limit nation search to 5 of each nation
$fewNations = array(1, 2, 3, 4, 5, 11938, 12436, 12437, 12439, 12441, 12447, 12448, 12449, 12466, 12531, 13083, 13099, 13141, 13216, 13345);
$rootRequest->addFilter(array('LocationID' => array('$in' => $fewNations)));

// Add what we are looking for
$nationRequest->addFilter(array('Nation' => $targetNation));

$extraRequest->addFilter(array('ExtraValue' => array('$gt' => 25)));

// Configure nodes
$rootNode = new FetchNode($metadataManager, $rootRequest, $storageManager, $batchSize, false);
$countyNode = new FetchNode($metadataManager, $countyRequest, $storageManager, $batchSize, false);
$rootNode->addChild($countyNode, 'LookupCountys');

$nationNode = new FetchNode($metadataManager, $nationRequest, $storageManager, $batchSize, false);
$countyNode->addChild($nationNode, 'LookupNations');

$extraNode = new FetchNode($metadataManager, $extraRequest, $storageManager, $batchSize, false);
//$rootNode->addChild($extraNode, 'Extras');


// Run the graph - original version with batch fetching (sub nodes could generate large arrays despite batching though)
/*$result = null;
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
*/


// Second version - results are output within child node batch cycles to ensure we never consume too much memory
$total = 0;
$result = null;
$input = array();
$rootNode->prepare($input);
do {
    $result = $rootNode->fetch();
    if (is_array($result)) {
        echo 'We have a result set: ' . count($result[0]) . PHP_EOL;
//var_dump($result[]);
        $total += count($result[0]);
    }
} while(false !== $result);



// Profile
$endTime = microtime(true);
echo 'Batch size: ' . $batchSize;
echo ' Target: ' . $targetNation;
echo ' Time: ' . round($endTime - $startTime, 2) . " Sec.";
echo ' Total found: ' . $total;
echo " Memory: ".(memory_get_peak_usage(true)/1024/1024)." MB";
echo PHP_EOL;