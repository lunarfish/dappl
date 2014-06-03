<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 19:37
 */


// Setup the managers
$storageManager = new StorageManager(array());
$metadataManager = new MetadataManager(array(
	'metadata_container_name' => 'mongo.metadata',
	'metadata_default_resource_name' => 'Entities'
), $storageManager);
$resultProcessor = new FetchNodeResultProjectionProcessor();

// Setup filter parsing
$scanner = new RequestFilterTokenizer();
$parser = new RequestFilterPredicateParser();


$startTime = microtime(true);
$batchSize = 10000;


$filter = <<< 'HEREDOC'
((LocationID gt 13000) and (LookupCountys%2FLookupNations%2FNation eq 'Wales')
HEREDOC;

$defaultResourceName = 'Locations';

$graph = new FetchNodeGraph($metadataManager, $storageManager, $resultProcessor, $batchSize);
$predicates = $graph->extractPredicates($scanner, $parser, $filter);
$rootNode = $graph->buildGraph($defaultResourceName, $predicates, array('LocationID', 'Address1', 'PostCode', 'LookupCountyID', 'LookupCountys/Description'));

$request = $rootNode->getBaseRequest();
//$request->setSelect(array('LocationID', 'Address1', 'PostCode', 'LookupCountyID'));

$total = 0;
$result = null;
$input = new FetchResultCollection();
$rootNode->prepare($input);
do {
	$result = $rootNode->fetch();
	if (is_object($result)) {
		echo 'We have a result set: ' . count($result) . PHP_EOL;
		foreach($result as $row) {
			echo json_encode($row) . PHP_EOL;
		}
//var_dump($result[]);
		$total += count($result);
	}
} while(false !== $result);



// Profile
$endTime = microtime(true);
echo 'Batch size: ' . $batchSize;
echo ' Target: ' . $defaultResourceName;
echo ' Time: ' . round($endTime - $startTime, 2) . " Sec.";
echo ' Total found: ' . $total;
echo " Memory: ".(memory_get_peak_usage(true)/1024/1024)." MB";
echo PHP_EOL;
