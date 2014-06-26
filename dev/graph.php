<?php

/**
 * Basic cli interface
 *
 *
 *
 * php -f /var/www/crm/src/Dappl/src/Dappl/dev/graph.php LookupCountys 10 "LookupCountyID gt 1" LookupCountyID
 * php -f /var/www/crm/src/Dappl/src/Dappl/dev/graph.php LookupCountys 10 "substringof('ham',Description) eq true" LookupCountyID,Description
 *
 *
 *
 * From live-app-crm /var/scripts/bin/reports/sql
 * php -f /var/www/crm/src/Dappl/src/Dappl/dev/graph.php People 500 "PersonCentreRoles/Centre/PartnerType eq 'Access Point'" PersonID,Email
 *
 * php -f /var/www/crm/src/Dappl/src/Dappl/dev/graph.php People 10 "((PersonCentreRoles/Centre/PartnerType eq 'Access Point') and (PersonID gt 200)) and (PersonID lt 220)" PersonID,Email
 *
 *
 *
 * above must satisfy this sql:
 * SELECT distinct email
FROM People, Centres, PeopleLinksCentresRoles
WHERE People.PersonID = PeopleLinksCentresRoles.PersonID
AND Centres.CentreID = PeopleLinksCentresRoles.CentreID
AND Centres.CentreActive = 1
AND Centres.PartnerType = 'Access Point'
AND People.NetsuiteID != 0
AND email != '';
 *
 *
 * Also a report to find FirstName contains 'John', LastName contains 'Smith' fails
 *
 */

use \Dappl\Storage\Manager as StorageManager;
use \Dappl\Metadata\Manager as MetadataManager;
use \Dappl\Storage\Graph\ResultProjectionProcessor;
use \Dappl\Fetch\FilterTokenizer;
use \Dappl\Fetch\PredicateParser;
use \Dappl\Storage\Graph\Graph;
use \Dappl\Storage\Graph\ResultCollection;


require_once __DIR__ . '/../../crm/vendor/autoload.php';



// Profiling
$startTime = microtime(true);
$batchSize = 10000;
$isDebugging = false;

// Validate input
if (isset($argc)) {
    if (($argc < 3) || ($argc > 5)) {
        echo 'Error: Invalid arguments. Usage: php -f ' . __FILE__ . ' <entitySet: req> <batchsize: req> <filter: optional> <select: optional>' . PHP_EOL;
        exit(1);
    }
}

// Parse input
$entitySet = $argv[1];
$batchSize = (int)$argv[2];
$filter = isset($argv[3]) ? $argv[3] : '';
$select = isset($argv[4]) ? $argv[4] : '';

// Process select
$select = explode(',', $select);


// Temp measure - load container names for each entity from yaml file - in future will be embedded within metadata
$containerNames = \Symfony\Component\Yaml\Yaml::parse(__DIR__ . '/../../crm/boot/config/crm/dataservice.yaml');

// Need to shuffle to create a src, vendor and config directory structure to put this in...
// default to local when apache envars is missing - important for cli where there are no apache envars...
$environment = isset($_SERVER['TINDER_CORE_ENVIROMENT']) ? $_SERVER['TINDER_CORE_ENVIROMENT'] : 'local';
$driverParams = \Symfony\Component\Yaml\Yaml::parse(__DIR__ . '/../../crm/boot/config/crm/datastore_' . $environment . '.yaml');

// Setup the managers
$storageManager = new StorageManager(array('debug' => $isDebugging, 'driver_params' => $driverParams));
$metadataManager = new MetadataManager(array(
	'metadata_container_name' => 'mongo.metadata',
	'metadata_default_resource_name' => 'Entities',
    'container_names' => $containerNames
), $storageManager);
$resultProcessor = new ResultProjectionProcessor();

// Setup filter parsing
$scanner = new FilterTokenizer();
$parser = new PredicateParser();

// Build graph
$graph = new Graph($metadataManager, $storageManager, $resultProcessor, $batchSize, $isDebugging);
$predicates = $graph->extractPredicates($scanner, $parser, $filter);
$rootNode = $graph->buildGraph($entitySet, $predicates, $select);


// Execute request
$total = 0;
$result = null;
$input = new ResultCollection();
$rootNode->prepare($input);
do {
	$result = $rootNode->fetch();
	if (is_object($result)) {
		//echo 'We have a result set: ' . count($result) . PHP_EOL;
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
echo ' Target: ' . $entitySet;
echo ' Time: ' . round($endTime - $startTime, 2) . " Sec.";
echo ' Total found: ' . $total;
echo " Memory: ".(memory_get_peak_usage(true)/1024/1024)." MB";
echo PHP_EOL;
