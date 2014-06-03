<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 15:53
 */

namespace Dappl\Controller;

//use Dappl\Fetch\Predicate;
use Dappl\Fetch\FilterTokenizer;
use Dappl\Fetch\PredicateParser;
use Dappl\Storage\Graph\ResultProjectionProcessor;
use Dappl\Metadata\Manager as MetadataManager;
use Dappl\Storage\Manager as StorageManager;
use Dappl\Storage\Graph\ResultCollection;
use Dappl\Storage\Graph\Graph;


class Report
{
	private $defaultResourceList;
	private $connectionConfig;


	/**
	 * @param $isDebug
	 * @param array $dataServiceConfig defaultResourceName => connection key identifier
	 * @param $dataStoreConfig connection key identifiers => connection details
	 */
	public function __construct($isDebug, array $dataServiceConfig, $dataStoreConfig)
	{
		$this->defaultResourceList = $dataServiceConfig;
		$this->connectionConfig = $dataStoreConfig;
	}


	public function report($defaultResourceName)
	{
		// Get filter tokenizer, parser, etc working...
		// Setup filter parsing

		// Setup the managers
		$storageManager = new StorageManager(array());
		$metadataManager = new MetadataManager(array(
			'metadata_container_name' => 'mongo.metadata',
			'metadata_default_resource_name' => 'Entities'
		), $storageManager);
		$resultProcessor = new ResultProjectionProcessor();

// Setup filter parsing
		$scanner = new FilterTokenizer();
		$parser = new PredicateParser();


		$startTime = microtime(true);
		$batchSize = 10000;


		$filter = <<< 'HEREDOC'
((LocationID gt 13000) and (LookupCountys%2FLookupNations%2FNation eq 'Wales')
HEREDOC;

		$defaultResourceName = 'Locations';

		$graph = new Graph($metadataManager, $storageManager, $resultProcessor, $batchSize);
		$predicates = $graph->extractPredicates($scanner, $parser, $filter);
		$rootNode = $graph->buildGraph($defaultResourceName, $predicates, array('LocationID', 'Address1', 'PostCode', 'LookupCountyID', 'LookupCountys/Description'));

		$request = $rootNode->getBaseRequest();
//$request->setSelect(array('LocationID', 'Address1', 'PostCode', 'LookupCountyID'));

		$total = 0;
		$result = null;
		$input = new ResultCollection();
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

	}
} 