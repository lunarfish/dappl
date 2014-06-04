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
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;

class Report
{
	private $defaultResourceList;
	private $connectionConfig;
	private $request;


	/**
	 * @param $isDebug
	 * @param array $dataServiceConfig defaultResourceName => connection key identifier
	 * @param array|\Dappl\Controller\connection $dataStoreConfig connection key identifiers => connection details
	 * @param \Symfony\Component\HttpFoundation\Request $request
	 */
	public function __construct($isDebug, array $dataServiceConfig, array $dataStoreConfig, Request $request)
	{
		$this->defaultResourceList = $dataServiceConfig;
		$this->connectionConfig = $dataStoreConfig;
		$this->request = $request;
	}


	public function report($defaultResourceName)
	{
		// Setup the managers
		$storageManager = new StorageManager(array());
		$metadataManager = new MetadataManager(array(
			'metadata_container_name' => 'mongo.metadata',
			'metadata_default_resource_name' => 'Entities'
		), $storageManager);
		$resultProcessor = new ResultProjectionProcessor();

		// Setup filter parsing
		$filter = $this->request->query->get('$filter');
		$scanner = new FilterTokenizer();
		$parser = new PredicateParser();

		// Build graph
		$batchSize = 500;
		$graph = new Graph($metadataManager, $storageManager, $resultProcessor, $batchSize);
		$predicates = $graph->extractPredicates($scanner, $parser, $filter);
		$rootNode = $graph->buildGraph($defaultResourceName, $predicates, array('LocationID', 'Address1', 'PostCode', 'LookupCountyID', 'LookupCountys/Description'));

		// Run graph
		$output = array();
		$input = new ResultCollection();
		$rootNode->prepare($input);
		do {
			$result = $rootNode->fetch();
			if (is_object($result)) {
				$result->appendItems($output);
			}
		} while(false !== $result);

		return new JsonResponse($output);
	}
} 