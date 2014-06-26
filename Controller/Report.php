<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 15:53
 */

namespace Dappl\Controller;

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


	/**
	 * Runs a multi-node report on a breeze fetch request.
	 * Uses $filter and $select from _GET
	 *
	 * 		<p>Previous examples:</p>
	 *		<p>https://mbp.tim.tinderfoundation.org/api/reports/Locations?$filter=((((((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (substringof('a single quote ''yes''',Address1) eq true)) and (startswith(Address2,'double%22 single'' quotes') eq true)) and (Address3 ne 'double%22 only')) and (Address4 eq 'single'' only')) and (Town eq 'percent %2522 twentytwo')&$select=Town,Address2,LookupCountys%2FDescription</p>
	 * 		<p>https://mbp.tim.tinderfoundation.org/api/reports/Locations?$filter=((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (LookupCountys%2FLookupNations%2FNation eq 'Wales')&$select=Town,Address2,LookupCountys%2FDescription</p>
	 *
	 * @param $defaultResourceName Root collection to fetch from
	 * @return JsonResponse
	 */
	public function report($defaultResourceName)
	{
        // Setup the managers
		$storageManager = new StorageManager(array('driver_params' => $this->connectionConfig));
		$metadataManager = new MetadataManager(array(
			'metadata_container_name' => 'mongo.metadata',
			'metadata_default_resource_name' => 'Entities',
            'container_names' => $this->defaultResourceList
		), $storageManager);
		$resultProcessor = new ResultProjectionProcessor();

		// Setup filter parsing
		$filter = $this->request->query->get('$filter');
		$scanner = new FilterTokenizer();
		$parser = new PredicateParser();

		// Parse select
		$select = $this->request->query->get('$select');
		$select = explode(',', $select);

		// Build graph
		$batchSize = 500;
		$graph = new Graph($metadataManager, $storageManager, $resultProcessor, $batchSize, false);
		$predicates = $graph->extractPredicates($scanner, $parser, $filter);
		$rootNode = $graph->buildGraph($defaultResourceName, $predicates, $select);

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