<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 02/06/2014
 * Time: 14:30
 */






require_once('StorageRequest.php');
require_once('FetchCursor.php');
require_once('StorageManager.php');
require_once('EntityMetadata.php');
require_once('MetadataManager.php');
require_once('FetchNode.php');
require_once('FetchNodeResultProcessor.php');
require_once('RequestFilterParser.php');


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


/*
// Graph requests
$rootRequest = new StorageRequest('Locations', $metadataManager->metadataForEntity('Location'));
$countyRequest = new StorageRequest('LookupCountys', $metadataManager->metadataForEntity('LookupCounty'));
$nationRequest = new StorageRequest('LookupNations', $metadataManager->metadataForEntity('LookupNation'));
$extraRequest = new StorageRequest('Extras', $metadataManager->metadataForEntity('Extra'));

// Limit nation search to 5 of each nation
$fewNations = array(1, 2, 3, 4, 5, 11938, 12436, 12437, 12439, 12441, 12447, 12448, 12449, 12466, 12531, 13083, 13099, 13141, 13216, 13345);
$rootRequest->addFilter(array('LocationID' => array('$in' => $fewNations)));
$rootRequest->setSelect(array('LocationID', 'Address1', 'PostCode', 'LookupCountyID'));

// Add what we are looking for
$nationRequest->addFilter(array('Nation' => $targetNation));
//$nationRequest->setSelect(array('LocationID', 'Address1', 'PostCode'));

$extraRequest->addFilter(array('ExtraValue' => array('$gt' => 25)));
$extraRequest->setSelect(array('ExtraValue'));

$countyRequest->addFilter(array('LookupNationID' => 3));
$countyRequest->setSelect(array('LookupCountyID', 'Description'));


// How do we want our results cooked? Either expanded or projection (flattened)
// FetchNodeResultProjectionProcessor | FetchNodeResultExpandProcessor
*/
$filter = <<< 'HEREDOC'
(LookupCountys%2FLookupNations%2FNation eq 'Wales')
HEREDOC;

$defaultResourceName = 'Locations';

$graph = new FetchNodeGraph($metadataManager, $storageManager, $resultProcessor, 100);
$predicates = $graph->extractPredicates($scanner, $parser, $filter);
$rootNode = $graph->buildGraph($defaultResourceName, $predicates);


// Builds a graph of fetch nodes from a filter
class FetchNodeGraph
{
    private $metadataManager;
    private $storageManager;
    private $resultProcessor;
    private $batchSize;

    private $rootNode;


    public function __construct(MetadataManager $metadataManager, StorageManager $storageManager, FetchNodeResultProcessorInterface $resultProcessor, $batchSize)
    {
        $this->metadataManager = $metadataManager;
        $this->storageManager = $storageManager;
        $this->resultProcessor = $resultProcessor;
        $this->batchSize = $batchSize;
    }


    public function extractPredicates(RequestFilterTokenizer $scanner, RequestFilterPredicateParser $parser, $input)
    {
        // Create tokens
        $tokens = $scanner->tokenize($input);

        // Create predicate list
        $predicates = $parser->getPredicates($tokens);

        return $predicates;
    }


    public function buildGraph($defaultResourceName, array $predicates)
    {
        // Construct root node
        $rootRequest = new StorageRequest($defaultResourceName, $this->metadataManager->metadataForDefaultResourceName($defaultResourceName));
        $this->rootNode = $this->createFetchNode($rootRequest);

        // Iterate predicates

        // Expand property path

        // Get node for path

        // Add predicate


    }



    public function getFetchNode(array $path, $createIfMissing = true)
    {

    }


    private function createFetchNode(StorageRequest $request)
    {
        return new FetchNode($this->metadataManager, $request, $this->storageManager, $this->batchSize, $this->resultProcessor);
    }
} 