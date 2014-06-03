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



// Builds a graph of fetch nodes from a filter
class FetchNodeGraph
{
    private $metadataManager;
    private $storageManager;
    private $resultProcessor;
    private $batchSize;

    private $rootNode;
    private $nodeCache;


    public function __construct(MetadataManager $metadataManager, StorageManager $storageManager, FetchNodeResultProcessorInterface $resultProcessor, $batchSize)
    {
        $this->metadataManager = $metadataManager;
        $this->storageManager = $storageManager;
        $this->resultProcessor = $resultProcessor;
        $this->batchSize = $batchSize;
        $this->nodeCache = array();
    }


    /**
     * Returns array of RequestFilterPredicate instances for the given Breeze/OData input string
     * @param RequestFilterTokenizer $scanner
     * @param RequestFilterPredicateParser $parser
     * @param $input
     * @return array
     */
    public function extractPredicates(RequestFilterTokenizer $scanner, RequestFilterPredicateParser $parser, $input)
    {
        // Create tokens
        $tokens = $scanner->tokenize($input);

        // Create predicate list
        $predicates = $parser->getPredicates($tokens);

        return $predicates;
    }


    /**
     *
     * @param $defaultResourceName root collection to build graph from
     * @param array $predicates RequestFilterPredicate instances
     * @param array $select Property paths to include in result set
     * @return FetchNode
     */
    public function buildGraph($defaultResourceName, array $predicates, array $select)
    {
        // Construct root node
        $this->rootNode = $this->createNodeForPath($defaultResourceName, $defaultResourceName);

        // Iterate predicates
        foreach($predicates as $predicate) {
            // Split property path
            $path = $predicate->getProperty();
            $this->processNavigationPathNode($path, $this->rootNode, $defaultResourceName, function($node) use($predicate) {
                // Now we have the node for this predicate loaded. Get the request
                $request = $node->getBaseRequest();

                // Add predicate
                $request->addPredicate($predicate);
            });
        }

        // Iterate select
        foreach($select as $selectPath) {
            $this->processNavigationPathNode($selectPath, $this->rootNode, $defaultResourceName, function($node, $propertyName) {
                // Now we have the node for this predicate loaded. Get the request
                $request = $node->getBaseRequest();

                // Add predicate
                $request->addSelect($propertyName);
            });
        }

        return $this->rootNode;
    }


    /**
     * Walks all the steps in a property path (eg Locations/LookupCountys/LookupNations/Nation), creating FetchNode instances
     * as it goes if they are not already in the graph. When the node is found (in this case the node for LookupNations)
     * the $process callback is invoked with arguments ($node, $propertyName) where $propertyName is the last property in $path
     * (in this case Nations)
     * @param $path
     * @param $rootNode
     * @param $defaultResourceName
     * @param $process
     */
    private function processNavigationPathNode($path, FetchNode $rootNode, $defaultResourceName, $process)
    {
        // Split path into segments
        $segments = explode('/', $path);

        // Property is the last one
        $propertyIndex = count($segments) - 1;

        // Traverse navigation properties to get to the entity to receive this predicate
        $currentNode = $rootNode;
        $nodePath = $defaultResourceName;
        for($i = 0; $i < $propertyIndex; $i++) {
            // Do we have a node for this path
            $nodePath .= ('/' . $segments[$i]);
            $nextNode = $this->getNodeForPath($nodePath);
            if (!$nextNode) {
                // We don't have one - create it
                // Load navigation property
                $metadata = $currentNode->getMetadata();
                $navigationProperty = $this->metadataManager->getNavigationProperty($metadata->getEntityName(), $segments[$i]);

                // Get target entity
                $targetEntity = $navigationProperty->getEntityTypeName(true);

                // Get target metadata, so we can obtain the default resource name
                $targetMetadata = $this->metadataManager->metadataForEntity($targetEntity);
                $targetDefaultResourceName = $targetMetadata->getDefaultResourceName();

                // Create the node
                $nextNode = $this->createNodeForPath($nodePath, $targetDefaultResourceName);

                // Add the node to its parent
                $currentNode->addChild($nextNode, $segments[$i]);
            }

            // Update current node
            $currentNode = $nextNode;
        }

        $process($currentNode, $segments[$propertyIndex]);
    }


    /**
     * Returns a FetchNode instance from the cache for given path, eg Locations/LookupCountys
     * or false
     * @param $path
     * @return false | FetchNode
     */
    public function getNodeForPath($path)
    {
        return array_key_exists($path, $this->nodeCache) ? $this->nodeCache[$path] : false;
    }


    /**
     * Creates a new FetchNode instance for $defaultResourceName collection and stores in the cache with $nodePath as the key.
     * @param $nodePath
     * @param $defaultResourceName
     * @return FetchNode
     * @throws Exception
     */
    private function createNodeForPath($nodePath, $defaultResourceName)
    {
        if (array_key_exists($nodePath, $this->nodeCache)) {
            throw new Exception(sprintf('Cannot create node at path: [%s] for resource: [%s] - already exists in cache', $nodePath, $defaultResourceName));
        }
        $request = new StorageRequest($defaultResourceName, $this->metadataManager->metadataForDefaultResourceName($defaultResourceName));
        $node = new FetchNode($this->metadataManager, $request, $this->storageManager, $this->batchSize, $this->resultProcessor);
        $this->nodeCache[$nodePath] = $node;
        return $node;
    }
} 