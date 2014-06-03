<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 02/06/2014
 * Time: 14:30
 */

namespace Dappl\Storage\Graph;


use \Dappl\Metadata\Manager as MetadataManager;
use \Dappl\Storage\Manager as StorageManager;
use \Dappl\Fetch\FilterTokenizer;
use \Dappl\Fetch\PredicateParser;
use \Dappl\Fetch\Request;


// Builds a graph of fetch nodes from a filter
class Graph
{
    private $metadataManager;
    private $storageManager;
    private $resultProcessor;
    private $batchSize;

    private $rootNode;
    private $nodeCache;


    public function __construct(MetadataManager $metadataManager, StorageManager $storageManager, ResultProcessorInterface $resultProcessor, $batchSize)
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
    public function extractPredicates(FilterTokenizer $scanner, PredicateParser $parser, $input)
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
    private function processNavigationPathNode($path, Node $rootNode, $defaultResourceName, $process)
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
            throw new \Exception(sprintf('Cannot create node at path: [%s] for resource: [%s] - already exists in cache', $nodePath, $defaultResourceName));
        }
        $request = new Request($defaultResourceName, $this->metadataManager->metadataForDefaultResourceName($defaultResourceName));
        $node = new Node($this->metadataManager, $request, $this->storageManager, $this->batchSize, $this->resultProcessor);
        $this->nodeCache[$nodePath] = $node;
        return $node;
    }
} 