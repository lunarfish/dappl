<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 14:34
 */

namespace Dappl\Storage\Graph;

use \Dappl\Metadata\Manager as MetadataManager;
use \Dappl\Storage\Manager as StorageManager;
use \Dappl\Fetch\Request;
use \Dappl\Metadata\NavigationProperty;


class Node
{
    private $metadataManager;
    private $baseRequest;
    private $batchSize;
    private $resultProcessor;
    private $children;

    private $parent;
    private $navigationProperty;

    private $storageManager;
    private $batchFetchCursor;

    // New format data
    private $inputData;
    private $childNodeResults;
    private $fetchResultCollection;
    private $isDebugging;


    // State enum
    const UNREADY = 0;
    const READY = 1;
    const COMPLETE = 2;


    public function __construct(MetadataManager $metadataManager, Request $baseRequest, StorageManager $storageManager, $batchSize, ResultProcessorInterface $resultProcessor, $isDebugging)
    {
        $this->metadataManager = $metadataManager;
        $this->baseRequest = $baseRequest;
        $this->storageManager = $storageManager;
        $this->batchSize = $batchSize;
        $this->resultProcessor = $resultProcessor;
        $this->children = array();
        $this->parent = null;

        $this->batchFetchCursor = null;
        $this->keepList = array();
        $this->childNodeResults = array();
        $this->isDebugging = $isDebugging;
    }


    public function addChild(Node $node, $navigationPropertyName)
    {
        // Add new node to child list
        $this->children[] = $node;

        // Add self as the nodes parent
        $node->parent = $this;

        // Set the navigation property of this node the new node is mapped to.
        // First ensure navigation property exists
        $nodeMetadata = $this->baseRequest->getMetadata();
        $navigationProperty = $this->metadataManager->getNavigationProperty($nodeMetadata->getEntityName(), $navigationPropertyName);
        $node->setNavigationProperty($navigationProperty);

        // Add the key the child will lock onto as an index of our results collection
        $fetchResultCollection = $this->getFetchResultCollection();
        $fetchResultCollection->addIndex($navigationProperty->getEntityKey());
    }


    public function setNavigationProperty(NavigationProperty $navigationProperty)
    {
        $this->navigationProperty = $navigationProperty;
    }


    public function getNavigationProperty()
    {
        return $this->navigationProperty;
    }


    public function getName()
    {
        return $this->baseRequest->getDefaultResourceName();
    }


    public function isRoot()
    {
        return $this->parent ? false : true;
    }


    public function getSelectFields(array &$fields, $namespace = '')
    {
        // Concatenate input namespace with the localnamespace (navigation property name)
        $localNamespace = $this->navigationProperty ? $this->navigationProperty->getName() : '';
        $namespace = empty($namespace) ? '' : ($namespace . '.');
        $prefix = $namespace . $localNamespace;
        $addPrefix = empty($prefix) ? '' : ($prefix . '.');

        // Add $select fields for this node
        $nodeFields = $this->baseRequest->getSelect();
        foreach($nodeFields as $nodeField) {
            $fields[] = $addPrefix . $nodeField;
        }

        // Handle child nodes
        foreach($this->children as $childNode) {
            $childNode->getSelectFields($fields, $prefix);
        }
    }


    /**
     * Returns the entity metadata for this node
     */
    public function getMetadata()
    {
        return $this->baseRequest->getMetadata();
    }


    /**
     * Returns the base request for this node. This is the base request that is used as a prototype to build the actual request
     * that is performed for this node, which may include extra filters to match parent results.
     * @return StorageRequest
     */
    public function getBaseRequest()
    {
        return $this->baseRequest;
    }


    /**
     * Clears any current cursor. This resets the state of this node to UNREADY
     * Also purges any child nodes belonging to this node.
     */
    public function purge()
    {
        $this->log('Purging cursor and child nodes');

        // Purge me
        $this->batchFetchCursor = null;

        // Purge my children
        $this->purgeChildNodes();
    }


    /**
     * Purges just the child nodes
     */
    public function purgeChildNodes()
    {
        $this->childNodeResults = array();
        foreach($this->children as $childNode) {
            $childNode->purge();
        }
    }


    /**
     * Returns class constants describing current state of this node
     * UNREADY - we either been purged or not had prepare() called
     * READY - prepared and able to return results
     * COMPLETE - returned everything we can for the criteria set in prepare()
     */
    public function getState($checkChildren = false)
    {
        if (!$this->batchFetchCursor) {
            return self::UNREADY;
        }

        if (count($this->children) && $checkChildren && !$this->batchFetchCursor->hasMore()) {
            // We only consider ourselves complete when all our child nodes are also complete
            $state = self::COMPLETE;
            foreach($this->children as $childNode) {
                if (self::COMPLETE != $childNode->getState()) {
                    $state = self::READY;
                    break;
                }
            }
            return $state;
        } else {
            // No child nodes, so it's just our state to return
            return $this->batchFetchCursor->hasMore() ? self::READY : self::COMPLETE;
        }
    }


    /**
     * Lazy loads the fetch result collection for this node.
     * @param bool $purge - if true removes all results from the collection.
     * @return ResultCollection
     */
    public function getFetchResultCollection($purge = false)
    {
        if (!$this->fetchResultCollection) {
            // Create a new fetch result collection
            $this->fetchResultCollection = new ResultCollection($this);

            // Set primary key from our base request metadata
            $pk = $this->baseRequest->getMetadata()->getKey();
            if ($pk) {
                $this->fetchResultCollection->setPrimaryKeyName($pk);
            }

            // Set indexes from the relationship property
            if ($this->navigationProperty) {
                $childKey = $this->navigationProperty->getEntityKey();
                $this->fetchResultCollection->addIndex($childKey);
            }
        }
        if ($purge) {
            //$this->log('getFetchResultCollection purge');
            $this->fetchResultCollection->purge();
        }
        return $this->fetchResultCollection;
    }


    public function prepare(ResultCollection $inputResultCollection)
    {
        $this->log('*prepare method*');

        // We should not be called if we already have a cursor that is in flight
        if (self::UNREADY !== $this->getState()) {
            throw new \Exception($this->getName() . ' node error - prepare called with existing active batch cursor');
        }

        // Create a new batch request, from the original details provided when the node was created
        $request = new Request($this->baseRequest->getDefaultResourceName(), $this->baseRequest->getMetadata());
        $request->setFilter($this->baseRequest->getFilter());

        // Add any extra filter from the source data - eg extract any relationship keys to filter by
        $this->addFilterFromSourceData($request, $inputResultCollection);

        // Create the cursor
        $this->batchFetchCursor = $this->storageManager->prepareBatchFetch($request, $this->batchSize);

        // Clear our result collection
        $fetchResultCollection = $this->getFetchResultCollection(true);

        // Store the input data for aggregating results
        $this->inputData = $inputResultCollection;

        if (count($this->children)) {
            // This is not a leaf node, so we must fetch our results now so it's ready when child node results come in and we need to compile
            $this->batchFetchCursor->getNextBatch($fetchResultCollection);

            $this->childNodeResults = array();
        } else {
            // We are a leaf node so don't fetch here - we will do that in fetch()
        }
    }


    /**
     * Fetches and collates results for the input given to this node in prepare()
     * This method should be called multiple times until all results are returned
     *
     * Returns:
     * - array of results: please call us again via fetch() to potentially get more
     * - false: this batch is complete and we are ready for more input via prepare()
     * - true: no results this time but the batch is not yet complete - please call us again via fetch() to get the rest
     *
     * @return false | array
     * @throws Exception
     */
    public function fetch()
    {
        $this->log('*fetch method*');

        // Sanity check. We must always have a cursor defined
        if (!$this->batchFetchCursor) {
            throw new \Exception($this->getName() . ' node error - fetch called us when we had no cursor defined');
        }

        // Fetch process depends on if we are a leaf or parent node
        if (count($this->children)) {
            // Get results from child nodes. If we previously returned result continue from the end
            $startIndex = count($this->childNodeResults) ? count($this->childNodeResults) - 1 : 0;
            $result = $this->fetchFromChildNode($startIndex);

            if (true === $result) {
                // If child nodes returned true they have more results to fetch. We cannot call back directly as we could lead to too much recursion
                // Return true so we get called again by client
                return $result;
            }

            if ($result) {
                // Child nodes returned a result, return it.
                return $this->resultProcessor->combineNodeAndChildNodeResults($result, $this->getFetchResultCollection());
            }

            // Child nodes returned nothing. Maybe this node has more results to fetch from it's cursor?
            if (self::COMPLETE == $this->getState()) {
                // Nope, we are done.
                $this->log('Node completed base request. Purging ready to receive a new request');
                $this->purge();
                return false;
            }

            // We have more, go get it.
            $fetchResultCollection = $this->getFetchResultCollection(true);
            $this->batchFetchCursor->getNextBatch($fetchResultCollection);
            $this->log('Fetch parent - fetched data rows: ' . count($fetchResultCollection) . ' purging child nodes to receive new requests.');

            // Reset child results.
            $this->purgeChildNodes();

            // Return true so we can get called again by the client
            return true;
        } else {
			// If we are a remote leaf node, return leaf fetch. If we are a leaf root node, process results to ensure projection is processed.
			$leafResults = $this->fetchFromLeafNode();
            if (true === $leafResults) {
                return true;
            }
            return $this->isRoot() && is_object($leafResults) ? $this->resultProcessor->combineNodeAndChildNodeResults(array(), $leafResults) : $leafResults;
        }
    }


    /**
     * When prepare is called we are passed a result collection from the previous node up the chain. This node should only fetch results
     * related to these input results, using the navigation property details to specify the key.
     *
     * @param StorageRequest $request - The current request to ammend
     * @param ResultCollection $inputResultCollection - The result set to extract filter from
     */
    private function addFilterFromSourceData(Request $request, ResultCollection $inputResultCollection)
    {
        if ($this->navigationProperty) {
            $nodeKey = $this->navigationProperty->getRelatedEntityKey();
            $sourceKey = $this->navigationProperty->getEntityKey();
            $request->addFilter(array($nodeKey => array('$in' => $inputResultCollection->getUniqueValues($sourceKey))));
        }
    }


    /**
     * Fetches and collates results for the input given to this node in prepare()
     * This is a recursive method as all child nodes of this node must return results before we can collate and return ourself
     * Note this is AND predicate behavior. If we just returned child node results directly we would get OR predicate behavior.
     * - something for later development!
     *
     * Returns:
     * - array of results - please call us again via fetch() to potentially get more
     * - false if this batch is complete and we need more input via prepare()
     *
     * @param $childIndex
     * @return false | array
     */
    private function fetchFromChildNode($childIndex)
    {
        // We have completed all child nodes if child index is beyond bounds - we have a full stack of results
        if ($childIndex >= count($this->children)) {
            return $this->childNodeResults;
        }

        // We finish if child index is less than zero - we backed up to the end
        if ($childIndex < 0) {
            return false;
        }

        // Not complete - get the child node for this index
        $childNode = $this->children[$childIndex];

        // Is child node complete?
        // - the getState(true) to check child nodes also is a bit hacky - need to refactor so this isn't required
        $state = $childNode->getState(true);
        if (self::COMPLETE === $state) {
            // This node is complete
            $this->log('Child node: ' . $childNode->getName() . ' completed. Purging child node and removing results');
            $childNode->purge();

            // Remove results for this node and all above
            $removeIndex = count($this->childNodeResults) - 1;
            while ($removeIndex >= $childIndex) {
                unset($this->childNodeResults[$removeIndex]);
                $removeIndex--;
            }

            // we need to walk back towards the beginning of the stack now - earlier children may have results that will produce results from this one
            return $this->fetchFromChildNode($childIndex - 1);
        }

        // Is child node ready
        if (self::UNREADY === $state) {
            // No - we need to give it a result set for it to prepare itself from
            $childPrepareResultCollection = null;
            $nodeFetchResultCollection = $this->getFetchResultCollection();
            if (!$childIndex) {
                // For the first child, it prepares from the results for this node
                $childPrepareResultCollection = $nodeFetchResultCollection;
            } else {
                // We have more than one child, this child should prepare from this node's results filtered by the previous childs results.
                // It's this part that defines AND/OR logic between nodes. We are hard coding AND behavior here for now.
                // (OR behavior would be to not filter the previous childs results)
                $childPrepareResultCollection = $this->newFetchResultCollectionFilteredByChildResults($nodeFetchResultCollection, $this->childNodeResults[$childIndex - 1]);
            }

            $childNode->prepare($childPrepareResultCollection);
        }

        // Child node is ready, fetch some data
        $childResult = $childNode->fetch();
        if ($childResult) {
            // We have data - store it in the results array provided and call ourself to dive into the next level
            $this->childNodeResults[$childIndex] = $childResult;
            return $this->fetchFromChildNode($childIndex + 1);
        }

        // This child produced no results - return false to indicate this batch is completed
        return false;
    }


    /**
     * For leaf nodes to fetch and return data in the nodes fetch result collection
     *
     * Returns:
     * - array of results: please call us again via fetch() to potentially get more
     * - false: this batch is complete and we are ready for more input via prepare()
     * - true: no results this time but the batch is not yet complete - please call us again via fetch() to get the rest
     *
     * @return bool|FetchResultCollection
     */
    private function fetchFromLeafNode()
    {
        // Is this leaf node done?
        if (self::COMPLETE == $this->getState()) {
            // No, we are done. Return false to indicate we are done with what we were given in the last prepare().
            // If you want us to do anything further, send us more input via prepare()
            $this->log('Leaf node complete');
            return false;
        }

        // We are not complete, so we still have work to do with this batch cursor - go get some data.
        $fetchResultCollection = $this->getFetchResultCollection(true);
        $this->batchFetchCursor->getNextBatch($fetchResultCollection);
        $this->log('Leaf fetch - fetched data rows: ' . count($fetchResultCollection));

        // This node is a leaf node (no kids) so it can return results. Do we have any?
        if (count($fetchResultCollection)) {
            return $fetchResultCollection;
        } else {
            // Nothing this time, but we may have more. Please call again via fetch()
            return true;
        }
    }


    /**
     * Creates a new result collection based on an input collection filtered by a result collection of a related entity.
     * The results should only have entities where the relationship exists, and therefore filters out any entities in the original
     * base that do not have related entities.
     * @param $baseCollection
     * @param $filterCollection
     * @return ResultCollection
     */
    public function newFetchResultCollectionFilteredByChildResults(ResultCollection $baseCollection, ResultCollection $filterCollection)
    {
        // Create a new result set to return.
        $returnResultCollection = new ResultCollection($this);

        // Define keys to match original node result collection
        $returnResultCollection->setPrimaryKeyName($baseCollection->getPrimaryKeyName());
        $returnResultCollection->addIndexNames($baseCollection->getIndexNames());

        // Get the navigation property between the filter collection and the base collection
        $navigationProperty = $filterCollection->getNode()->getNavigationProperty();
        $nodeKey = $navigationProperty->getRelatedEntityKey();
        $childKey = $navigationProperty->getEntityKey();

        // Iterate filter collection
        foreach($filterCollection as $filterEntity) {
            // Locate the related base entities for this child entity
            $baseEntities = $baseCollection->getEntitiesByIndex($nodeKey, $filterCollection->getIndexValue($filterEntity, $childKey));

            // Add them to the result collection. Dupes will be ignored.
            $returnResultCollection->addEntities($baseEntities);
        }

        return $returnResultCollection;
    }


    /**
     * Log method for cli development/debugging
     * @param $msg
     */
    public function log($msg)
    {
        if ($this->isDebugging) {
            echo sprintf('[Node: %s] - %s%s', $this->getName(), $msg, PHP_EOL);
        }
    }
} 