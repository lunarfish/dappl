<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 14:34
 */

class FetchNode
{
    private $metadataManager;
    private $baseRequest;
    private $batchSize;
    private $shouldExpand;
    private $children;

    private $parent;
    private $navigationProperty;

    private $storageManager;
    private $batchFetchCursor;
    private $keepList;

    // New format data
    private $inputData;
    private $nodeData;
    private $childNodeResults;

    // State enum
    const UNREADY = 0;
    const READY = 1;
    const COMPLETE = 2;


    public function __construct(MetadataManager $metadataManager, StorageRequest $baseRequest, StorageManager $storageManager, $batchSize, $shouldExpand)
    {
        $this->metadataManager = $metadataManager;
        $this->baseRequest = $baseRequest;
        $this->storageManager = $storageManager;
        $this->batchSize = $batchSize;
        $this->shouldExpand = $shouldExpand;
        $this->children = array();
        $this->parent = null;

        $this->batchFetchCursor = null;
        $this->keepList = array();
        $this->childNodeResults = array();
    }


    public function addChild(FetchNode $node, $navigationPropertyName)
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
    }


    public function setNavigationProperty(NavigationProperty $navigationProperty)
    {
        $this->navigationProperty = $navigationProperty;
    }


    public function getName()
    {
        return $this->baseRequest->getDefaultResourceName();
    }


    public function execute(array &$sourceBatchResult)
    {
        // Do we have a cursor to work with?
        if (!$this->batchFetchCursor) {
            // Create a new batch request, from the original details provided when the node was created
            $request = new StorageRequest($this->baseRequest->getDefaultResourceName(), $this->baseRequest->getMetadata());
            $request->setFilter($this->baseRequest->getFilter());

            // Add any extra filter from the source data - eg extract any relationship keys to filter by
            $this->addFilterFromSourceData($request, $sourceBatchResult);

            // Create the cursor
            $this->batchFetchCursor = $this->storageManager->prepareBatchFetch($request, $this->batchSize);
        }

        // Fetch batch of results
        $nodeBatchResult = $this->batchFetchCursor->getNextBatch();
//var_dump($nodeBatchResult);

        // Cascade to child nodes.
        foreach($this->children as $child) {
            // Iterate until each child node has completed it's processing for this batch of results.
            while($child->execute($nodeBatchResult));
        }

        // This node and it's children have completed their fetches for this batch iteration.
        // Now process the source: store expand data and mark items in the source that match
        $this->processBatchIteration($sourceBatchResult, $nodeBatchResult);

        if (!$this->batchFetchCursor->hasMore()) {
            // We have processed all our results now for this source. Now we can remove any unmatched items from the source
            $this->completeBatchFetch($sourceBatchResult);

            // No more results, return false to indicate we are done for this batch iteration
            return false;
        }

        // Return true to indicate we are done but there maybe further batches for this node
        return true;
    }


    private function addFilterFromSourceData(StorageRequest $request, array &$sourceBatchResult)
    {
        if ($this->navigationProperty) {
            $nodeKey = $this->navigationProperty->getRelatedEntityKey();
            $sourceKey = $this->navigationProperty->getEntityKey();

/*            switch($request->getDefaultResourceName()) {
                case 'Locations':
                    // Do nothing
                    return;
                    break;

                case 'LookupCountys':
                    // Filter by the key that maps the previous entity to this entity (defined in the navigation property metadata)
                    // Need to use a resolver for this - key could be in either entities metadata, depending on relation type: 1:1, 1:many
                    $nodeKey = 'LookupCountyID';
                    $sourceKey = 'LookupCountyID';
                    break;

                case 'LookupNations':
                    $nodeKey = 'LookupNationID';
                    $sourceKey = 'LookupNationID';
                    break;

                case 'CentresLinksLookupTargetAudiences':
                    $nodeKey = 'CentreID';
                    $sourceKey = 'LocationID';
                    break;
            }
*/
            $pkList = array();
            foreach($sourceBatchResult as $row) {
                // Store keys in array index to prevent dupes
                $pkList[$row[$sourceKey]] = true;
            }

            $request->addFilter(array($nodeKey => array('$in' => array_keys($pkList))));
        }
    }


    private function processBatchIteration(array &$sourceBatchResult, array $nodeBatchResult)
    {
        // Now process the source: store expand data or delete unmatched entities

        // @todo: expand here

        if ($this->navigationProperty) {
            $nodeKey = $this->navigationProperty->getRelatedEntityKey();
            $sourceKey = $this->navigationProperty->getEntityKey();

/*            switch($this->entityName) {
                case 'Locations':
                    // Do nothing


                    // hack here. as this is the root we want to decant the node results into the source, so the client can retrieve the result
                    foreach($nodeBatchResult as $row) {
                        $sourceBatchResult[] = $row;
                    }


                    $nodeKey = 'LocationID';
                    //$sourceKey = 'LocationID';
                    break;

                case 'LookupCountys':
                    // Filter by the key that maps the previous entity to this entity (defined in the navigation property metadata)
                    // Need to use a resolver for this - key could be in either entities metadata, depending on relation type: 1:1, 1:many
                    $nodeKey = 'LookupCountyID';
                    //$sourceKey = 'LookupCountyID';
                    break;

                case 'LookupNations':
                    $nodeKey = 'LookupNationID';
                    //$sourceKey = 'LookupNationID';
                    break;

                case 'CentresLinksLookupTargetAudiences':
                    $nodeKey = 'CentreID';
                    //$sourceKey = 'LocationID';
                    break;
            }*/

            // Get the mapping key (key that links node to source) values of all our matched results
            foreach($nodeBatchResult as $row) {
                $this->keepList[$row[$nodeKey]] = true;
            }
        } else {
            // hack here. as this is the root we want to decant the node results into the source, so the client can retrieve the result
            foreach($nodeBatchResult as $row) {
                $sourceBatchResult[] = $row;
            }
        }
    }


    private function completeBatchFetch(array &$sourceBatchResult)
    {
        // Remove original items that do not match the nodes results

        // If this proves to be the right way to go we can optimise by storing results indexed by key hash.
        // Key must be a key hash to allow for compound keys, so a compound key may look something like 1345.56.7
        // Again metadata will save us here :-)
        //$nodeKey = null;
        /*$sourceKey = null;
        switch($this->entityName) {
            case 'Locations':
                //$nodeKey = 'LocationID';
                $sourceKey = 'LocationID';
                break;

            case 'LookupCountys':
                //$nodeKey = 'LookupCountyID';
                $sourceKey = 'LookupCountyID';
                break;

            case 'LookupNations':
                //$nodeKey = 'LookupNationID';
                $sourceKey = 'LookupNationID';
                break;

            case 'CentresLinksLookupTargetAudiences':
                //$nodeKey = 'CentreID';
                $sourceKey = 'LocationID';
                break;
        }*/

        $sourceKey = 'LocationID';
        if ($this->navigationProperty) {
            $sourceKey = $this->navigationProperty->getEntityKey();

            $indexesToRemove = array();
            $keepList = array_keys($this->keepList);
            foreach($sourceBatchResult as $i => $row) {
                if (!in_array($row[$sourceKey], $keepList)) {
                    $indexesToRemove[] = $i;
                }
            }

            foreach($indexesToRemove as $bye) {
                unset($sourceBatchResult[$bye]);
            }
        }

        $this->batchFetchCursor = null;
        $this->keepList = array();
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
    public function getState()
    {
        if (!$this->batchFetchCursor) {
            return self::UNREADY;
        }
        return $this->batchFetchCursor->hasMore() ? self::READY : self::COMPLETE;
    }


    public function prepare(array $inputData)
    {
        // We should not be called if we already have a cursor that is in flight
        if (self::UNREADY !== $this->getState()) {
            throw new Exception($this->getName() . ' node error - prepare called with existing active batch cursor');
        }

        // Create a new batch request, from the original details provided when the node was created
        $request = new StorageRequest($this->baseRequest->getDefaultResourceName(), $this->baseRequest->getMetadata());
        $request->setFilter($this->baseRequest->getFilter());

        // Add any extra filter from the source data - eg extract any relationship keys to filter by
        $this->addFilterFromSourceData($request, $inputData);

        // Create the cursor
        $this->batchFetchCursor = $this->storageManager->prepareBatchFetch($request, $this->batchSize);
        $this->log('Created batch fetch cursor from input data rows: ' . count($inputData));

        // Store the input data for argregating results
        $this->inputData = $inputData;

        if (count($this->children)) {
            // This is not a leaf node, so we must fetch our results now so it's ready when child node results come in and we need to compile
            $this->nodeData = $this->batchFetchCursor->getNextBatch();
            $this->log('Prepare parent - fetched data rows: ' . count($this->nodeData));

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
     * - array of results - please call us again via fetch() to potentially get more
     * - false if this batch is complete and we are ready for more input via prepare()
     *
     * @return false | array
     * @throws Exception
     */
    public function fetch()
    {
        // Sanity check. We must always have a cursor defined
        if (!$this->batchFetchCursor) {
            throw new Exception($this->getName() . ' node error - fetch called us when we had no cursor defined');
        }

        // Fetch process depends on if we are a leaf or parent node
        if (count($this->children)) {
            // Get results from child nodes
            $result = $this->fetchFromChildNode(0);
            if ($result) {
                // Child nodes returned a result, return it.

                // @todo: combine with node results

                return $result;
            }

            // Child nodes returned nothing. Maybe this node has more results to fetch from it's cursor?
            if (self::COMPLETE == $this->getState()) {
                // Nope, we are done.
                $this->purge();
                return false;
            }

            // We have more, go get it.
            $this->nodeData = $this->batchFetchCursor->getNextBatch();
            $this->log('Fetch parent - fetched data rows: ' . count($this->nodeData));

            // Reset child results.
            $this->purgeChildNodes();

            // Call ourselves to start over
            return $this->fetch();
        } else {
            return $this->fetchFromLeafNode();
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

            // @todo: collate results

            return $this->childNodeResults;
        }

        // We finish if child index is less than zero - we backed up to the end
        if ($childIndex < 0) {
            return false;
        }

        // Not complete - get the child node for this index
        $childNode = $this->children[$childIndex];

        // Is child node complete?
        $state = $childNode->getState();
        if (self::COMPLETE === $state) {
            // This node is complete
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

            // @todo: prepare child node with combination of this nodes results and other child node results stored in $this->childNodeResults

            $childNode->prepare($this->nodeData);
        }

        // Child node is ready, fetch some data
        $childResult = $childNode->fetch();
        if (is_array($childResult)) {
            // We have data - store it in the results array provided and call ourself to dive into the next level
            $this->childNodeResults[$childIndex] = $childResult;
            return $this->fetchFromChildNode($childIndex + 1);
        }

        // This child produced no results - return false to indicate this batch is completed
        return false;
    }


    private function fetchFromLeafNode()
    {
        // Is this leaf node done?
        if (!$this->batchFetchCursor->hasMore()) {
            // No, we are done. Return false to indicate we are done with what we were given in the last prepare().
            // If you want us to do anything further, send us more input via prepare()
            return false;
        }

        // We are not complete, so we still have work to do with this batch cursor - go get some data.
        $this->nodeData = $this->batchFetchCursor->getNextBatch();
        $this->log('Leaf fetch - fetched data rows: ' . count($this->nodeData));

        // This node is a leaf node (no kids) so it can return results. Do we have any?
        if (count($this->nodeData)) {
            return $this->nodeData;
        } else {
            // Nothing this time.
            // If we have more results to fetch from this cursor return false. Otherwise return true to show we are done now.
            return false;
        }
    }


    public function log($msg)
    {
        echo $this->getName() . ': ' . $msg . PHP_EOL;
    }
} 