<?php

interface FetchNodeResultProcessorInterface
{
    /**
     * Method to perform 2 critical tasks:
     * 1. To filter out any fetched data that should not be part of the returned result for this node (assuming AND on all predicates)
     * 2. To combine the fetched data into one result collection (either expand or projection)
     *
     * @param array $childResults - and array of completed FetchResultCollection objects, from each child node
     * @param FetchResultCollection $nodeResultCollection - the completed results for the node
     * @return \FetchResultCollection - a new FetchResultCollection containing the filtered and combined results
     */
    public function combineNodeAndChildNodeResults(array $childResults, FetchResultCollection $nodeResultCollection);
}


class FetchNodeResultExpandProcessor implements FetchNodeResultProcessorInterface
{
    /**
     * Method to perform 2 critical tasks:
     * 1. To filter out any fetched data that should not be part of the returned result for this node (assuming AND on all predicates)
     * 2. To combine the fetched data into one result collection
     *
     * Process is:
     * - Look at the last child results first. Link each last child result to the node results
     * - Embed the last child result into the node entity navigation property
     * - Call this the return result
     * - Move to the next but last child.
     * - Link each return result item to the next but last child item, and embed in it's navigation property
     *
     * IT MAY LOOK LIKE WE ARE DUPLICATING EFFORT - Some of this is processed in prepare().
     * There probably can be a more efficient way of processing but this is done to decouple the setting up of the fetch tree
     * to results returning back to the client code. It's important to decouple these so results can return before the entire tree
     * has finished processing, to avoid possible fatal memory consumption for large datasets.
     *
     * @param array $childResults
     * @param FetchResultCollection $nodeResultCollection
     * @return \FetchResultCollection
     */
    public function combineNodeAndChildNodeResults(array $childResults, FetchResultCollection $nodeResultCollection)
    {
        // HACK: protect original $nodeResultCollection - some of the operations here remove entities from $nodeResultCollection. Fine for single batch operations
        //       but multiple batch calls with the incomplete $nodeResultCollection
        //       @todo: alter algorithm to avoid having to do this.
        $nodeResultCollection = clone $nodeResultCollection;

        // Create a new result set to return.
        $returnResultCollection = new FetchResultCollection($nodeResultCollection->getFetchNode());

        // Define keys to match original node result collection
        $returnResultCollection->setPrimaryKeyName($nodeResultCollection->getPrimaryKeyName());
        $returnResultCollection->addIndexNames($nodeResultCollection->getIndexNames());

        // Last child first - merge and embed matches in last child into result collection (3 way merge)
        $childResultCollection = array_pop($childResults);
        $this->createFetchResultCollectionEmbeddedWithChildResults($returnResultCollection, $nodeResultCollection, $childResultCollection);

        // Process other child nodes. Iterate the result collection and embed and matching child results
        while($childResultCollection = array_pop($childResults)) {
            $this->embedMatchingChildResults($returnResultCollection, $childResultCollection);
        }

        return $returnResultCollection;
    }


    /**
     * Embeds any child results that match the return results via their navigation property.
     * @param FetchResultCollection $returnResultCollection
     * @param FetchResultCollection $childResultCollection
     */
    public function embedMatchingChildResults(FetchResultCollection $returnResultCollection, FetchResultCollection $childResultCollection)
    {
        // Get the navigation property between the child collection and the base collection
        $navigationProperty = $childResultCollection->getFetchNode()->getNavigationProperty();
        $nodeKey = $navigationProperty->getRelatedEntityKey();
        $childKey = $navigationProperty->getEntityKey();
        $name = $navigationProperty->getName();

        foreach($returnResultCollection as $nodeEntity) {
            // Get matching child nodes
            $childEntities = $childResultCollection->getEntitiesByIndex($childKey, $returnResultCollection->getIndexValue($nodeEntity, $nodeKey));
            if ($navigationProperty->isScalar()) {
                // For scalar properties the object is embedded directly
                $childEntities = count($childEntities) ? $childEntities[0] : null;
            }
            $nodeEntity->$name = $childEntities;
        }
    }


    /**
     * Performs a 3 way merge. Takes entities in $nodeResultCollection that match related entities in $childResultCollection and puts them
     * in $returnResultCollection. The child entities are embedded.
     *
     * @param FetchResultCollection $returnResultCollection
     * @param FetchResultCollection $nodeResultCollection
     * @param FetchResultCollection $childResultCollection
     */
    public function createFetchResultCollectionEmbeddedWithChildResults(FetchResultCollection $returnResultCollection, FetchResultCollection $nodeResultCollection, FetchResultCollection $childResultCollection)
    {
        // Determine navigation property for this child node
        $childFetchNode = $childResultCollection->getFetchNode();
        $childNavigationProperty = $childFetchNode->getNavigationProperty();
        $childNavigationPropertyName = $childNavigationProperty->getName();
        $nodeKey = $childNavigationProperty->getRelatedEntityKey();
        $childKey = $childNavigationProperty->getEntityKey();

        // Iterate by the entity key
        //$childResultCollection->iterateByIndexName($childNavigationProperty->getEntityKey());
        foreach($childResultCollection as $childResultEntity) {
            // Get the corresponding node entity. First move any matches from node collection to return collection
            // There maybe more than one, for many:1 relationships (eg LookupCounty -> LookupNation)
            // (Moving records incase some of the matched entities are already in results, and some are in node collection
            // -  this way we get them all)
            $nodeEntities = $nodeResultCollection->getEntitiesByIndex($nodeKey, $childResultCollection->getIndexValue($childResultEntity, $childKey));
            foreach($nodeEntities as $nodeEntity) {
                $returnResultCollection->addEntity($nodeEntity);
                $nodeResultCollection->removeEntity($nodeEntity);
            }

            // Now get all the result collection matches and embed the child entity
            $nodeEntities = $returnResultCollection->getEntitiesByIndex($nodeKey, $childResultCollection->getIndexValue($childResultEntity, $childKey));
            foreach($nodeEntities as $nodeEntity) {
                if ($childNavigationProperty->isScalar()) {
                    // Scalar navigation property, only one allowed so embed directly
                    $nodeEntity->$childNavigationPropertyName = $childResultEntity;
                } else {
                    // Non scalar, multiple entity navigation property so embed in array
                    // Create node entity navigation property, if required
                    if (!property_exists($nodeEntity, $childNavigationPropertyName)) {
                        $nodeEntity->$childNavigationPropertyName = array();
                    }

                    // Add child entity to the node entity
                    array_push($nodeEntity->$childNavigationPropertyName, $childResultEntity);
                }
            }
        }
    }
}












class FetchNodeResultProjectionProcessor implements FetchNodeResultProcessorInterface
{
    /**
     * Method to perform 2 critical tasks:
     * 1. To filter out any fetched data that should not be part of the returned result for this node (assuming AND on all predicates)
     * 2. To flatten (projection) the fetched data into one result collection
     *
     * Process is:
     * - Create a new FetchResultCollection to store output (resultCollection)
     *   - Add same indexes from $nodeResultCollection. Instead of setting primary key, set it as an index.
     *   - This will allow us to collect multiple result entities for each primary key - this will occur as embedded child results are flattened
     * - Seed this result collection with the node collection results that have results matching the last child results
     *   (AND predicate behavior between child nodes)
     *   - Use the last child as that had the most specific (Again with the current AND everything behaviour) fetch filter, so it's results
     *     will refer to the fewest node results
     *   - We now have a filtered result set containing node entity data, but no child entity data yet
     *   - Create a copy to iterate over.
     * - Iterate the copy of the filtered results
     *   - For each result, iterate each child results.
     *   - Embed the child results, creating extra result entities for all but the first
     */
    public function combineNodeAndChildNodeResults(array $childResults, FetchResultCollection $nodeResultCollection)
    {
        // HACK: protect original $nodeResultCollection - some of the operations here remove entities from $nodeResultCollection. Fine for single batch operations
        //       but multiple batch calls with the incomplete $nodeResultCollection
        //       @todo: alter algorithm to avoid having to do this.
        $nodeResultCollection = clone $nodeResultCollection;

        // Create a new result set to return.
        $returnResultCollection = new FetchResultCollection($nodeResultCollection->getFetchNode());

        // Define keys to match original node result collection
        // NOTE here we are setting the primary key as an ordinary index, as a projection result set may contain multiple rows with the same primary key
        $returnResultCollection->addIndex($nodeResultCollection->getPrimaryKeyName());
        $returnResultCollection->addIndexNames($nodeResultCollection->getIndexNames());

        // Last child first - merge matches in last child into result collection (3 way merge)
        $childResultCollection = end($childResults);
        $this->createNodeFetchResultCollectionMatchingChildResults($returnResultCollection, $nodeResultCollection, $childResultCollection);

        // We only want to iterate this filtered return result set, but add to it as we go.
        // To avoid breaking the iterator make a copy before we start.
        $returnNodesToIterate = $returnResultCollection->getAll();
        foreach($returnNodesToIterate as $nodeEntity) {
            // using this node entity as a base object, embed all the child nodes, adding extra rows for multiple results
            foreach($childResults as $childResultCollection) {
                $this->embedMatchingChildResults($nodeEntity, $returnResultCollection, $childResultCollection);
            }
        }

        // If the node result collection is from the root node, we can strip fields down to those defined in $select of requests.
        $fetchNode = $returnResultCollection->getFetchNode();
        if ($fetchNode->isRoot()) {
            // Extract all required fields from the node tree
            $fields = array();
            $fetchNode->getSelectFields($fields);

            // Crop result entities to those fields
            foreach($returnResultCollection as $entity) {
                foreach($entity as $key => $value) {
                    if (!in_array($key , $fields)) {
                        unset($entity->$key);
                    }
                }
            }
        }

        return $returnResultCollection;
    }


    /**
     * Performs a 3 way merge. Takes entities in $nodeResultCollection that match related entities in $childResultCollection and puts them
     * in $returnResultCollection.
     *
     * @param FetchResultCollection $returnResultCollection
     * @param FetchResultCollection $nodeResultCollection
     * @param FetchResultCollection $childResultCollection
     */
    private function createNodeFetchResultCollectionMatchingChildResults(FetchResultCollection $returnResultCollection, FetchResultCollection $nodeResultCollection, FetchResultCollection $childResultCollection)
    {
        // Determine navigation property for this child node
        $childFetchNode = $childResultCollection->getFetchNode();
        $childNavigationProperty = $childFetchNode->getNavigationProperty();
        $nodeKey = $childNavigationProperty->getRelatedEntityKey();
        $childKey = $childNavigationProperty->getEntityKey();

        // Iterate child results
        foreach($childResultCollection as $childResultEntity) {
            // Get the corresponding node entity. Move any matches from node collection to return collection
            // There maybe more than one, for many:1 relationships (eg LookupCounty -> LookupNation)
            // (Moving records incase some of the matched entities are already in results, and some are in node collection
            // -  this way we get them all)
            $nodeEntities = $nodeResultCollection->getEntitiesByIndex($nodeKey, $childResultCollection->getIndexValue($childResultEntity, $childKey));
            foreach($nodeEntities as $nodeEntity) {
                $returnResultCollection->addEntity($nodeEntity);
                $nodeResultCollection->removeEntity($nodeEntity);
            }
        }
    }


    /**
     * Copies fields from source entity into target entity, prefixing properties with $namespace to avoid collisions
     *
     * @param $targetEntity
     * @param $namespace
     * @param $sourceEntity
     */
    private function importChildEntity($targetEntity, $namespace, $sourceEntity)
    {
        foreach($sourceEntity as $key => $value) {
            $name = $namespace . '.' . $key;
            $targetEntity->$name = $value;
        }
    }


    /**
     * For the source node entity, finds and existing related result entities.
     * Iterates these, then for each result entity embeds any matching child results, creating extra entities if there are more than one child results.
     * @param $sourceNodeEntity
     * @param FetchResultCollection $returnResultCollection
     * @param FetchResultCollection $childResultCollection
     */
    private function embedMatchingChildResults($sourceNodeEntity, FetchResultCollection $returnResultCollection, FetchResultCollection $childResultCollection)
    {
        // Get the navigation property between the child collection and the base collection
        $navigationProperty = $childResultCollection->getFetchNode()->getNavigationProperty();
        $nodeKey = $navigationProperty->getRelatedEntityKey();
        $childKey = $navigationProperty->getEntityKey();
        $name = $navigationProperty->getName();

        // We can't directly iterate the child results - there maybe results that do not match (AND behavior)
        // First we must get the results matching the node entity for this child, then iterate them.
        $matchingNodeEntities = $returnResultCollection->getEntitiesByIndex($nodeKey, $returnResultCollection->getIndexValue($sourceNodeEntity, $nodeKey));
        foreach($matchingNodeEntities as $nodeEntity) {
            // If we have multiple child results we have to create extra rows. Store the nodeEntity state before it gets mutated.
            $prototypeNodeEntity = clone $nodeEntity;

            // Get the child entities matching this node entity.
            $childEntities = $childResultCollection->getEntitiesByIndex($childKey, $returnResultCollection->getIndexValue($nodeEntity, $nodeKey));
            $isFirst = true;
            foreach($childEntities as $childEntity) {
                if ($isFirst) {
                    // The first child can be embedded directly into the node entity
                    $this->importChildEntity($nodeEntity, $name, $childEntity);

                    $isFirst = false;
                } else {
                    // Not the first - we have to create new objects from the node prototype
                    $newEntity = clone $prototypeNodeEntity;
                    $this->importChildEntity($newEntity, $name, $childEntity);

                    // Add this new entity into our result set
                    $returnResultCollection->addEntity($newEntity);
                }
            }
        }
    }
}