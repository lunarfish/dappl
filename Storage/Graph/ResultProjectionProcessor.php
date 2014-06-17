<?php

namespace Dappl\Storage\Graph;


class ResultProjectionProcessor implements ResultProcessorInterface
{
    /**
     * Method to perform 2 critical tasks:
     * 1. To filter out any fetched data that should not be part of the returned result for this node (assuming AND on all predicates)
     * 2. To flatten (projection) the fetched data into one result collection
     *
     * Process is:
     * - Create a new ResultCollection to store output (resultCollection)
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
    public function combineNodeAndChildNodeResults(array $childResults, ResultCollection $nodeResultCollection)
    {
        // HACK: protect original $nodeResultCollection - some of the operations here remove entities from $nodeResultCollection. Fine for single batch operations
        //       but multiple batch calls with the incomplete $nodeResultCollection
        //       @todo: alter algorithm to avoid having to do this.
        $nodeResultCollection = clone $nodeResultCollection;

		$returnResultCollection = null;
		if (count($childResults)) {
			// Create a new result set to return.
			$returnResultCollection = new ResultCollection($nodeResultCollection->getNode());

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
		} else {
			// We didn't need to process child results, so just use the copy.
			// - playing safe here, may be possible always to use original input instead
			$returnResultCollection = $nodeResultCollection;
		}

        // If the node result collection is from the root node, we can strip fields down to those defined in $select of requests.
        $fetchNode = $returnResultCollection->getNode();
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
     * @param ResultCollection $returnResultCollection
     * @param ResultCollection $nodeResultCollection
     * @param ResultCollection $childResultCollection
     */
    private function createNodeFetchResultCollectionMatchingChildResults(ResultCollection $returnResultCollection, ResultCollection $nodeResultCollection, ResultCollection $childResultCollection)
    {
        // Determine navigation property for this child node
        $childFetchNode = $childResultCollection->getNode();
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
     * @param ResultCollection $returnResultCollection
     * @param ResultCollection $childResultCollection
     */
    private function embedMatchingChildResults($sourceNodeEntity, ResultCollection $returnResultCollection, ResultCollection $childResultCollection)
    {
        // Get the navigation property between the child collection and the base collection
        $navigationProperty = $childResultCollection->getNode()->getNavigationProperty();
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