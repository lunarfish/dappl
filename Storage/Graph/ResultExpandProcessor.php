<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 19:42
 */

namespace Dappl\Storage\Graph;


class ResultExpandProcessor implements ResultProcessorInterface
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
	 * @param ResultCollection $nodeResultCollection
	 * @return ResultCollection
	 */
	public function combineNodeAndChildNodeResults(array $childResults, ResultCollection $nodeResultCollection)
	{
		// HACK: protect original $nodeResultCollection - some of the operations here remove entities from $nodeResultCollection. Fine for single batch operations
		//       but multiple batch calls with the incomplete $nodeResultCollection
		//       @todo: alter algorithm to avoid having to do this.
		$nodeResultCollection = clone $nodeResultCollection;

		// Create a new result set to return.
		$returnResultCollection = new ResultCollection($nodeResultCollection->getFetchNode());

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
	 * @param ResultCollection $returnResultCollection
	 * @param ResultCollection $childResultCollection
	 */
	public function embedMatchingChildResults(ResultCollection $returnResultCollection, ResultCollection $childResultCollection)
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
	 * @param ResultCollection $returnResultCollection
	 * @param ResultCollection $nodeResultCollection
	 * @param ResultCollection $childResultCollection
	 */
	public function createFetchResultCollectionEmbeddedWithChildResults(ResultCollection $returnResultCollection, ResultCollection $nodeResultCollection, ResultCollection $childResultCollection)
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
