<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 16:27
 */

namespace Dappl\Storage\Graph;


class ResultCollection implements \Countable, \Iterator
{
    private $items;
    private $primaryKey;
    private $indexNames;
    private $indexValues;
    private $fetchNode;


    public function __construct(Node $fetchNode = null)
    {
        $this->items = array();
        $this->indexNames = array();
        $this->indexValues = array();
        $this->fetchNode = $fetchNode;
    }


    /**
     * Returns the node owning this result collection. This should always return a value, apart from the collection
     * passed to the root node in FetchNode::fetch()
     * @return FetchNode
     */
    public function getFetchNode()
    {
        return $this->fetchNode;
    }


    /**
     * Set the name of the primary key for entities stored in this collection
     * @param $primaryKeyName
     * @throws Exception
     */
    public function setPrimaryKeyName($primaryKeyName)
    {
        if (!empty($primaryKeyName)) {
            // At the moment keep things simple and require keys and indexes to be set upfront
            if (count($this->items)) {
                throw new \Exception('Cannot set primary key, we already have items stored');
            }

            $this->primaryKey = $primaryKeyName;
        }
    }


    /**
     * Get the name of the primary key for entities stored in this collection
     * @return mixed
     */
    public function getPrimaryKeyName()
    {
        return $this->primaryKey;
    }


    /**
     * Adds an index. Must be called before any entities are added to the collection.
     * @param $indexName
     * @throws Exception
     */
    public function addIndex($indexName)
    {
        // At the moment keep things simple and require keys and indexes to be set upfront
        if (count($this->items)) {
            throw new \Exception('Cannot add index, we already have items stored');
        }

        // Ignore nothing or if the index is already defined as primary key
        if (empty($indexName) || ($this->primaryKey == $indexName)) {
            return;
        }

        if (!in_array($indexName, $this->indexNames)) {
            $this->indexNames[] = $indexName;
            $this->indexValues[$indexName] = array();
        }
    }


    /**
     * Adds multiple indexes
     * @param array $indexNames
     */
    public function addIndexNames(array $indexNames)
    {
        foreach($indexNames as $indexName) {
            $this->addIndex($indexName);
        }
    }


    /**
     * Return array of index names defined for this collection
     * @return array
     */
    public function getIndexNames()
    {
        return $this->indexNames;
    }


    /**
     * Returns the primary key value for given entity
     *
     * Throws exception if the entity does not possess the primary key property
     *
     * We have problems in our data. Some pk and fk values are stored as floats in mongo, and floats should not be used as array indexes!
     * As a workaround here we convert any float values to int, although the correct way is to validate data type before storing!
     *
     * @param $entity
     * @throws Exception
     * @return int | string
     */
    public function getPrimaryKeyValue($entity)
    {
        if (!property_exists($entity, $this->primaryKey)) {
            throw new \Exception(sprintf('Cannot find primary key: [%s] in entity: [%s]', $this->primaryKey, serialize($entity)));
        }
        $pk = $this->primaryKey;
        return is_float($entity->$pk) ? (int)$entity->$pk : $entity->$pk;
    }


    /**
     * Returns the index value for given entity
     *
     * Throws exception if the entity does not possess the index property
     *
     * We have problems in our data. Some pk and fk values are stored as floats in mongo, and floats should not be used as array indexes!
     * As a workaround here we convert any float values to int, although the correct way is to validate data type before storing!
     *
     * @param $entity
     * @param $indexName
     * @throws Exception
     * @return int | string
     */
    public function getIndexValue($entity, $indexName)
    {
        if (empty($indexName)) {
            throw new \Exception('Cannot get index value, no index specified');
        }

        // Check index value exists on the entity
        if (!property_exists($entity, $indexName)) {
            throw new \Exception(sprintf('Cannot find index: [%s] in entity: [%s]', $indexName, serialize($entity)));
        }
        return is_float($entity->$indexName) ? (int)$entity->$indexName : $entity->$indexName;
    }


    /**
     * Adds an entity to the collection, and stores via any primary key/indexes defined.
     *
     * If the entity is already stored it is ignored.
     *
     * Throws exception if entity is invalid
     *
     * @param $entity
     * @throws Exception
     */
    public function addEntity($entity)
    {
        // Sanity check
        if (!$entity || !is_object($entity)) {
            throw new \Exception('Error - cannot add entity with value: ' . serialize($entity));
        }

        // Add to items either by primary key or normal array index
        $itemIndex = null;
        if ($this->primaryKey) {
            // Use entity primary key value
            $itemIndex = $this->getPrimaryKeyValue($entity);

            // Check it's not already there
            if (array_key_exists($itemIndex, $this->items)) {
                return;
            }
        } else {
            // Check it's not already there
            if (false !== array_search($entity, $this->items, true)) {
                return;
            }

            // Use array index
            $itemIndex = count($this->items);
        }

        // Add entity to main items storage
        $this->items[$itemIndex] = $entity;

        // Add the item index to each index
        foreach($this->indexNames as $indexName) {
            // Create index value entry if required
            $indexValue = $this->getIndexValue($entity, $indexName);
            if (!array_key_exists($indexValue, $this->indexValues[$indexName])) {
                $this->indexValues[$indexName][$indexValue] = array();
            }

            // Store item index. Now we can refer back to all the entities matching this index value
            $this->indexValues[$indexName][$indexValue][] = $entity;
        }
    }


    /**
     * Convenience method to add multiple entities
     * @param array $entities
     */
    public function addEntities(array $entities)
    {
        foreach($entities as $entity) {
            $this->addEntity($entity);
        }
    }


    /**
     * Removes entity from the collection and removes any associated key/indexes
     *
     * @param $entity
     */
    public function removeEntity($entity)
    {
        // Remove from indexes
        foreach($this->indexNames as $indexName) {
            // Create index value entry if required
            $indexValue = $this->getIndexValue($entity, $indexName);
            if (array_key_exists($indexValue, $this->indexValues[$indexName])) {
                $deleteKey = array_search($entity, $this->indexValues[$indexName][$indexValue], true);
                if (false !== $deleteKey) {
                    unset($this->indexValues[$indexName][$indexValue][$deleteKey]);
                }
            }
        }

        // Remove from primary
        if ($this->primaryKey) {
            // Via primary key
            unset($this->items[$this->getPrimaryKeyValue($entity)]);
        } else {
            // By search
            $deleteKey = array_search($entity, $this->items, true);
            if (false !== $deleteKey) {
                unset($this->items[$deleteKey]);
            }
        }
    }


    /**
     * Returns an array of entities that match the index name. The index name can be the primary key or a secondary index.
     * For consistency always returns an array of entities, even if the primary key is defined (so there can only be one result)
     *
     * @param $indexName
     * @param $value
     * @return array|null
     * @throws Exception
     */
    public function getEntitiesByIndex($indexName, $value)
    {
        // Clean input - handle some float values pk and fk values stored incorrectly in the db
        if (is_float($value)) {
            $value = (int)$value;
        }

        if ($this->primaryKey == $indexName) {
            // Use primary key
            return array_key_exists($value, $this->items) ? array($this->items[$value]) : array();
        } else if (in_array($indexName, $this->indexNames)) {
            // Use index
            $itemIndexes = null;
            if (array_key_exists($value, $this->indexValues[$indexName])) {
                $itemIndexes = &$this->indexValues[$indexName][$value];
            }
            return $itemIndexes && count($itemIndexes) ? $itemIndexes : array();
        } else {
            // oh dear
            throw new \Exception(sprintf('Undefined index:[%s]', $indexName));
        }
    }


    /**
     * Removes all entities and indexes from the collection
     */
    public function purge()
    {
        $this->items = array();
        foreach($this->indexNames as $indexName) {
            $this->indexValues[$indexName] = array();
        }
    }


    /**
     * Returns an array of key values for the give primary key/index
     *
     * @param $fieldName
     * @return array|null
     */
    public function getUniqueValues($fieldName)
    {
        $result = null;
        if ($fieldName == $this->primaryKey) {
            // Field name is our primary key. Gather and return them
            $result = array_keys($this->items);
        } else if (in_array($fieldName, $this->indexNames)) {
            // We have an index defined for this. Gather and return
            $result = array_keys($this->indexValues[$fieldName]);
        } else {
            // Brute force - no index defined for this field
            //echo __METHOD__ . '(' . $fieldName . ') using brute force!!!' . PHP_EOL;
            $pkList = array();
            foreach($this->items as &$entity) {
                // Store keys in array index to prevent dupes
                $pkList[$entity[$fieldName]] = true;
            }
            $result = array_keys($pkList);
        }
        return $result;
    }


    /**
     * Return a copy of all items in the collection
     * @return array
     */
    public function getAll()
    {
        return $this->items;
    }


	/**
	 * Appends collection items to target array
	 * list ... each seems to be fasterest on the block to do this
	 * http://www.php.net/manual/en/function.array-walk.php#112722
	 * @param $target
	 */
	public function appendItems(array &$target)
	{
		reset($this->items);
		while (list($key, $value) = each($this->items)) {
			$target[] = $value;
		}
	}


    /**
     * To support countable interface
     *
     * @return int
     */
    public function count()
    {
        return count($this->items);
    }


    /**
     * Very basic support iterator interface, operating directly on the items array.
     * Fetching objects from the collection during iteration has not been tested...
     * Want to later be able to implement iterating by predefined index via
     * something like iterateByIndexName()
     *
     * May later also implement interfaces to provide recursive tree iteration,
     * to flatten a results set with expands into a projection (flattened) result set
     *
     */
    public function rewind() {
        return reset($this->items);
    }

    public function current() {
        return current($this->items);
    }

    public function key() {
        return key($this->items);
    }

    public function next() {
        return next($this->items);
    }

    public function valid() {
        $key = key($this->items);
        return !is_null($key);
    }
}