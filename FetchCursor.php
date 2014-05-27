<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 16:27
 */


interface FetchCursor
{
    public function getNext();
    public function hasMore();
}


class MongoFetchCursor implements FetchCursor
{
    private $mongoCursor;


        public function __construct(MongoCursor $cursor)
    {
        $this->mongoCursor = $cursor;
    }


    public function getNext()
    {
        $result = false;
        if ($this->mongoCursor->hasNext()) {
            $result = $this->mongoCursor->getNext();
        }
        return $result;
    }


    public function hasMore()
    {
        return $this->mongoCursor->hasNext();
    }
}



class BatchFetchCursor
{
    private $cursor;
    private $batchSize;


    public function __construct(FetchCursor $cursor, $batchSize)
    {
        $this->cursor = $cursor;
        $this->batchSize = $batchSize;
    }


    public function getNextBatch(FetchResultCollection $resultCollection)
    {
        $i = $this->batchSize;
        while ($i > 0) {
            $result = $this->cursor->getNext();
            if (!$result) {
                break;
            }
            $resultCollection->addEntity((object)$result);
            $i--;
        }
        return $resultCollection;
    }


    public function hasMore()
    {
        return $this->cursor->hasMore();
    }
}



class FetchResultCollection implements Countable, Iterator
{
    private $items;
    private $primaryKey;
    private $indexNames;
    private $indexValues;
    private $fetchNode;


    public function __construct(FetchNode $fetchNode = null)
    {
        $this->items = array();
        $this->indexNames = array();
        $this->indexValues = array();
        $this->fetchNode = $fetchNode;
    }


    public function getFetchNode()
    {
        return $this->fetchNode;
    }


    public function setPrimaryKeyName($primaryKeyName)
    {
        if (!empty($primaryKeyName)) {
            // At the moment keep things simple and require keys and indexes to be set upfront
            if (count($this->items)) {
                throw new Exception('Cannot set primary key, we already have items stored');
            }

            $this->primaryKey = $primaryKeyName;
        }
    }


    public function getPrimaryKeyName()
    {
        return $this->primaryKey;
    }


    public function addIndex($indexName)
    {
        // At the moment keep things simple and require keys and indexes to be set upfront
        if (count($this->items)) {
            throw new Exception('Cannot add index, we already have items stored');
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


    public function addIndexNames(array $indexNames)
    {
        foreach($indexNames as $indexName) {
            $this->addIndex($indexName);
        }
    }


    public function getIndexNames()
    {
        return $this->indexNames;
    }


    public function addEntity($entity)
    {
        // Sanity check
        if (!$entity || !is_object($entity)) {
            throw new Exception('Error - cannot add entity with value: ' . serialize($entity));
        }

        // Add to items either by primary key or normal array index
        $itemIndex = null;
        if ($this->primaryKey) {
            // Use entity primary key value
            if (!property_exists($entity, $this->primaryKey)) {
                throw new Exception(sprintf('Cannot add entity to fetch result collection, missing primary key: [%s] in entity: [%s]', $this->primaryKey, serialize($entity)));
            }
            $pk = $this->primaryKey;
            $itemIndex = $entity->$pk;

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
            // Check index value exists on the entity
            if (!property_exists($entity, $indexName)) {
                throw new Exception(sprintf('Cannot add entity to fetch result collection, missing index: [%s] in entity: [%s]', $indexName, serialize($entity)));
            }

            // Create index value entry if required
            $indexValue = $entity->$indexName;
            if (!array_key_exists($indexValue, $this->indexValues[$indexName])) {
                $this->indexValues[$indexName][$indexValue] = array();
            }

            // Store item index. Now we can refer back to all the entities matching this index value
            $this->indexValues[$indexName][$indexValue][] = $entity;
        }
    }


    public function removeEntity($entity)
    {
        // Remove from indexes
        foreach($this->indexNames as $indexName) {
            // Check index value exists on the entity
            if (!property_exists($entity, $indexName)) {
                throw new Exception(sprintf('Cannot remove entity, missing index: [%s] in entity: [%s]', $indexName, serialize($entity)));
            }

            // Create index value entry if required
            $indexValue = $entity->$indexName;
            if (array_key_exists($indexValue, $this->indexValues[$indexName])) {
                $deleteKey = array_search($entity, $this->indexValues[$indexName][$indexValue], true);
                if (false !== $deleteKey) {
                    unset($this->indexValues[$indexName][$indexValue][$deleteKey]);
                }
            }
        }

        // Remove from primary
        if ($this->primaryKey) {
            // Via key
            $pk = $this->primaryKey;
            unset($this->items[$entity->$pk]);
        } else {
            // By search
            $deleteKey = array_search($entity, $this->items, true);
            if (false !== $deleteKey) {
                unset($this->items[$deleteKey]);
            }
        }
    }


    public function getEntitiesByIndex($indexName, $value)
    {
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
            throw new Exception(sprintf('Undefined index:[%s]', $indexName));
        }
    }


    public function purge()
    {
        $this->items = array();
        foreach($this->indexNames as $indexName) {
            $this->indexValues[$indexName] = array();
        }
    }


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
echo __METHOD__ . '(' . $fieldName . ') using brute force!!!' . PHP_EOL;
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
     * Fetches to objects during iteration may mess things up...!
     * Want to later be able to implement iterating by predefined index via
     * something like iterateByIndexName()
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
//echo sprintf('key: %s, value: %s', key($this->items), json_encode(current($this->items))) . PHP_EOL;
        return !empty($key);
    }
}