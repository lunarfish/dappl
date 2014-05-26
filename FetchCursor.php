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
            $resultCollection->addEntity($result);
            $i--;
        }
        return $resultCollection;
    }


    public function hasMore()
    {
        return $this->cursor->hasMore();
    }
}



class FetchResultCollection implements Countable
{
    private $items;
    private $primaryKey;
    private $indexNames;
    private $indexValues;


    public function __construct()
    {
        $this->items = array();
        $this->indexNames = array();
        $this->indexValues = array();
    }


    public function setPrimaryKeyName($primaryKeyName)
    {
        // At the moment keep things simple and require keys and indexes to be set upfront
        if (count($this->items)) {
            throw new Exception('Cannot set primary key, we already have items stored');
        }

        $this->primaryKey = $primaryKeyName;
    }


    public function addIndex($indexName)
    {
        // At the moment keep things simple and require keys and indexes to be set upfront
        if (count($this->items)) {
            throw new Exception('Cannot add index, we already have items stored');
        }

        $this->indexNames[] = $indexName;
        $this->indexValues[$indexName] = array();
    }


    public function addEntity(array $entity)
    {
        // Add to items either by primary key or normal array index
        $itemIndex = null;
        if ($this->primaryKey) {
            // Use entity primary key value
            if (!array_key_exists($this->primaryKey, $entity)) {
                throw new Exception(sprintf('Cannot add entity to fetch result collection, missing primary key: [%s] in entity: [%s]', $this->primaryKey, serialize($entity)));
            }
            $itemIndex = $entity[$this->primaryKey];
        } else {
            // Use array index
            $itemIndex = count($this->items);
        }

        // Add entity to main items storage
        $this->items[$itemIndex] = $entity;

        // Add the item index to each index
        foreach($this->indexNames as $indexName) {
            // Check index value exists on the entity
            if (!array_key_exists($indexName, $entity)) {
                throw new Exception(sprintf('Cannot add entity to fetch result collection, missing index: [%s] in entity: [%s]', $indexName, serialize($entity)));
            }

            // Create index value entry if required
            $indexValue = $entity[$indexName];
            if (!array_key_exists($indexValue, $this->indexValues[$indexName])) {
                $this->indexValues[$indexName][$indexValue] = array();
            }

            // Store item index. Now we can refer back to all the entities matching this index value
            $this->indexValues[$indexName][$indexValue][] = $itemIndex;
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
            foreach($this->items as $entity) {
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
}