<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 25/06/2014
 * Time: 15:33
 */

namespace Dappl\Storage\Driver;

use \Dappl\Fetch\Request as StorageRequest;


class MongoDriver implements DriverInterface
{
    private $db;
    private $isDebugging;

    public function connect(array $params, $isDebugging)
    {
        // Are we already connected?
        if ($this->db) {
            throw new \Exception('Already connected');
        }

        // Extract parameters
        $dbName = array_key_exists('dbname', $params) ? $params['dbname'] : null;

        $mongo = new \MongoClient();
        $this->db = $mongo->selectDB($dbName);
        if (!$this->db) {
            throw new \Exception(sprintf('Could not connect to db: [%s]', $dbName));
        }

        $this->isDebugging = $isDebugging;
    }


    public function fetchOne(StorageRequest $request)
    {
        $collection = $this->db->selectCollection($request->getDefaultResourceName());
        return $collection->findOne($request->getFilter());
    }


    public function prepareFetch(StorageRequest $request)
    {
        // All mongo code for testing. Split out later to driver classes
        $collection = $this->db->selectCollection($request->getDefaultResourceName());
        $cursor = $collection->find($request->getFilter());
        if ($this->isDebugging) {
            echo sprintf('Storage request on: %s, filter: %s, results: %d %s',
                $request->getDefaultResourceName(),
                json_encode($request->getFilter()),
                $cursor->count(),
                PHP_EOL);
        }
        return new \Dappl\Storage\Cursor\MongoCursor($cursor);
    }


    public function prepareBatchFetch(StorageRequest $request, $batchSize)
    {
        $cursor = $this->prepareFetch($request);
        return new \Dappl\Storage\Cursor\BatchCursor($cursor, $batchSize, $this->isDebugging);
    }

} 