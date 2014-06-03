<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 16:16
 */

namespace Dappl\Storage;

use \Dappl\Fetch\Request as StorageRequest;


class Manager
{
    private $mongoDB;
    private $metadata;


    public function __construct(array $params)
    {
        // Params will be current crm datastore_dev.yaml stuff describing connection details and driver to use

        // All mongo code for testing. Split out later to driver classes
        $mongo = new \MongoClient();
        $this->mongoDB = $mongo->selectDB('ukonline');

        $mongo2 = new \MongoClient();
        $this->metadata = $mongo2->selectDB('metadata');
    }


    public function fetchOne(StorageRequest $request)
    {
        // stub for real functionality
        $collection = $this->metadata->selectCollection('Entities');
        return $collection->findOne($request->getFilter());
    }


    public function prepareFetch(StorageRequest $request)
    {
        // All mongo code for testing. Split out later to driver classes
        $collection = $this->mongoDB->selectCollection($request->getDefaultResourceName());
        $cursor = $collection->find($request->getFilter());
echo sprintf('Storage request on: %s, filter: %s, results: %d %s',
             $request->getDefaultResourceName(),
             json_encode($request->getFilter()),
             $cursor->count(),
             PHP_EOL);
        return new \Dappl\Storage\Cursor\MongoCursor($cursor);
    }


    public function prepareBatchFetch(StorageRequest $request, $batchSize)
    {
        $cursor = $this->prepareFetch($request);
        return new \Dappl\Storage\Cursor\BatchCursor($cursor, $batchSize);
    }
} 