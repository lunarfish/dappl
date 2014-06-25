<?php

namespace Dappl\Storage\Driver;

use \Dappl\Fetch\Request as StorageRequest;


/**
 * DriverInterface
 *
 * A base interface to be implemented by driver objects for various data source types (MySQL, Mongo, etc)
 *
 * These concrete classes will operate directly on their respective databases
 *
 */
interface DriverInterface
{
    public function connect(array $params);


    public function fetchOne(StorageRequest $request);


    public function prepareFetch(StorageRequest $request);


    public function prepareBatchFetch(StorageRequest $request, $batchSize);
}