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
    private $isDebugging;
    private $driverParams;
    private $driverCache;


    public function __construct(array $params)
    {
        // Params will be current crm datastore_dev.yaml stuff describing connection details and driver to use
        if (!array_key_exists('driver_params', $params) || !is_array($params['driver_params'])) {
            throw new \Exception('driver_params are not defined');
        }
        $this->driverParams = $params['driver_params'];

        // @todo: instead provide logging instance to direct messages to
        $this->isDebugging = array_key_exists('debug', $params) && $params['debug'];

        $this->driverCache = array();
    }


    public function getDriver(StorageRequest $request)
    {
        // Get the container name from the metadata, which defines where we should fetch data from
        $containerName = $request->getMetadata()->getContainerName();
        if (!array_key_exists($containerName, $this->driverCache)) {
            // Check we have parameters for this container
            if (!array_key_exists($containerName, $this->driverParams)) {
                throw new \Exception(sprintf('No driver parameters defined for container: [%s]', $containerName));
            }

            // At the moment the driver type is embedded into the container name
            // @todo: change to a 'driver' parameter
            $bang = explode('.', $containerName);
            if (2 != count($bang)) {
                throw new \Exception(sprintf('Illegal container name: [%s], must be <driver name>.<container name>', $containerName));
            }

            $class = '\Dappl\Storage\Driver\\' . ucfirst($bang[0]) . 'Driver';
            $driver = new $class();
            $driver->connect($this->driverParams[$containerName]);
            $this->driverCache[$containerName] = $driver;
        }
        return $this->driverCache[$containerName];
    }


    public function fetchOne(StorageRequest $request)
    {
        $driver = $this->getDriver($request);
        return $driver->fetchOne($request);
    }


    public function prepareFetch(StorageRequest $request)
    {
        $driver = $this->getDriver($request);
        return $driver->prepareFetch($request);
    }


    public function prepareBatchFetch(StorageRequest $request, $batchSize)
    {
        $driver = $this->getDriver($request);
        return $driver->prepareBatchFetch($request, $batchSize);
    }
} 