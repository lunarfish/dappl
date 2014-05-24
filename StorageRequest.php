<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 16:21
 */

class StorageRequest
{
    private $defaultResourceName;
    private $filter;
    private $metadata;


    public function __construct($defaultResourceName, EntityMetadata $metadata)
    {
        $this->defaultResourceName = $defaultResourceName;
        $this->metadata = $metadata;
        $this->filter = array();
    }


    public function getDefaultResourceName()
    {
        return $this->defaultResourceName;
    }


    // Array for now, needs to be a predicate in future
    Public function setFilter(array $filter)
    {
        $this->filter = $filter;
    }


    public function addFilter(array $filter)
    {
        $this->filter = array_merge($this->filter, $filter);
    }


    public function getFilter()
    {
        return $this->filter;
    }


    public function getMetadata()
    {
        return $this->metadata;
    }
} 