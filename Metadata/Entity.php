<?php

namespace Dappl\Metadata;

class Entity
{
    private $containerName;
    private $data;
    private $dataPropertiesCache;

    const DATATYPE_INT32 = 'Int32';
    const DATATYPE_STRING = 'String';
    const DATATYPE_DATETIME = 'DateTime';


    public function setContainerName($name)
    {
        $this->containerName = $name;
    }


    public function getContainerName()
    {
        return $this->containerName;
    }


    public function getEntityName()
    {
        // All entities should have a name
        if (!isset($this->data['shortName'])) {
            throw new \Exception('No entity name defined in: [' . serialize($this) . ']');
        }
        return $this->data['shortName'];
    }


    public function getDefaultResourceName()
    {
        // All entities should have a default resource name
        if (!isset($this->data['defaultResourceName'])) {
            throw new \Exception('No defaultResourceName defined in: [' . serialize($this) . ']');
        }
        return $this->data['defaultResourceName'];
    }


    public function setData(array $data)
    {
        // Current format metadata is stored in 'description' property
        if (!array_key_exists('description', $data)) {
            throw new \Exception('Metadata description missing');
        }
        $this->data = $data['description'];
    }


    /**
     * Returns the types
     */
    public function getPropertyDataType($propertyName)
    {
        $fields = $this->getPropertyFields($propertyName);
        if (!isset($fields['dataType'])) {
            throw new \Exception(sprintf('Could not find data type for property [%s] on entity: [%s]', $propertyName, $this->getEntityName()));
        }
        return $fields['dataType'];
    }


    public function getPropertyFields($propertyName)
    {
        if (!$this->dataPropertiesCache) {
            // Build cache
            if (is_array($this->data) &&
                array_key_exists('dataProperties', $this->data) &&
                is_array($this->data['dataProperties'])) {
                $this->dataPropertiesCache = array();
                foreach($this->data['dataProperties'] as $prop) {
                    $this->dataPropertiesCache[$prop['name']] = $prop;
                }
            } else {
                throw new \Exception(sprintf('Could not find property [%s] for entity: [%s] - data properties not defined in metadata', $propertyName, $this->getEntityName()));
            }
        }

        if (!array_key_exists($propertyName, $this->dataPropertiesCache)) {
            throw new \Exception(sprintf('Could not find property [%s] for entity: [%s]', $propertyName, $this->getEntityName()));
        }
        return $this->dataPropertiesCache[$propertyName];
    }


    public function hasNavigationProperty($navigationPropertyName)
    {
        // Yep, this is inefficient. It would be nice for this object to pre-render and cache all these things in a more fetchable way,
        // then serialize the whole object.
        // Or store the data in a more fetchable way (not in the breeze format) so the breeze fetch is more expensive but then this can then be cached.
        // (and breeze never asks for a single entity, it would be caching the whole set in one go.
        // Later decisions! :-) - (second is more work but seems better?)
        $result = $this->getNavigationPropertyFields($navigationPropertyName);
        return $result ? true : false;
    }


    public function getNavigationPropertyFields($navigationPropertyName)
    {
        $result = null;
        if (is_array($this->data) &&
            array_key_exists('navigationProperties', $this->data) &&
            is_array($this->data['navigationProperties'])) {
            foreach($this->data['navigationProperties'] as $prop) {
                if ($navigationPropertyName == $prop['name']) {
                    $result = $prop;
                    break;
                }
            }
        }
        return $result;
    }


    public function getKey()
    {
        // Not handling compound keys yet - not sure how they are defined and work with breeze yet.
        $result = null;
        if (is_array($this->data) &&
            array_key_exists('key', $this->data) &&
            isset($this->data['key']['propertyRef']['name'])) {
            $result = $this->data['key']['propertyRef']['name'];
        }
        return $result;
    }
} 