<?php


/**
 * Class NavigationProperty
 */
class NavigationProperty {

    private $entityKey;
    private $relatedEntityKey;
    private $name;
    private $rawFields;


    public function __construct($navigationPropertyName, array $rawFields)
    {
        $this->name = $navigationPropertyName;
        $this->rawFields = $rawFields;
    }


    public function setKeys($entityKey, $relatedEntityKey)
    {
        $this->entityKey = $entityKey;
        $this->relatedEntityKey = $relatedEntityKey;
    }


    public function getEntityKey()
    {
        return $this->entityKey;
    }


    public function getRelatedEntityKey()
    {
        return $this->relatedEntityKey;
    }


    public function isScalar()
    {
        $result = false;
        if (array_key_exists('isScalar', $this->rawFields) && $this->rawFields['isScalar']) {
            $result = true;
        }
        return $result;
    }


    public function getName()
    {
        return $this->name;
    }


    public function getEntityTypeName($stripNamespace)
    {
        $result = null;
        if (array_key_exists('entityTypeName', $this->rawFields)) {
            $result = $this->rawFields['entityTypeName'];
            if ($stripNamespace && preg_match('/^([^:]+):#.*$/', $result, $matches)) {
                $result = $matches[1];
            }
        }
        return $result;
    }
}





class EntityMetadata {

    private $containerName;
    private $data;


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
            throw new Exception('No entity name defined in: [' . serialize($this) . ']');
        }
        return $this->data['shortName'];
    }


    public function getDefaultResourceName()
    {
        // All entities should have a default resource name
        if (!isset($this->data['defaultResourceName'])) {
            throw new Exception('No defaultResourceName defined in: [' . serialize($this) . ']');
        }
        return $this->data['defaultResourceName'];
    }


    public function setData(array $data)
    {
        // Current format metadata is stored in 'description' property
        if (!array_key_exists('description', $data)) {
            throw new Exception('Metadata description missing');
        }
        $this->data = $data['description'];
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