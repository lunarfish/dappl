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
    private $select;


    public function __construct($defaultResourceName, EntityMetadata $metadata)
    {
        $this->defaultResourceName = $defaultResourceName;
        $this->metadata = $metadata;
        $this->filter = array();
        $this->select = array();
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


    public function addPredicate(RequestFilterPredicate $predicate)
    {
        // TEMP HACK!!!
        // Turn this predicate into a mongo filter

        // should just store this predicate in entirety and this driver code goes in a driver class.

        // TODO: convert variable types dependant on metadata (mongo only?)



        // TODO: need to support $in, essential for reporting!!!!


        $filter = null;
        $value = $predicate->getValue();
        $property = $predicate->getProperty(true);



        // Clean value according to metadata data type
        $dataType = $this->metadata->getPropertyDataType($property);
        switch($dataType) {
            case EntityMetadata::DATATYPE_INT32:
                $value = (int)$value;
                break;

            case EntityMetadata::DATATYPE_STRING:
                $value = (string)$value;
                break;

            case EntityMetadata::DATATYPE_DATETIME:
                $value = new MongoDate(strtotime($value));
                break;

            default:
                throw new Exception(sprintf('Unknown data type: [%s] found on property: [%s] of resource: [%s]', $dataType, $property, $this->defaultResourceName));
        }





        $operator = null;
echo sprintf('%s: Adding predicate: %s on path: %s', $this->defaultResourceName, $predicate, $property) . PHP_EOL;
        switch($predicate->getOperator()) {
            case 'eq':
                // WE CANNOT MERGE THIS WITH ANOTHER
                if (array_key_exists($property, $this->filter)) {
                    throw new Exception(sprintf('Cannot add equals predicate to property: [%s], a predicate already exists: [%s]', $property, json_encode($this->filter[$property])));
                }
                // Set direct value and hope we don't get another operator for this property
                // - or maybe equality takes priority and others are ignored?
                $this->filter[$property] = $value;
                return;
                break;

            case 'gt':
                $operator = '$gt';
                break;

            case 'ge':
                $operator = '$gte';
                break;

            case 'lt':
                $operator = '$lt';
                break;

            case 'le':
                $operator = '$lte';
                break;

            case 'ne':
                $operator = '$ne';
                break;

            case 'substringof':
                $operator = new \MongoRegex('/' . str_replace("'","", $value) . '/i');
                break;

            case 'endswith':
                $operator = new \MongoRegex('/' . str_replace("'","", $value) . '$/i');
                break;

            case 'startswith':
                $operator = new \MongoRegex('/^' . str_replace("'","", $value) . '/i');
                break;

            default:
                throw new Exception(sprintf('%s: unknown predicate operator: %s', __METHOD__, $predicate));
        }

        if (!array_key_exists($property, $this->filter)) {
            $this->filter[$property] = array();
        }

        if (is_array($this->filter[$property])) {
            $this->filter[$property][$operator] = $value;
        } else {
            throw new Exception(sprintf('Cannot add predicate: [%s], an equality value: [%s] exists already', $predicate, $this->filter[$property]));
        }
    }


    public function setSelect(array $select)
    {
        $this->select = $select;
    }


    public function addSelect($selectProperty)
    {
        $this->select[] = $selectProperty;
    }


    public function getSelect()
    {
        return $this->select ? $this->select : array();
    }


    public function getMetadata()
    {
        return $this->metadata;
    }
} 