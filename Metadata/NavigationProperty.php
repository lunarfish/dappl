<?php

namespace Dappl\Metadata;

/**
 * Class NavigationProperty
 */
class NavigationProperty
{
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