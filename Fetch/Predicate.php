<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 17:01
 */

namespace Dappl\Fetch;


class Predicate
{
	private $property;
	private $operator;
	private $value;


	public function __construct($property, $operator, $value)
	{
		$this->property = $property;
		$this->operator = $operator;
		$this->value = $value;
	}


	/**
	 * Returns the property path this predicate should operate on, eg Locations/LookupCountrys/Description
	 * @param bool $filterPath if true the path is remove to just include the property name
	 * @return mixed
	 */
	public function getProperty($filterPath = false)
	{
		if ($filterPath) {
			$segments = explode('/', $this->property);
			return end($segments);
		} else {
			return $this->property;
		}
	}


	public function getOperator()
	{
		return $this->operator;
	}


	public function getValue()
	{
		return $this->value;
	}


	public function __toString()
	{
		return sprintf('(%s %s %s)', $this->property, $this->operator, $this->value);
	}
}