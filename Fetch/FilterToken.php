<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 17:05
 */

namespace Dappl\Fetch;


class FilterToken
{
	private $type;
	private $value;

	const OPEN_BRACKET = '(';
	const CLOSE_BRACKET = ')';
	const COMMAND = 'COMMAND'; // eg endswith
	const COMMA = ',';
	const OPERATOR = 'OPERATOR'; // eg eq, gt, lt
	const QUOTED_STRING = 'QUOTED_STRING';
	const STRING = 'STRING';
	const BOOL = 'BOOL';
	const BINARY = 'BINARY'; // eg AND/OR


	public function __construct($type, $value = null)
	{
		$this->type = $type;
		$this->value = $value;
	}


	public function getType()
	{
		return $this->type;
	}


	public function getValue()
	{
		return $this->value;
	}


	public static function getOperatorList()
	{
		return array('eq', 'gt', 'ge', 'lt', 'le', 'ne');
	}


	public static function getBinaryList()
	{
		return array('and', 'or');
	}


	public static function getCommandList()
	{
		return array('substringof', 'endswith', 'startswith');
	}


	public static function getBoolList()
	{
		return array('true', 'false');
	}


	public function __toString()
	{
		return $this->type . ' => ' . $this->value . PHP_EOL;
	}
}