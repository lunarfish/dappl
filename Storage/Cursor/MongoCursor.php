<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 19:17
 */

namespace Dappl\Storage\Cursor;


class MongoCursor implements CursorInterface
{
	private $mongoCursor;


	public function __construct(\MongoCursor $cursor)
	{
		$this->mongoCursor = $cursor;
	}


	public function getNext()
	{
		$result = false;
		if ($this->mongoCursor->hasNext()) {
			$result = (object)$this->mongoCursor->getNext();
		}
		return $result;
	}


	public function hasMore()
	{
		return $this->mongoCursor->hasNext();
	}
}