<?php

namespace Dappl\Storage\Cursor;


/**
 * Interface FetchCursor
 *
 * A base interface to be implemented by cursor objects for various data source types (MySQL, Mongo, etc)
 */
interface CursorInterface
{
	/**
	 * Returns a data object on success or false on fail
	 * @return object | false
	 */
	public function getNext();

	/**
	 * @return bool - true if the cursor has more data to fetch
	 */
	public function hasMore();
}