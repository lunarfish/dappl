<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 19:21
 */

namespace Dappl\Storage\Cursor;

use \Dappl\Storage\Graph\ResultCollection;


class BatchCursor
{
	private $cursor;
	private $batchSize;
    private $isDebugging;


	public function __construct(CursorInterface $cursor, $batchSize, $isDebugging)
	{
		$this->cursor = $cursor;
		$this->batchSize = $batchSize;
        $this->isDebugging = $isDebugging;
	}


	/**
	 * Populate the fetch result collection with data from the current cursor, upto the batch size
	 * @param FetchResultCollection $resultCollection
	 * @return FetchResultCollection
	 */
	public function getNextBatch(ResultCollection $resultCollection)
	{
		$i = $this->batchSize;
		while ($i > 0) {
			$result = $this->cursor->getNext();
			if (!$result) {
				break;
			}
			$resultCollection->addEntity($result);
			$i--;
		}

        if ($this->isDebugging) {
            echo sprintf('Fetched batch from: %s items: %d %s',
                $resultCollection->getNode()->getName(),
                count($resultCollection),
                PHP_EOL);
        }

		return $resultCollection;
	}


	public function hasMore()
	{
		return $this->cursor->hasMore();
	}
}