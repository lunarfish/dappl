<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 16/05/2014
 * Time: 16:27
 */


interface FetchCursor
{
    public function getNext();
    public function hasMore();
}


class MongoFetchCursor implements FetchCursor
{
    private $mongoCursor;


    public function __construct(MongoCursor $cursor)
    {
        $this->mongoCursor = $cursor;
    }


    public function getNext()
    {
        $result = false;
        if ($this->mongoCursor->hasNext()) {
            $result = $this->mongoCursor->getNext();
        }
        return $result;
    }


    public function hasMore()
    {
        return $this->mongoCursor->hasNext();
    }
}



class BatchFetchCursor
{
    private $cursor;
    private $batchSize;


    public function __construct(FetchCursor $cursor, $batchSize)
    {
        $this->cursor = $cursor;
        $this->batchSize = $batchSize;
    }


    public function getNextBatch()
    {
        $list = array();
        $i = $this->batchSize;
        while ($i > 0) {
            $result = $this->cursor->getNext();
            if (!$result) {
                break;
            }
            $list[] = $result;
            $i--;
        }
        return $list;
    }


    public function hasMore()
    {
        return $this->cursor->hasMore();
    }
}