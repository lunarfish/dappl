<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 19:41
 */

namespace Dappl\Storage\Graph;


interface ResultProcessorInterface
{
	/**
	 * Method to perform 2 critical tasks:
	 * 1. To filter out any fetched data that should not be part of the returned result for this node (assuming AND on all predicates)
	 * 2. To combine the fetched data into one result collection (either expand or projection)
	 *
	 * @param array $childResults - and array of completed FetchResultCollection objects, from each child node
	 * @param ResultCollection $nodeResultCollection - the completed results for the node
	 * @return ResultCollection - a new FetchResultCollection containing the filtered and combined results
	 */
	public function combineNodeAndChildNodeResults(array $childResults, ResultCollection $nodeResultCollection);
}