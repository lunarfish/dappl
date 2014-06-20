<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 17:06
 */

namespace Dappl\Fetch;


/**
 * First step in parsing a breeze/OData request filter segment.
 * Converts filter string into an array of FilterToken tokens which will be easier to parse and
 * convert into predicates
 *
 * Class FilterTokenizer
 */
class FilterTokenizer
{
	private $input;
	private $pos;
	private $tokens;


	/**
	 * @param $input The input string to convert
	 * @return array of FilterToken objects
	 */
	public function tokenize($input)
	{
		$this->input = $input;
		$this->pos = 0;
		$this->tokens = array();
		$this->execute();
		return $this->tokens;
	}


	private function pushToken($type, $value = null)
	{
		$this->tokens[] = new FilterToken($type, $value);
	}


	private function execute()
	{
		while(false !== ($char = $this->getNextChar())) {
			switch($char) {
				case '(':
					$this->pushToken(FilterToken::OPEN_BRACKET);
					break;

				case ')':
					$this->pushToken(FilterToken::CLOSE_BRACKET);
					break;

				case ',':
					$this->pushToken(FilterToken::COMMA);
					break;

				case ' ':
					$this->eatOperatorOrBinary($char);
					break;

				case "'":
					$this->eatQuotedString($char);
					break;

				default:
					$this->eatCommandStringOrBool($char);
					break;
			}
		}
	}


    /**
     * Returns the current pointer character, or false if we've hit the end.
     * @param bool $advancePointer - set to false to return character without advancing pointer
     * @return bool | char
     */
    private function getNextChar($advancePointer = true)
	{
		$char = false;
		if ($this->pos < strlen($this->input)) {
			$char = $this->input[$this->pos];
			if ($advancePointer) {
				$this->pos = $this->pos + 1;
			}
		}
		return $char;
	}


	private function skipBackChar()
	{
		if ($this->pos) {
			$this->pos = $this->pos - 1;
		}
	}


	private function isStringTerminator($char)
	{
		return ($char === '(') || ($char === ')') || ($char === ',') || ($char === ' ') || (false === $char);
	}


	/**
	 * This handles either a command (contains|startswith|etc) bool (true|false) or an unquoted string - a property path.
	 * String is terminated by a close bracket or comma
	 *
	 * @param $word
	 * @throws Exception
	 */
	private function eatCommandStringOrBool($word)
	{
		static $commandList, $boolList;
		if (!$commandList) {
			$commandList = FilterToken::getCommandList();
			$boolList = FilterToken::getBoolList();
		}

		// Eat upto the next close bracket, space or comma
		$col = $this->pos;
		$char = $this->getNextChar();
		while(!$this->isStringTerminator($char)) {
			$word .= $char;
			$char = $this->getNextChar();
		}

        if (false !== $char) {
            // If we are not finished, rewind
            $this->skipBackChar();
        }

        // Match command?
        $word = trim($word);
        $key = array_search($word, $commandList);
        if (false !== $key) {
            $this->pushToken(FilterToken::COMMAND, $commandList[$key]);
            return;
        }

        // Match binary?
        $key = array_search($word, $boolList);
        if (false !== $key) {
            $this->pushToken(FilterToken::BOOL, $boolList[$key]);
            return;
        }

        // Anything else must be a string. Convert any escaped forward slashes while we are here.
        $word = str_replace('%2F', '/', $word);
        $this->pushToken(FilterToken::STRING, $word);
	}


	/**
	 * Handle segment starting with a single quote. Scan until the last single quote, handling escaped single quotes.
	 * @param $word
	 * @throws Exception
	 */
	private function eatQuotedString($word)
	{
		$done = false;
		$col = $this->pos;
		$word = ''; // We won't return the string wrapped with the single quotes
		while(!$done) {
			// Eat upto the next single quote
			$char = $this->getNextChar();
			while((false !== $char) && ($char !== "'")) {
				$word .= $char;
				$char = $this->getNextChar();
			}

			// Disaster - hit the end without closing the string
			if (false === $char) {
				throw new \Exception(sprintf('Failed to to eat quoted string from col: %d on input: [%s]', $col, $word));
			}

			// Have we really finished, or is this single quote escaping another?
			$nextChar = $this->getNextChar(false); // don't advance character pointer
			if ("'" !== $nextChar) {
				$done = true;
			} else {
				// Advance char pointer past the second single quote and add the single quote in the output
				// to unescape it.
				$this->getNextChar();
				$word .= "'";
			}
		}

		// Create token
		$this->pushToken(FilterToken::QUOTED_STRING, $word);
	}


	/**
	 * Handle something starting with whitespace:
	 * ' and ', ' or ', ' eq ',
	 * @param $word
	 * @throws Exception
	 */
	private function eatOperatorOrBinary($word)
	{
		static $operatorList, $binaryList;
		if (!$operatorList) {
			$operatorList = FilterToken::getOperatorList();
			$binaryList = FilterToken::getBinaryList();
		}

		// Eat upto the next whitespace
		$col = $this->pos;
		$word = ''; // We don't want the initial space in the output
		$char = $this->getNextChar();
		while((false !== $char) && ($char !== ' ')) {
			$word .= $char;
			$char = $this->getNextChar();
		}

		// Did we fail?
		if (false !== $char) {
			// Match operator?
			$word = trim($word);
			$key = array_search($word, $operatorList);
			if (false !== $key) {
				$this->pushToken(FilterToken::OPERATOR, $operatorList[$key]);
				return;
			}

			// Match binary?
			$key = array_search($word, $binaryList);
			if (false !== $key) {
				$this->pushToken(FilterToken::BINARY, $binaryList[$key]);
				return;
			}
		}
		throw new \Exception(sprintf('Failed to to eat operator or binary from col: %d on input: [%s]', $col, $word));
	}
}