<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 03/06/2014
 * Time: 17:03
 */

namespace Dappl\Fetch;


/**
 * Massive simplification - we are only dealing with the AND binary operator so we are not processing binary operations
 * on the predicates. Just parsing predicates and returning them in order of appearance.
 * To save on implementing the full recursive decent parser, save that fun for another day :-)
 *
 * Any 'OR' found will trigger an exception.
 *
 * Class RequestFilterPredicateParser
 */
class PredicateParser
{
	private $tokens;
	private $predicates;


	public function getPredicates(array $tokens)
	{
		// Set and reset token pointer
		$this->tokens = $tokens;
		reset($this->tokens);

		// Create output
		$this->predicates = array();

		// Iterate tokens and process in global scope
		while($this->hasMoreTokens()) {
			// Handle token
			$token = $this->getNextToken();
			switch($token->getType()) {
                case FilterToken::STRING:
                case FilterToken::COMMAND:
                    // If we are hitting a string/command here it's a single predicate with no parenthesis
                    $this->getPrevToken();
                    $this->eatPredicate();
                    break;

				case FilterToken::OPEN_BRACKET:
					$this->eatPredicate();
					break;

				case FilterToken::CLOSE_BRACKET:
				case FilterToken::BINARY:
					// do nothing, ignoring AND/OR processing for now.
					break;

				default:
					throw new \Exception('Found wrong token in global scope:' . $token);
					break;
			}
		}

		return $this->predicates;
	}


	private function pushPredicate(FilterToken $property, FilterToken $operator, FilterToken $value)
	{
		$predicate = new Predicate($property->getValue(), $operator->getValue(), $value->getValue());
		$this->predicates[] = $predicate;
	}


	private function eatPredicate()
	{
		// Get the next token
		if ($this->hasMoreTokens()) {
			$token = $this->getNextToken();
			switch($token->getType()) {
				case FilterToken::OPEN_BRACKET:
					// Wind back and do nothing, - when the recursive descendant parser is implemented here we will push a new scope onto the stack and pass processing to that
					$this->getPrevToken();
					break;

				case FilterToken::COMMAND:
					$this->eatCommandPredicate($token);
					break;

				case FilterToken::STRING:
					$this->eatPropertyPredicate($token);
					break;

				default:
					throw new \Exception(sprintf('Found illegal token: [%s] in method: %s from token: %s', $token, __METHOD__, $this->currentTokenIndex()));
			}
		} else {
			throw new \Exception('Ran out of tokens during ' . __METHOD__);
		}
	}


	/**
	 * Consume, validate and create a command predicate
	 * eg (endswith(PostCode,'XT') eq true)
	 */
	private function eatCommandPredicate($commandToken)
	{
		$expectedTypes = array(array(
			FilterToken::OPEN_BRACKET,
			FilterToken::STRING,
			FilterToken::COMMA,
			FilterToken::QUOTED_STRING,
			FilterToken::CLOSE_BRACKET,
			FilterToken::OPERATOR,
			FilterToken::BOOL
		), array(
			FilterToken::OPEN_BRACKET,
			FilterToken::QUOTED_STRING,
			FilterToken::COMMA,
			FilterToken::STRING,
			FilterToken::CLOSE_BRACKET,
			FilterToken::OPERATOR,
			FilterToken::BOOL
		));

		// If we are handling substringof, it has different expectations
		$mode = ('substringof' == $commandToken->getValue()) ? 1 : 0;

		// We already have the command token to start us off.
		$tokenList = array($commandToken);
		$start = $this->currentTokenIndex();

		// Eat tokens and validate against our expected list
		foreach($expectedTypes[$mode] as $expectedType) {
			if ($this->hasMoreTokens()) {
				// Check token
				$token = $this->getNextToken();
				$type = $token->getType();
				if ($type !== $expectedType) {
					throw new \Exception(sprintf('%s: Found incorrect token type: %s, expected: %s at %s processing %s' , __METHOD__, $type, $expectedType, $start, $commandToken));
				}

				// Store
				$tokenList[] = $token;
			} else {
				throw new \Exception(sprintf('%s: Ran out of tokens at %s processing %s'), __METHOD__, $start, $commandToken);
			}
		}

        // Eat the last token which must be either CLOSE_BRACKET or EOF
        if ($this->hasMoreTokens()) {
            // Check token
            $token = $this->getNextToken();
            $type = $token->getType();
            if (FilterToken::CLOSE_BRACKET !== $type) {
                throw new \Exception(sprintf('%s: Found incorrect final token type: %s, expected: %s at %s processing %s' , __METHOD__, $type, FilterToken::CLOSE_BRACKET, $start, $commandToken));
            }

            // Store
            $tokenList[] = $token;
        }

		// Create predicate from token list
		if ($mode) {
			$this->pushPredicate($tokenList[4], $tokenList[0], $tokenList[2]);
		} else {
			$this->pushPredicate($tokenList[2], $tokenList[0], $tokenList[4]);
		}
	}


	/**
	 * Consume, validate and create a property predicate
	 * eg (LocationID ge 1234)
	 */
	private function eatPropertyPredicate($propertyToken)
	{
		$expectedTypes = array(
			FilterToken::OPERATOR,
			FilterToken::STRING
		);

		// We already have the property token to start us off.
		$tokenList = array($propertyToken);
		$start = $this->currentTokenIndex();

		// Eat tokens and validate against our expected list
		foreach($expectedTypes as $i => $expectedType) {
			if ($this->hasMoreTokens()) {
				// Check token
				$token = $this->getNextToken();
				$type = $token->getType();

				// Fail if type is not expected type. Allow either string or quoted string for the second element
				if ($type !== $expectedType && (1 == $i) && ($type !== FilterToken::QUOTED_STRING)) {
					throw new \Exception(sprintf('%s: Found incorrect token type: %s, expected: %s at %s processing %s' , __METHOD__, $type, $expectedType, $start, $propertyToken));
				}

				// Store
				$tokenList[] = $token;
			} else {
				throw new \Exception(sprintf('%s: Ran out of tokens at %s processing %s', __METHOD__, $start, $propertyToken));
			}
		}

        // Eat the closing bracket or EOF
        if ($this->hasMoreTokens()) {
            // Check token
            $token = $this->getNextToken();
            $type = $token->getType();
            if (FilterToken::CLOSE_BRACKET != $type) {
                throw new \Exception(sprintf('%s: Expected close brackets at %s but found [%s]', __METHOD__, $start, $token->getValue()));
            }
        }

		// Create predicate from token list
		$this->pushPredicate($tokenList[0], $tokenList[1], $tokenList[2]);
	}


	private function getNextToken()
	{
		// Grab token
		$token = current($this->tokens);

		// Advance pointer
		next($this->tokens);
		return $token;
	}


	private function getPrevToken()
	{
		prev($this->tokens);
	}


	private function hasMoreTokens()
	{
		return null !== key($this->tokens);
	}


	private function currentTokenIndex()
	{
		return key($this->tokens);
	}
}
