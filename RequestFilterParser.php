<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 01/06/2014
 * Time: 22:27
 */


/*
$f = array();

$f[] = <<< 'HEREDOC'
((((((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (substringof('a single quote ''yes''',Address1) eq true)) and (startswith(Address2,'double%22 single'' quotes') eq true)) and (Address3 ne 'double%22 only')) and (Address4 eq 'single'' only')) and (Town eq 'percent %2522 twentytwo')
HEREDOC;

$f[] = <<< 'HEREDOC'
((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (LookupCountys%2FLookupNations%2FNation eq 'Wales')
HEREDOC;

$f[] = <<< 'HEREDOC'
((LocationID ge 1234) and (endswith(PostCode,'XT') eq true)) and (Address1 eq 'single'' double" percent% hello')
HEREDOC;

$f[] = <<< 'HEREDOC'
(LocationID ge 1234) and (endswith(PostCode,'XT') eq true)
HEREDOC;

$f[] = <<< 'HEREDOC'
(((((((((LocationID eq 1234) and (Address1 eq 'XT')) and (substringof('single'' double%22 percent%25 hello',Address1) eq true)) and (startswith(Address1,'hello!!') eq true)) and (endswith(Address1,'hello!!') eq true)) and (Address1 gt 'hello!!')) and (Address1 ge 'hello!!')) and (Address1 lt 'hello!!')) and (Address1 le 'hello!!')) and (LookupCountyID ne 5)
HEREDOC;


echo 'TEST (property equals string value) predicate - will fail at the moment' . PHP_EOL;



$scanner = new RequestFilterTokenizer();
$parser = new RequestFilterPredicateParser();
foreach($f as $input) {
    // Create token list
    $tokens = $scanner->tokenize($input);

    // Output
//    echo $input . PHP_EOL;
//    $line = 0;
//    foreach($tokens as $token) {
//        echo "($line) $token";
//        $line++;
//    }
//    echo PHP_EOL . PHP_EOL;


    // Create predicate list
    $predicates = $parser->getPredicates($tokens);

    // Output
    echo $input . PHP_EOL;
    foreach($predicates as $predicate) {
        echo $predicate . PHP_EOL;
    }
    echo PHP_EOL . PHP_EOL;
}
*/


class RequestFilterPredicate
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


    public function getProperty()
    {
        return $this->property;
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


/**
 * Massive simplification - we are only dealing with the AND binary operator so we are not processing binary operations
 * on the predicates. Just parsing predicates and returning them in order of appearance.
 * To save on implementing the full recursive decent parser, save that fun for another day :-)
 *
 * Any 'OR' found will trigger an exception.
 *
 * Class RequestFilterPredicateParser
 */
class RequestFilterPredicateParser
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
                case RequestFilterToken::OPEN_BRACKET:
                    $this->eatPredicate();
                    break;

                case RequestFilterToken::CLOSE_BRACKET:
                case RequestFilterToken::BINARY:
                    // do nothing, ignoring AND/OR processing for now.
                    break;

                default:
                    throw new Exception('Found wrong token in global scope:' . $token);
                    break;
            }
        }

        return $this->predicates;
    }


    private function pushPredicate(RequestFilterToken $property, RequestFilterToken $operator, RequestFilterToken $value)
    {
        $predicate = new RequestFilterPredicate($property->getValue(), $operator->getValue(), $value->getValue());
        $this->predicates[] = $predicate;
    }


    private function eatPredicate()
    {
        // Get the next token
        if ($this->hasMoreTokens()) {
            $token = $this->getNextToken();
            switch($token->getType()) {
                case RequestFilterToken::OPEN_BRACKET:
                    // Wind back and do nothing, - when the recursive descendant parser is implemented here we will push a new scope onto the stack and pass processing to that
                    $this->getPrevToken();
                    break;

                case RequestFilterToken::COMMAND:
                    $this->eatCommandPredicate($token);
                    break;

                case RequestFilterToken::STRING:
                    $this->eatPropertyPredicate($token);
                    break;

                default:
                    throw new Exception(sprintf('Found illegal token: [%s] in method: %s from token: %s', $token, __METHOD__, $this->currentTokenIndex()));
            }
        } else {
            throw new Exception('Ran out of tokens during ' . __METHOD__);
        }
    }


    /**
     * Consume, validate and create a command predicate
     * eg (endswith(PostCode,'XT') eq true)
     */
    private function eatCommandPredicate($commandToken)
    {
        $expectedTypes = array(array(
            RequestFilterToken::OPEN_BRACKET,
            RequestFilterToken::STRING,
            RequestFilterToken::COMMA,
            RequestFilterToken::QUOTED_STRING,
            RequestFilterToken::CLOSE_BRACKET,
            RequestFilterToken::OPERATOR,
            RequestFilterToken::BOOL,
            RequestFilterToken::CLOSE_BRACKET
        ), array(
            RequestFilterToken::OPEN_BRACKET,
            RequestFilterToken::QUOTED_STRING,
            RequestFilterToken::COMMA,
            RequestFilterToken::STRING,
            RequestFilterToken::CLOSE_BRACKET,
            RequestFilterToken::OPERATOR,
            RequestFilterToken::BOOL,
            RequestFilterToken::CLOSE_BRACKET
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
                    throw new Exception(sprintf('%s: Found incorrect token type: %s, expected: %s at %s processing %s' , __METHOD__, $type, $expectedType, $start, $commandToken));
                }

                // Store
                $tokenList[] = $token;
            } else {
                throw new Exception(sprintf('%s: Ran out of tokens at %s processing %s'), __METHOD__, $start, $commandToken);
            }
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
            RequestFilterToken::OPERATOR,
            RequestFilterToken::STRING,
            RequestFilterToken::CLOSE_BRACKET
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
                if ($type !== $expectedType && (1 == $i) && ($type !== RequestFilterToken::QUOTED_STRING)) {
                    throw new Exception(sprintf('%s: Found incorrect token type: %s, expected: %s at %s processing %s' , __METHOD__, $type, $expectedType, $start, $propertyToken));
                }

                // Store
                $tokenList[] = $token;
            } else {
                throw new Exception(sprintf('%s: Ran out of tokens at %s processing %s'), __METHOD__, $start, $propertyToken);
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



class RequestFilterToken
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


/**
 * First step in parsing a breeze/OData request filter segment.
 * Converts filter string into an array of RequestFilterToken tokens which will be easier to parse and
 * convert into predicates
 *
 * Class RequestFilterTokenizer
 */
class RequestFilterTokenizer
{
    private $input;
    private $pos;
    private $tokens;


    /**
     * @param $input The input string to convert
     * @return array of RequestFilterToken objects
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
        $this->tokens[] = new RequestFilterToken($type, $value);
    }


    private function execute()
    {
        while(false !== ($char = $this->getNextChar())) {
            switch($char) {
                case '(':
                    $this->pushToken(RequestFilterToken::OPEN_BRACKET);
                    break;

                case ')':
                    $this->pushToken(RequestFilterToken::CLOSE_BRACKET);
                    break;

                case ',':
                    $this->pushToken(RequestFilterToken::COMMA);
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
        return ($char === '(') || ($char === ')') || ($char === ',') || ($char === ' ');
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
            $commandList = RequestFilterToken::getCommandList();
            $boolList = RequestFilterToken::getBoolList();
        }

        // Eat upto the next close bracket, space or comma
        $col = $this->pos;
        $char = $this->getNextChar();
        while((false !== $char) && !$this->isStringTerminator($char)) {
            $word .= $char;
            $char = $this->getNextChar();
        }

        // Did we fail?
        if (false !== $char) {
            // Rewind
            $this->skipBackChar();

            // Match command?
            $word = trim($word);
            $key = array_search($word, $commandList);
            if (false !== $key) {
                $this->pushToken(RequestFilterToken::COMMAND, $commandList[$key]);
                return;
            }

            // Match binary?
            $key = array_search($word, $boolList);
            if (false !== $key) {
                $this->pushToken(RequestFilterToken::BOOL, $boolList[$key]);
                return;
            }

            // Anything else must be a string. Convert any escaped forward slashes while we are here.
            $word = str_replace('%2F', '/', $word);
            $this->pushToken(RequestFilterToken::STRING, $word);
        } else {
            throw new Exception(sprintf('Failed to to eat command, string or bool from col: %d on input: [%s]', $col, $word));
        }
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
                throw new Exception(sprintf('Failed to to eat quoted string from col: %d on input: [%s]', $col, $word));
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
        $this->pushToken(RequestFilterToken::QUOTED_STRING, $word);
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
            $operatorList = RequestFilterToken::getOperatorList();
            $binaryList = RequestFilterToken::getBinaryList();
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
                $this->pushToken(RequestFilterToken::OPERATOR, $operatorList[$key]);
                return;
            }

            // Match binary?
            $key = array_search($word, $binaryList);
            if (false !== $key) {
                $this->pushToken(RequestFilterToken::BINARY, $binaryList[$key]);
                return;
            }
        }
        throw new Exception(sprintf('Failed to to eat operator or binary from col: %d on input: [%s]', $col, $word));
    }
}