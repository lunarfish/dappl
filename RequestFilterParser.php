<?php
/**
 * Created by PhpStorm.
 * User: rick
 * Date: 01/06/2014
 * Time: 22:27
 */

$f = array();
$f[] = <<< 'HEREDOC'
(LocationID ge 1234) and (endswith(PostCode,'XT') eq true)
HEREDOC;

$f[] = <<< 'HEREDOC'
((((((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (substringof('a single quote ''yes''',Address1) eq true)) and (startswith(Address2,'double%22 single'' quotes') eq true)) and (Address3 ne 'double%22 only')) and (Address4 eq 'single'' only')) and (Town eq 'percent %2522 twentytwo')
HEREDOC;

$f[] = <<< 'HEREDOC'
((LocationID eq 1234) and (endswith(PostCode,'XT') eq true)) and (LookupCountys%2FLookupNations%2FNation eq 'Wales')
HEREDOC;

$f[] = <<< 'HEREDOC'
((LocationID ge 1234) and (endswith(PostCode,'XT') eq true)) and (Address1 eq 'single'' double" percent% hello')
HEREDOC;



$scanner = new RequestFilterTokenizer();
foreach($f as $input) {
    $tokens = $scanner->tokenize($input);
    echo $input . PHP_EOL;
    foreach($tokens as $token) {
        echo $token;
    }
    echo PHP_EOL . PHP_EOL;
}




class RequestFilterParser
{


    public function tokenize($input)
    {

    }
}



class RequestFilterToken
{
    public $type;
    public $value;

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