grammar Schema;

options {
  output = AST;
}

@header {
package org.apache.mahout.pig;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.HashMap;
}

@lexer::header {
package org.apache.mahout.pig;
}

@members {       
       private int position = 0;
       private Map<String, ArgumentEncoder> encoders = Maps.newHashMap();
     
       public Map<String, ArgumentEncoder> getEncoders() {
          return encoders;
       }

       public void reportError(RecognitionException e) {
         if (e instanceof FailedPredicateException) {
           throw new SchemaParseException("Variable multiply defined", e);
         } else {
           throw new SchemaParseException("Syntax error in schema: ", e);
         }
       }

}

schema 	:	{position = 0;} variable (COMMA! variable)*;

variable	:
          ID^ COLON! t=type[$ID.text, position++]
            {encoders.get($ID.text) == null}?
            {encoders.put($ID.text, $t.encoder);}
          | ID^ DOLLAR INTEGER COLON! t=type[$ID.text, Integer.parseInt($INTEGER.text)]
            {encoders.get($ID.text) == null}?
            {encoders.put($ID.text, $t.encoder);};

type [String name, int position] returns [ArgumentEncoder encoder]:	
          NUMERIC^ {$encoder = ArgumentEncoder.newNumericEncoder(position, $name);}
          | WORD^ {$encoder = ArgumentEncoder.newWordEncoder(position, $name);}
          | TEXT^ {$encoder = ArgumentEncoder.newTextEncoder(position, $name);}
          | TEXT^ LPAREN! className RPAREN! {$encoder = ArgumentEncoder.newTextEncoder(position, $name, $className.text);}
          ;
          
className	:	ID (DOT ID)*;

NUMERIC	:	'numeric';
WORD	:	'word';
TEXT	:	'text';
INTEGER:	('0'..'9')+;

LPAREN	:	'(';
RPAREN	:	')';
COLON	:	':';
ID  :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
    ;
DOT	:	'.';
COMMA	:	',';
DOLLAR	:	'$';

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

