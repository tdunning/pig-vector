grammar Formula;

options {
  output = AST;
}

@header {
package org.apache.mahout.pig;
}

@lexer::header {
package org.apache.mahout.pig;
}

@members {
  public void reportError(RecognitionException e) {
    if (e instanceof FailedPredicateException) {
      throw new FormulaParseException(e, "Invalid bias");
    } else {
      throw new FormulaParseException(e, "Syntax error in schema: ");
    }
  }
}

expression returns [EncodingSpec r]: s=sumExpression EOF  {$r = $s.r;};

sumExpression returns [EncodingSpec r]:
    f1=factor {$r = $f1.r;}
       (PLUS f2=factor {$r = EncodingSpec.add($r, $f2.r);})*
    ;

factor returns [EncodingSpec r]:
    a1=argument {$r = $a1.r;} (TIMES a2=argument {$r = EncodingSpec.interact($r, $a2.r);})*  ;
	
argument returns [EncodingSpec r]:
    LPAREN s1=sumExpression  RPAREN  {$r = $s1.r;}
    (UP_ARROW n = INTEGER {$r = EncodingSpec.pow($s1.r, Integer.parseInt($n.getText()));})?
    | ID {$r = EncodingSpec.atom($ID.text);}
    | bias {$r = EncodingSpec.atom("1");};

bias : i = INTEGER {"1".equals(i.getText())}?;

PLUS:       '+';
TIMES:      '*';
LPAREN:     '(';
RPAREN:     ')';
UP_ARROW:   '^';
INTEGER:	('0'..'9')+;
ID:         ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*;
DOLLAR:     '$';
WS       :           (' '|'\t'|'\f'|'\n'|'\r')+{ $channel=HIDDEN; };

