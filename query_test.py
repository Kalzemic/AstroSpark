from queryparser.adql import ADQLQueryTranslator

raw = "SELECT source_id, ra, dec FROM gaiadr3.gaia_source WHERE 1=CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 180, 0, 1))"

adt = ADQLQueryTranslator(raw)

adt.parse()

print(adt.tree.toStringTree(recog=adt.parser))

print(type(adt.tree))

print()
print()
from antlr4 import *

def find_node(tree, rule_name, parser):
    if hasattr(tree, 'getRuleIndex'):
        if parser.ruleNames[tree.getRuleIndex()] == rule_name:
            return tree
    for i in range(tree.getChildCount()):
        result = find_node(tree.getChild(i), rule_name, parser)
        if result:
            return result
    return None

circle = find_node(adt.tree, 'circle', adt.parser)
print(circle.toStringTree(recog=adt.parser))


print()
print() 

def get_leaf_value(tree):
    if tree.getChildCount() == 0:
        return tree.getText()
    for i in range(tree.getChildCount()):
        result = get_leaf_value(tree.getChild(i))
        if result:
            return result

ra_node = find_node(circle, 'coordinate1', adt.parser)
dec_node = find_node(circle, 'coordinate2', adt.parser)
radius_node = find_node(circle, 'radius', adt.parser)

print(get_leaf_value(ra_node))
print(get_leaf_value(dec_node))
print(get_leaf_value(radius_node))