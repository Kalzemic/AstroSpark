import math
import healpy as hp
from antlr4 import ParserRuleContext
from queryparser.adql import ADQLQueryTranslator

class QueryOptimizer():

    def _pixel_range_to_url(self,start: int, end: int, data_dir: str = "data") -> str:
        return f"{data_dir}/GaiaSource_{start:06d}-{end:06d}.csv.gz"
    
    def _pixels_to_urls(self, pixels) -> list[str]:
        urls = []
        for p in pixels:
            start = (int(p) // 3112) * 3112
            end = start + 3111
            url = self._pixel_range_to_url(start, end)
            if url not in urls:
                urls.append(url)
        return urls
    
    def _find_node(self, tree, rule_name, parser):
        
        if isinstance(tree, ParserRuleContext):
            if parser.ruleNames[tree.getRuleIndex()] == rule_name:
                return tree
        for i in range(tree.getChildCount()):
            result = self._find_node(tree.getChild(i), rule_name, parser)
            if result:
                return result
        return None
    
    def _find_all_nodes(self, tree, rule_name, parser, results):
        
        if isinstance(tree, ParserRuleContext):
            if parser.ruleNames[tree.getRuleIndex()] == rule_name:
                results.append(tree)
        for i in range(tree.getChildCount()):
            self._find_all_nodes(tree.getChild(i), rule_name, parser, results)
    
    def _get_leaf_value(self, tree):
        
        if tree.getChildCount() == 0:
            return tree.getText()
        for i in range(tree.getChildCount()):
            result = self._get_leaf_value(tree.getChild(i))
            if result:
                return result
            
    def _extract_circle(self, adt: ADQLQueryTranslator):
        
        circle = self._find_node(adt.tree, 'circle', adt.parser)
        if circle is None:
            return None
        ra = float(self._get_leaf_value(self._find_node(circle, 'coordinate1', adt.parser)))
        dec = float(self._get_leaf_value(self._find_node(circle, 'coordinate2', adt.parser)))
        radius = float(self._get_leaf_value(self._find_node(circle, 'radius', adt.parser)))
        return ra, dec, radius
    
    def _extract_point(self, adt: ADQLQueryTranslator):
        points = []
        self._find_all_nodes(adt.tree, 'point', adt.parser, points)
        for point in points:
            coord1 = self._find_node(point, 'coordinate1', adt.parser)
            coord2 = self._find_node(point, 'coordinate2', adt.parser)
            val1 = self._get_leaf_value(coord1)
            val2 = self._get_leaf_value(coord2)
            try:
                return float(val1), float(val2)
            except ValueError:
                continue
        return None
    
    def __call__(self, adt: ADQLQueryTranslator) -> list[str]:
        circle = self._extract_circle(adt)
        if circle is not None:
            ra, dec, radius = circle
        else:
            point = self._extract_point(adt)
            if point is not None:
                ra, dec = point
                radius = 5.0
            else:
                # no spatial constraint, caller must decide which partitions to load
                return None

        vec = hp.ang2vec(ra, dec, lonlat=True)
        pixels = hp.query_disc(nside=64, vec=vec, radius=math.radians(radius), inclusive=True)
        return self._pixels_to_urls(pixels)
