import re

def fix_distance(sql: str) -> str:
    # replace spoint <-> spoint with haversine
    pattern = r"DEGREES\(spoint\(RADIANS\((\w+)\),\s*RADIANS\((\w+)\)\)\s*<->\s*spoint\(RADIANS\(([0-9.]+)\),\s*RADIANS\(([0-9.]+)\)\)\)"
    replacement = r"DEGREES(acos(sin(RADIANS(\2)) * sin(RADIANS(\4)) + cos(RADIANS(\2)) * cos(RADIANS(\4)) * cos(RADIANS(\1 - \3))))"
    return re.sub(pattern, replacement, sql)