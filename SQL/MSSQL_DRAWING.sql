SELECT geometry::STGeomFromText(
'POLYGON((0 0,0 1,1 1,1 0,0 0))',
0 );
SELECT geometry::STGeomFromText(
'MULTIPOLYGON(
((0 0,0 1,1 1,1 0,0 0)),
((2 0,2 1,3 1,3 0,2 0)))', 0 );