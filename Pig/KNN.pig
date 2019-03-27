points = LOAD '$input' USING PigStorage(',') AS (id:long, px:double, py:double);
qx = LOAD '$qx' AS (qx:double);
qy = LOAD '$qy' AS (qy:double);

pointsGroup = GROUP points BY ($0,$1,$2);

uniquePoints = FOREACH pointsGroup 
{
	unique = LIMIT points 1;
	GENERATE FLATTEN(unique);
};

distance = FOREACH uniquePoints
{
	dist = SQRT(((px - (double)'$qx') * (px - (double)'$qx')) + ((py - (double)'$qy') * (py - (double)'$qy')));
	GENERATE id, px, py, dist;
};

sorted = ORDER distance BY ($3);
result = LIMIT sorted (int)$k;

DUMP result;