#begin
ANALYZE TABLE t1;
ANALYZE TABLE t2, t3;
ANALYZE TABLE t1 UPDATE HISTOGRAM ON c1, c2;
ANALYZE TABLE t2 UPDATE HISTOGRAM ON c1 WITH 2 BUCKETS;
ANALYZE TABLE t2 DROP HISTOGRAM ON c1;
#end
