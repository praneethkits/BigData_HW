#!/bin/sh
exec scala "$0" "$@"
!#

val a = Array(1,2,3,2,3,4,5,2,1,2,3,4,3,4,5)
val r = sc.parallelize(a)
val newr = r.map(x => (x,1))
newr.reduceByKey(_+_).collect()
