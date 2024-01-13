
# spark-abtest

spark-abtest provides a series of concise, performant and useful functions for data scientist/analyst to analysis abtest on Apache Spark.

- prop_test(y, t, confLevel = 0.95, alternative = 'two-sided', varEqual = false, removeNaN = true)
- ttest(y, t, confLevel = 0.95, alternative = 'two-sided', varEqual = false, removeNaN = true)
- cuped(y, t, x, confLevel = 0.95, alternative = 'two-sided', varEqual = false, removeNaN = true)

- ols(y, X, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- anova(y, t, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- ancova1(y, t, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- ancova2(y, t, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)

- cluster_ols(y, X, cid, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- cluster_anova(y, t, x, cid, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- cluster_ancova1(y, t, x, cid, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)
- cluster_ancova2(y, t, x, cid, confLevel = 0.95, stdErrorType = 'const', removeNaN = true)

- avg_rank()

## example

WIP



