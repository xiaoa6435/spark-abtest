

# spark-abtest

spark-abtest provides a series of concise, performant and useful functions for data scientist/analyst to analysis A/B TEST(abtest) on Apache Spark.

CI: [![GitHub Build Status](https://github.com/xiaoa6435/spark-abtest/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/xiaoa6435/spark-abtest/actions/workflows/build_and_test.yml)

# setup

spark-abtest is a SparkSessionExtensions, you can use it in most spark
clients like spark-sql/spark-shell/spark-submit/pyspark/spark-connect
with some extra configurations.


    --conf spark.jars=target/scala-2.12/spark-abtest-0.1.0-3.5.0_2.12.jar \ 
    --conf spark.sql.extensions=org.apache.spark.sql.extra.AbtestExtensions

you can download corresponding spark and scala version jar from
[releases](https://github.com/xiaoa6435/spark-abtest/releases).

for example, in spark-shell

    $SPARK_HOME/bin/spark-shell \
      --conf spark.jars=target/scala-2.12/spark-abtest-0.1.0-3.5.0_2.12.jar \
      --conf spark.sql.extensions=org.apache.spark.sql.extra.AbtestExtensions

# all functions

-   prop_test(y, t, confLevel = 0.95, alternative = ‘two-sided’,
    varEqual = false, removeNaN = true, correct = true)

-   ttest(y, t, confLevel = 0.95, alternative = ‘two-sided’, varEqual =
    false, removeNaN = true)

-   cuped(y, t, x, confLevel = 0.95, alternative = ‘two-sided’, varEqual
    = false, removeNaN = true)

-   ols(y, X, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
    true)

-   anova(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
    true)

-   ancova1(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
    true)

-   ancova2(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
    true)

-   cluster_ols(y, X, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
    removeNaN = true)

-   cluster_anova(y, t, x, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
    removeNaN = true)

-   cluster_ancova1(y, t, x, cid, confLevel = 0.95, stdErrorType =
    ‘HC1’, removeNaN = true)

-   cluster_ancova2(y, t, x, cid, confLevel = 0.95, stdErrorType =
    ‘HC1’, removeNaN = true)

-   avg_rank(): like rank(), but return average rank rather than min
    rank when ties

except avg_rank(window rank), all functions is aggregate function.

## input

-   y: metrics variable, double type, prop_test also accept boolean
    metrics col
-   t: treat variable
    -   two arm(A/B test): numeric type of 0/1, or boolean type
    -   multi arm(A/B/C… test):
        -   wrapped by `factor(t, array('t0', 't1', 't2'))`, array is
            different values of t
        -   the first is base group
        -   ttest does’t accept multi treatment
-   x: control variable, double type
-   confLevel: confidence level of the interval, default value is 0.95
-   alternative: a character string specifying the alternative
    hypothesis, must be one of “two.sided” (default), “greater” or
    “less”. You can specify just the initial letter.
-   varEqual: is used to estimate the variance otherwise the Welch (or
    Satterthwaite) approximation to the degrees of freedom is used
-   removeNaN: if true, any of y/t/x contains nan, corresponding row
    will be removed, like na.action = ‘omit’
-   stdErrorType: a character string specifying the estimation type, for
    ols/anova/ancova1/ancova2, default is const, for
    cluster_ols/anova/ancova1/ancova2, default is HC1

## output

-   two arm(A/B test): return a struct with some of the fields:
    -   delta: the estimated difference in means of y
    -   stderr: the standard error of the mean (difference), used as
        denominator in the t-statistic formula
    -   tvalue: the value of the t-statistic, equal to delta / tvalue
    -   pvalue: the p-value for the test
    -   lower: lower confidence limits for delta
    -   upper upper confidence limits for delta
    -   y0: mean of y in treat group (t = 0)
    -   y1: mean of y in control group (t = 1)
    -   x0: mean of x in treat group (t = 0)
    -   x1: mean of x in treat group (t = 0)
    -   theta: for cuped only
    -   my: mean of y in corresponding treat group
    -   mx: mean of x in corresponding treat group
-   multi arm(A/B/C… test): return an array of struct for each treat

# example

## init

``` python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars", "spark-abtest-0.1.0-3.5.0_2.12.jar")
    .config("spark.sql.extensions", "org.apache.spark.sql.extra.AbtestExtensions")
    .getOrCreate()
)
```

``` python
N = 100000
theta = 0.4
sdf = spark.sql(f"""
select user_id, t, mt, y, x
       ,x * {theta} + randn() as y2 -- continuous response variable, and correlation with x
  from (
       select id as user_id
              ,if(rand() < 0.5, 1.0, 0.0) as t -- treat/control group = 0.5:0.5
              ,case when rand() < 0.4 then 't0'
                    when rand() < 0.7 then 't1' 
                    else 't2' end as mt -- multi treat: base/t1/t2 = 0.4:0.3:0.3
              ,if(rand() < 0.1, 1.0, 0.0) as y --0/1 response variable, like is_visit, is_click...
              ,randn() as x --control variable
         from range({N})      
       ) t1     
""")
sdf.createOrReplaceTempView("sdf")
pdf = sdf.toPandas() # local pandas dataframe
pdf.head(5)
```

<!--suppress HtmlUnknownAttribute -->
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">user_id</th>
<th data-quarto-table-cell-role="th">t</th>
<th data-quarto-table-cell-role="th">mt</th>
<th data-quarto-table-cell-role="th">y</th>
<th data-quarto-table-cell-role="th">x</th>
<th data-quarto-table-cell-role="th">y2</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0</td>
<td>1.0</td>
<td>t0</td>
<td>0.0</td>
<td>-0.64626599</td>
<td>1.54772415</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>1</td>
<td>0.0</td>
<td>t0</td>
<td>0.0</td>
<td>-0.85735783</td>
<td>1.62497210</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>2</td>
<td>0.0</td>
<td>t0</td>
<td>0.0</td>
<td>0.63050955</td>
<td>-0.73287976</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>3</td>
<td>1.0</td>
<td>t0</td>
<td>0.0</td>
<td>0.98298468</td>
<td>0.44892532</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">4</td>
<td>4</td>
<td>0.0</td>
<td>t1</td>
<td>0.0</td>
<td>-0.83978164</td>
<td>-1.26275208</td>
</tr>
</tbody>
</table>

</div>

### 0/1 response: prop test/chisq test

#### two arm

``` python
spark.sql("select prop_test(y, t) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">estimate</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">chisq</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>[0.09937538912654897, 0.10111732956242904]</td>
<td>0.36465656</td>
<td>-0.00200086</td>
<td>0.00548474</td>
<td>0.82179266</td>
</tr>
</tbody>
</table>

</div>

``` python
import pandas as pd
from scipy.stats import chi2_contingency
observed = pd.crosstab(pdf['y'], pdf['t'])
chi2, p, dof, expected = chi2_contingency(observed)
(p, chi2)
```

    (0.3646565557469007, 0.8217926576217617)

in addition to p-values and chisq, spark-abtest also provides confidence
intervals(for more detail, refer to
[prop.test](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/prop.test.html)
or
[chisq.test](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/chisq.test.html)
function in R) and mean response ratio for each treat.

#### multi arm

``` python
spark.sql("select prop_test(y, factor(mt, array('t0', 't1', 't2'))) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">estimate</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">chisq</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>[0.10143841774048547, 0.10020523100420008, 0.0...</td>
<td>0.38456362</td>
<td>NaN</td>
<td>NaN</td>
<td>1.91129209</td>
</tr>
</tbody>
</table>

</div>

``` python
observed = pd.crosstab(pdf['y'], pdf['mt'])
chi2, p, dof, expected = chi2_contingency(observed)
(p, chi2)
```

    (0.38456361940111916, 1.9112920872850279)

### continuous response: ttest

#### two arm and unequal variance, aka [Welch’s t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)

``` python
spark.sql("select ttest(y2, t) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">y0</th>
<th data-quarto-table-cell-role="th">y1</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00240728</td>
<td>0.00683655</td>
<td>-0.35211824</td>
<td>0.72475033</td>
<td>-0.01580684</td>
<td>0.01099229</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
</tr>
</tbody>
</table>

</div>

``` python
from scipy import stats
x = pdf[pdf['t'] == 1.0]['y2']
y = pdf[pdf['t'] == 0.0]['y2']
ret = stats.ttest_ind(x, y, equal_var=False)
ci = ret.confidence_interval(0.95)
f"tvalue: {ret.statistic:.6f}, pvalue: {ret.pvalue:.6f}, lower: {ci.low:.6f}, upper: {ci.high:.6f}"
```

    'tvalue: -0.352118, pvalue: 0.724750, lower: -0.015807, upper: 0.010992'

#### two arm and equal variance

##### spark-abtest

``` python
spark.sql("select ttest(y2, t, 0.95, 'two-sided', true) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">y0</th>
<th data-quarto-table-cell-role="th">y1</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00240728</td>
<td>0.00683657</td>
<td>-0.35211763</td>
<td>0.72475079</td>
<td>-0.01580686</td>
<td>0.01099231</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
</tr>
</tbody>
</table>

</div>

this actually equal to `anova` or `ols`

``` python
spark.sql("select anova(y2, t) as ret from sdf").select("ret.*").toPandas() # inline expand array of struct
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00240728</td>
<td>0.00683657</td>
<td>-0.35211763</td>
<td>0.72475079</td>
<td>-0.01580686</td>
<td>0.01099231</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select inline(ols(y2, array(1.0, t))) from sdf").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00010504</td>
<td>0.00484427</td>
<td>-0.02168431</td>
<td>0.98269982</td>
<td>-0.00959976</td>
<td>0.00938967</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>-0.00240728</td>
<td>0.00683657</td>
<td>-0.35211763</td>
<td>0.72475079</td>
<td>-0.01580686</td>
<td>0.01099231</td>
</tr>
</tbody>
</table>

</div>

##### python scipy.stat

``` python
from scipy import stats
x = pdf[pdf['t'] == 1.0]['y2']
y = pdf[pdf['t'] == 0.0]['y2']
ret = stats.ttest_ind(x, y)
ci = ret.confidence_interval(0.95)
f"tvalue: {ret.statistic:.6f}, pvalue: {ret.pvalue:.6f}, lower: {ci.low:.6f}, upper: {ci.high:.6f}"
```

    'tvalue: -0.352118, pvalue: 0.724751, lower: -0.015807, upper: 0.010992'

#### mannwhitneyua test

``` python
from scipy.stats import mannwhitneyu
_, p = mannwhitneyu(x, y)
print(p)

spark.sql("""
select ttest(y2_rank, t).pvalue as mannwhitneyu_pvalue
  from (
       select avg_rank() over (order by y2) as y2_rank
              ,t
         from sdf
       ) t1
""").toPandas()
```

    0.6348526173470306

    24/01/23 19:43:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    24/01/23 19:43:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    24/01/23 19:43:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
    24/01/23 19:43:44 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">mannwhitneyu_pvalue</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0.63485152</td>
</tr>
</tbody>
</table>

</div>

#### multi arm: anova

ttest is not suitable for more than two treatment groups, in
spark-abtest, use anova instead

``` python
spark.sql("select inline(anova(y2, factor(mt, array('t0', 't1', 't2')))) from sdf").toPandas() # inline expand array of struct
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00320884</td>
<td>0.00540173</td>
<td>-0.59403883</td>
<td>0.55248749</td>
<td>-0.01379615</td>
<td>0.00737848</td>
<td>-0.00320884</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>0.00752212</td>
<td>0.00755395</td>
<td>0.99578717</td>
<td>0.31935597</td>
<td>-0.00728352</td>
<td>0.02232777</td>
<td>0.00431329</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>-0.00696294</td>
<td>0.00969043</td>
<td>-0.71853771</td>
<td>0.47242748</td>
<td>-0.02595606</td>
<td>0.01203018</td>
<td>-0.01017177</td>
</tr>
</tbody>
</table>

</div>

``` python
import statsmodels.api as sm
from statsmodels.formula.api import ols

anova = ols('y2 ~ C(mt)', data=pdf).fit().summary()
anova.tables[1]
```

### Variance Reduction: cuped/ancova1/ancova2

refer
[deep-dive-into-variance-reduction](https://www.microsoft.com/en-us/research/group/experimentation-platform-exp/articles/deep-dive-into-variance-reduction/)
or [alexdeng book chapter
10](https://alexdeng.github.io/causal/sensitivity.html#vrreg)

``` python
import pandas as pd
pd.set_option('display.float_format', lambda x: f'{x:,.8f}')
spark.sql("select ttest(y2, t) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">y0</th>
<th data-quarto-table-cell-role="th">y1</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00240728</td>
<td>0.00683655</td>
<td>-0.35211824</td>
<td>0.72475033</td>
<td>-0.01580684</td>
<td>0.01099229</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select cuped(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">y0</th>
<th data-quarto-table-cell-role="th">y1</th>
<th data-quarto-table-cell-role="th">x0</th>
<th data-quarto-table-cell-role="th">x1</th>
<th data-quarto-table-cell-role="th">theta</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00250543</td>
<td>0.00633984</td>
<td>-0.39518820</td>
<td>0.69270484</td>
<td>-0.01493143</td>
<td>0.00992058</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
<td>0.00021016</td>
<td>0.00045288</td>
<td>0.40439612</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select ancova1(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
<th data-quarto-table-cell-role="th">mx0</th>
<th data-quarto-table-cell-role="th">mx</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00250543</td>
<td>0.00633990</td>
<td>-0.39518449</td>
<td>0.69270758</td>
<td>-0.01493155</td>
<td>0.00992069</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
<td>0.00021016</td>
<td>0.00045288</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select ancova2(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
<th data-quarto-table-cell-role="th">mx0</th>
<th data-quarto-table-cell-role="th">mx</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.00250543</td>
<td>0.00633993</td>
<td>-0.39518271</td>
<td>0.69270890</td>
<td>-0.01493161</td>
<td>0.00992075</td>
<td>-0.00010504</td>
<td>-0.00251232</td>
<td>0.00021016</td>
<td>0.00045288</td>
</tr>
</tbody>
</table>

</div>

## randomization unit \> analysis unit: delta method or cluster-robust standard error

when randomization unit larger than analysis unit, usually we need use
[delta method](https://arxiv.org/pdf/1803.06336) or [cluster-robust
variance estimator](https://arxiv.org/abs/2105.14705). Functions
prefixed with \`cluster\`\` can handle these problems.

### sample data

randomization unit is user_id and analysis unit is user_id + visit_id

``` python
import pandas as pd
import numpy as np

num_user = 1000
lambda_ = 10
pcdf_ = pd.DataFrame({
    "user_id": range(num_user),  # each user may have multi visit
    # treat/control group = 0.5:0.5
    "num_visit": np.random.poisson(lambda_, size=num_user),
    "t": np.random.choice([0.0, 1.0], p=[0.5, 0.5], size=num_user),
    "mt": np.random.choice(
        ['t0', 't1', 't2'],
        p=[0.4, 0.3, 0.3],
        size=num_user
    ),  # multi treat: base/t1/t2 = 0.4:0.3:0.3,
    # 0/1 respone variable, like is_visti, is_click...
    "mp": np.random.uniform(size=num_user),
    "mx": np.random.randn(num_user),  # control variable
})


def visit_df(idf_):
    idf = idf_.iloc[0]
    num_visit = idf['num_visit']
    y = np.random.choice(
        [0.0, 1.0], p=[1.0 - idf['mp'], idf['mp']], size=num_visit)
    x = np.random.normal(idf['mx'], size=num_visit)
    my2 = idf['mx'] * theta + np.random.randn()
    y2 = np.random.normal(my2, size=num_visit) 
    ret = pd.DataFrame({
        "user_id": idf["user_id"],
        "visit_id": range(num_visit),
        "t": idf["t"],
        "mt": idf["mt"],
        "y": y,
        "x": x,
        "y2": y2
    })
    return ret


pcdf = pcdf_.groupby("user_id", group_keys=False).apply(visit_df)
scdf = spark.createDataFrame(pcdf)
scdf.createOrReplaceTempView("scdf")
pcdf.head(5)
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">user_id</th>
<th data-quarto-table-cell-role="th">visit_id</th>
<th data-quarto-table-cell-role="th">t</th>
<th data-quarto-table-cell-role="th">mt</th>
<th data-quarto-table-cell-role="th">y</th>
<th data-quarto-table-cell-role="th">x</th>
<th data-quarto-table-cell-role="th">y2</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0</td>
<td>0</td>
<td>0.00000000</td>
<td>t0</td>
<td>1.00000000</td>
<td>-2.00802713</td>
<td>-1.70662868</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>0</td>
<td>1</td>
<td>0.00000000</td>
<td>t0</td>
<td>1.00000000</td>
<td>-2.74050928</td>
<td>-0.55966320</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>0</td>
<td>2</td>
<td>0.00000000</td>
<td>t0</td>
<td>1.00000000</td>
<td>-2.34139859</td>
<td>-1.58488664</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>0</td>
<td>3</td>
<td>0.00000000</td>
<td>t0</td>
<td>1.00000000</td>
<td>-1.97814922</td>
<td>-1.20993685</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">4</td>
<td>0</td>
<td>4</td>
<td>0.00000000</td>
<td>t0</td>
<td>1.00000000</td>
<td>-2.32212300</td>
<td>-1.17054778</td>
</tr>
</tbody>
</table>

</div>

### 0/1 or continuous response: cluster_ttest

``` python
spark.sql("select cluster_ttest(y2, t, user_id) as ret from scdf").select("ret.*").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0.01441253</td>
<td>0.07627543</td>
<td>0.18895374</td>
<td>0.85013291</td>
<td>-0.13510268</td>
<td>0.16392773</td>
<td>-0.03379021</td>
<td>-0.01937769</td>
</tr>
</tbody>
</table>

</div>

#### delta method with variance reduction

``` python
spark.sql("select cluster_ancova1(y2, t, x, user_id) as ret from scdf").select("ret.*").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
<th data-quarto-table-cell-role="th">mx0</th>
<th data-quarto-table-cell-role="th">mx</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0.00309024</td>
<td>0.07224579</td>
<td>0.04277391</td>
<td>0.96588261</td>
<td>-0.13852608</td>
<td>0.14470655</td>
<td>-0.03379021</td>
<td>-0.01937769</td>
<td>-0.06156514</td>
<td>-0.01019905</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select cluster_ancova2(y2, t, x, user_id) as ret from scdf").select("ret.*").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
<th data-quarto-table-cell-role="th">my0</th>
<th data-quarto-table-cell-role="th">my</th>
<th data-quarto-table-cell-role="th">mx0</th>
<th data-quarto-table-cell-role="th">mx</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>0.00312417</td>
<td>0.07226738</td>
<td>0.04323066</td>
<td>0.96551853</td>
<td>-0.13853445</td>
<td>0.14478279</td>
<td>-0.03379021</td>
<td>-0.01937769</td>
<td>-0.06156514</td>
<td>-0.01019905</td>
</tr>
</tbody>
</table>

</div>

### ols/cluster_ols

``` python
spark.sql("select inline(ols(y2, array(1.0, t, x, t * x))) from scdf").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.01991035</td>
<td>0.02028927</td>
<td>-0.98132440</td>
<td>0.32645656</td>
<td>-0.05968141</td>
<td>0.01986070</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>0.00272057</td>
<td>0.02928153</td>
<td>0.09291066</td>
<td>0.92597640</td>
<td>-0.05467713</td>
<td>0.06011826</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>0.22544998</td>
<td>0.01383548</td>
<td>16.29505891</td>
<td>0.00000000</td>
<td>0.19832965</td>
<td>0.25257031</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>-0.01093015</td>
<td>0.02040205</td>
<td>-0.53573755</td>
<td>0.59215188</td>
<td>-0.05092228</td>
<td>0.02906199</td>
</tr>
</tbody>
</table>

</div>

``` python
import statsmodels.api as sm
import statsmodels.formula.api as smf

ret = smf.ols(formula='y2 ~ t * x', data=pcdf).fit().summary().tables[1]
pd.DataFrame(ret)
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">0</th>
<th data-quarto-table-cell-role="th">1</th>
<th data-quarto-table-cell-role="th">2</th>
<th data-quarto-table-cell-role="th">3</th>
<th data-quarto-table-cell-role="th">4</th>
<th data-quarto-table-cell-role="th">5</th>
<th data-quarto-table-cell-role="th">6</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td></td>
<td>coef</td>
<td>std err</td>
<td>t</td>
<td>P&gt;|t|</td>
<td>[0.025</td>
<td>0.975]</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>Intercept</td>
<td>-0.0199</td>
<td>0.020</td>
<td>-0.981</td>
<td>0.326</td>
<td>-0.060</td>
<td>0.020</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>t</td>
<td>0.0027</td>
<td>0.029</td>
<td>0.093</td>
<td>0.926</td>
<td>-0.055</td>
<td>0.060</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>x</td>
<td>0.2254</td>
<td>0.014</td>
<td>16.295</td>
<td>0.000</td>
<td>0.198</td>
<td>0.253</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">4</td>
<td>t:x</td>
<td>-0.0109</td>
<td>0.020</td>
<td>-0.536</td>
<td>0.592</td>
<td>-0.051</td>
<td>0.029</td>
</tr>
</tbody>
</table>

</div>

``` python
spark.sql("select inline(cluster_ols(y2, array(1.0, t, x, t * x), user_id)) from scdf").toPandas()
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">delta</th>
<th data-quarto-table-cell-role="th">stderr</th>
<th data-quarto-table-cell-role="th">tvalue</th>
<th data-quarto-table-cell-role="th">pvalue</th>
<th data-quarto-table-cell-role="th">lower</th>
<th data-quarto-table-cell-role="th">upper</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td>-0.01991035</td>
<td>0.05397288</td>
<td>-0.36889552</td>
<td>0.71221342</td>
<td>-0.12570808</td>
<td>0.08588737</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>0.00272057</td>
<td>0.07213233</td>
<td>0.03771632</td>
<td>0.96991462</td>
<td>-0.13867333</td>
<td>0.14411447</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>0.22544998</td>
<td>0.03126138</td>
<td>7.21177413</td>
<td>0.00000000</td>
<td>0.16417138</td>
<td>0.28672857</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>-0.01093015</td>
<td>0.04018747</td>
<td>-0.27197896</td>
<td>0.78564382</td>
<td>-0.08970569</td>
<td>0.06784539</td>
</tr>
</tbody>
</table>

</div>

``` python
import statsmodels.api as sm
import statsmodels.formula.api as smf

fit = smf.ols(formula='y2 ~ t * x', data=pcdf).fit()
ret = fit.get_robustcov_results(cov_type='cluster', groups=pcdf['user_id']).summary().tables[1]
pd.DataFrame(ret)
```

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>

<table class="dataframe" data-quarto-postprocess="true" data-border="1">
<thead>
<tr class="header" style="text-align: right;">
<th data-quarto-table-cell-role="th"></th>
<th data-quarto-table-cell-role="th">0</th>
<th data-quarto-table-cell-role="th">1</th>
<th data-quarto-table-cell-role="th">2</th>
<th data-quarto-table-cell-role="th">3</th>
<th data-quarto-table-cell-role="th">4</th>
<th data-quarto-table-cell-role="th">5</th>
<th data-quarto-table-cell-role="th">6</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td data-quarto-table-cell-role="th">0</td>
<td></td>
<td>coef</td>
<td>std err</td>
<td>t</td>
<td>P&gt;|t|</td>
<td>[0.025</td>
<td>0.975]</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">1</td>
<td>Intercept</td>
<td>-0.0199</td>
<td>0.054</td>
<td>-0.369</td>
<td>0.712</td>
<td>-0.126</td>
<td>0.086</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">2</td>
<td>t</td>
<td>0.0027</td>
<td>0.072</td>
<td>0.038</td>
<td>0.970</td>
<td>-0.139</td>
<td>0.144</td>
</tr>
<tr class="even">
<td data-quarto-table-cell-role="th">3</td>
<td>x</td>
<td>0.2254</td>
<td>0.031</td>
<td>7.212</td>
<td>0.000</td>
<td>0.164</td>
<td>0.287</td>
</tr>
<tr class="odd">
<td data-quarto-table-cell-role="th">4</td>
<td>t:x</td>
<td>-0.0109</td>
<td>0.040</td>
<td>-0.272</td>
<td>0.786</td>
<td>-0.090</td>
<td>0.068</td>
</tr>
</tbody>
</table>

</div>




