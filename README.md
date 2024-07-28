

# spark-abtest

Analyzing A/B testing with massive data can be tedious: you need to understand the details of the formulas for various statistical tests, and get the required sufficient statistics on a distributed computing cluster, then pull the sufficient statistics locally and manually calculate the final results. This project provides a series of concise, consistent and high-performance functions to make it easier for data scientists/data analysts to analyze A/B testing in Apache Spark.

CI: [![GitHub Build Status](https://github.com/xiaoa6435/spark-abtest/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/xiaoa6435/spark-abtest/actions/workflows/build_and_test.yml)

# setup

spark-abtest is a SparkSessionExtensions, you can use it in most spark
clients like spark-sql/spark-shell/spark-submit/pyspark/spark-connect
with some extra configurations.


    --conf spark.jars=target/scala-2.12/spark-abtest-0.1.0-3.5.0_2.12.jar \ 
    --conf spark.sql.extensions=org.apache.spark.sql.extra.AbtestExtensions

you can download the corresponding spark and scala version jar from
[releases](https://github.com/xiaoa6435/spark-abtest/releases).

for example, in spark-shell

    $SPARK_HOME/bin/spark-shell \
      --conf spark.jars=target/scala-2.12/spark-abtest-0.1.0-3.5.0_2.12.jar \
      --conf spark.sql.extensions=org.apache.spark.sql.extra.AbtestExtensions

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


|     | user_id | t   | mt  | y   | x           | y2          |
|-----|---------|-----|-----|-----|-------------|-------------|
| 0   | 0       | 1.0 | t0  | 0.0 | -0.64626599 | 1.54772415  |
| 1   | 1       | 0.0 | t0  | 0.0 | -0.85735783 | 1.62497210  |
| 2   | 2       | 0.0 | t0  | 0.0 | 0.63050955  | -0.73287976 |
| 3   | 3       | 1.0 | t0  | 0.0 | 0.98298468  | 0.44892532  |
| 4   | 4       | 0.0 | t1  | 0.0 | -0.83978164 | -1.26275208 |


### 0/1 response: prop test/chisq test

#### two arm

``` python
spark.sql("select prop_test(y, t) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | estimate                                     | pvalue     | lower       | upper      | chisq      |
|-----|----------------------------------------------|------------|-------------|------------|------------|
| 0   | \[0.09937538912654897, 0.10111732956242904\] | 0.36465656 | -0.00200086 | 0.00548474 | 0.82179266 |


``` python
import pandas as pd
from scipy.stats import chi2_contingency
observed = pd.crosstab(pdf['y'], pdf['t'])
chi2, p, dof, expected = chi2_contingency(observed)
(p, chi2)
```

    (0.3646565557469007, 0.8217926576217617)

in addition to p-values and chisq, spark-abtest, also provides confidence
intervals (for more detail, refer to
[prop.test](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/prop.test.html)
or
[chisq.test](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/chisq.test.html)
function in R) and mean response ratio for each treat.

#### multi arm

``` python
spark.sql("select prop_test(y, factor(mt, array('t0', 't1', 't2'))) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | estimate                                           | pvalue     | lower | upper | chisq      |
|-----|----------------------------------------------------|------------|-------|-------|------------|
| 0   | \[0.10143841774048547, 0.10020523100420008, 0.0... | 0.38456362 | NaN   | NaN   | 1.91129209 |


``` python
observed = pd.crosstab(pdf['y'], pdf['mt'])
chi2, p, dof, expected = chi2_contingency(observed)
(p, chi2)
```

    (0.38456361940111916, 1.9112920872850279)

### continuous response: ttest

#### two arms and unequal variance, aka [Welch’s t-test](https://en.wikipedia.org/wiki/Student%27s_t-test)

``` python
spark.sql("select ttest(y2, t) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | y0          | y1          |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|
| 0   | -0.00240728 | 0.00683655 | -0.35211824 | 0.72475033 | -0.01580684 | 0.01099229 | -0.00010504 | -0.00251232 |


``` python
from scipy import stats
x = pdf[pdf['t'] == 1.0]['y2']
y = pdf[pdf['t'] == 0.0]['y2']
ret = stats.ttest_ind(x, y, equal_var=False)
ci = ret.confidence_interval(0.95)
f"tvalue: {ret.statistic:.6f}, pvalue: {ret.pvalue:.6f}, lower: {ci.low:.6f}, upper: {ci.high:.6f}"
```

    'tvalue: -0.352118, pvalue: 0.724750, lower: -0.015807, upper: 0.010992'

#### two arms and equal variance

##### spark-abtest

``` python
spark.sql("select ttest(y2, t, 0.95, 'two-sided', true) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | y0          | y1          |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|
| 0   | -0.00240728 | 0.00683657 | -0.35211763 | 0.72475079 | -0.01580686 | 0.01099231 | -0.00010504 | -0.00251232 |


this actually equal to `anova` or `ols`

``` python
spark.sql("select anova(y2, t) as ret from sdf").select("ret.*").toPandas() # inline expand array of struct
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | my0         | my          |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|
| 0   | -0.00240728 | 0.00683657 | -0.35211763 | 0.72475079 | -0.01580686 | 0.01099231 | -0.00010504 | -0.00251232 |


``` python
spark.sql("select inline(ols(y2, array(1.0, t))) from sdf").toPandas()
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      |
|-----|-------------|------------|-------------|------------|-------------|------------|
| 0   | -0.00010504 | 0.00484427 | -0.02168431 | 0.98269982 | -0.00959976 | 0.00938967 |
| 1   | -0.00240728 | 0.00683657 | -0.35211763 | 0.72475079 | -0.01580686 | 0.01099231 |


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


|     | mannwhitneyu_pvalue |
|-----|---------------------|
| 0   | 0.63485152          |


#### multi arm: anova

ttest is not suitable for more than two treatment groups, in
spark-abtest, use anova instead

``` python
spark.sql("select inline(anova(y2, factor(mt, array('t0', 't1', 't2')))) from sdf").toPandas() # inline expand array of struct
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | my          |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|
| 0   | -0.00320884 | 0.00540173 | -0.59403883 | 0.55248749 | -0.01379615 | 0.00737848 | -0.00320884 |
| 1   | 0.00752212  | 0.00755395 | 0.99578717  | 0.31935597 | -0.00728352 | 0.02232777 | 0.00431329  |
| 2   | -0.00696294 | 0.00969043 | -0.71853771 | 0.47242748 | -0.02595606 | 0.01203018 | -0.01017177 |


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


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | y0          | y1          |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|
| 0   | -0.00240728 | 0.00683655 | -0.35211824 | 0.72475033 | -0.01580684 | 0.01099229 | -0.00010504 | -0.00251232 |


``` python
spark.sql("select cuped(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | y0          | y1          | x0         | x1         | theta      |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|------------|------------|------------|
| 0   | -0.00250543 | 0.00633984 | -0.39518820 | 0.69270484 | -0.01493143 | 0.00992058 | -0.00010504 | -0.00251232 | 0.00021016 | 0.00045288 | 0.40439612 |


``` python
spark.sql("select ancova1(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | my0         | my          | mx0        | mx         |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|------------|------------|
| 0   | -0.00250543 | 0.00633990 | -0.39518449 | 0.69270758 | -0.01493155 | 0.00992069 | -0.00010504 | -0.00251232 | 0.00021016 | 0.00045288 |


``` python
spark.sql("select ancova2(y2, t, x) as ret from sdf").select("ret.*").toPandas() # select("ret.*") to expand all struct fields
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      | my0         | my          | mx0        | mx         |
|-----|-------------|------------|-------------|------------|-------------|------------|-------------|-------------|------------|------------|
| 0   | -0.00250543 | 0.00633993 | -0.39518271 | 0.69270890 | -0.01493161 | 0.00992075 | -0.00010504 | -0.00251232 | 0.00021016 | 0.00045288 |


## randomization unit \> analysis unit: delta method or cluster-robust standard error

when randomization unit is larger than analysis unit, usually we need to use
[delta method](https://arxiv.org/pdf/1803.06336) or [cluster-robust
variance estimator](https://arxiv.org/abs/2105.14705).
functions prefixed with \`cluster\`\` can handle these problems.

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


|     | user_id | visit_id | t          | mt  | y          | x           | y2          |
|-----|---------|----------|------------|-----|------------|-------------|-------------|
| 0   | 0       | 0        | 0.00000000 | t0  | 1.00000000 | -2.00802713 | -1.70662868 |
| 1   | 0       | 1        | 0.00000000 | t0  | 1.00000000 | -2.74050928 | -0.55966320 |
| 2   | 0       | 2        | 0.00000000 | t0  | 1.00000000 | -2.34139859 | -1.58488664 |
| 3   | 0       | 3        | 0.00000000 | t0  | 1.00000000 | -1.97814922 | -1.20993685 |
| 4   | 0       | 4        | 0.00000000 | t0  | 1.00000000 | -2.32212300 | -1.17054778 |


### 0/1 or continuous response: cluster_ttest

``` python
spark.sql("select cluster_ttest(y2, t, user_id) as ret from scdf").select("ret.*").toPandas()
```


|     | delta      | stderr     | tvalue     | pvalue     | lower       | upper      | my0         | my          |
|-----|------------|------------|------------|------------|-------------|------------|-------------|-------------|
| 0   | 0.01441253 | 0.07627543 | 0.18895374 | 0.85013291 | -0.13510268 | 0.16392773 | -0.03379021 | -0.01937769 |


#### delta method with variance reduction

``` python
spark.sql("select cluster_ancova1(y2, t, x, user_id) as ret from scdf").select("ret.*").toPandas()
```


|     | delta      | stderr     | tvalue     | pvalue     | lower       | upper      | my0         | my          | mx0         | mx          |
|-----|------------|------------|------------|------------|-------------|------------|-------------|-------------|-------------|-------------|
| 0   | 0.00309024 | 0.07224579 | 0.04277391 | 0.96588261 | -0.13852608 | 0.14470655 | -0.03379021 | -0.01937769 | -0.06156514 | -0.01019905 |


``` python
spark.sql("select cluster_ancova2(y2, t, x, user_id) as ret from scdf").select("ret.*").toPandas()
```


|     | delta      | stderr     | tvalue     | pvalue     | lower       | upper      | my0         | my          | mx0         | mx          |
|-----|------------|------------|------------|------------|-------------|------------|-------------|-------------|-------------|-------------|
| 0   | 0.00312417 | 0.07226738 | 0.04323066 | 0.96551853 | -0.13853445 | 0.14478279 | -0.03379021 | -0.01937769 | -0.06156514 | -0.01019905 |


### ols/cluster_ols

``` python
spark.sql("select inline(ols(y2, array(1.0, t, x, t * x))) from scdf").toPandas()
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      |
|-----|-------------|------------|-------------|------------|-------------|------------|
| 0   | -0.01991035 | 0.02028927 | -0.98132440 | 0.32645656 | -0.05968141 | 0.01986070 |
| 1   | 0.00272057  | 0.02928153 | 0.09291066  | 0.92597640 | -0.05467713 | 0.06011826 |
| 2   | 0.22544998  | 0.01383548 | 16.29505891 | 0.00000000 | 0.19832965  | 0.25257031 |
| 3   | -0.01093015 | 0.02040205 | -0.53573755 | 0.59215188 | -0.05092228 | 0.02906199 |


``` python
import statsmodels.api as sm
import statsmodels.formula.api as smf

ret = smf.ols(formula='y2 ~ t * x', data=pcdf).fit().summary().tables[1]
pd.DataFrame(ret)
```

|     | 0         | 1       | 2       | 3      | 4        | 5       | 6       |
|-----|-----------|---------|---------|--------|----------|---------|---------|
| 0   |           | coef    | std err | t      | P\>\|t\| | \[0.025 | 0.975\] |
| 1   | Intercept | -0.0199 | 0.020   | -0.981 | 0.326    | -0.060  | 0.020   |
| 2   | t         | 0.0027  | 0.029   | 0.093  | 0.926    | -0.055  | 0.060   |
| 3   | x         | 0.2254  | 0.014   | 16.295 | 0.000    | 0.198   | 0.253   |
| 4   | t:x       | -0.0109 | 0.020   | -0.536 | 0.592    | -0.051  | 0.029   |


``` python
spark.sql("select inline(cluster_ols(y2, array(1.0, t, x, t * x), user_id)) from scdf").toPandas()
```


|     | delta       | stderr     | tvalue      | pvalue     | lower       | upper      |
|-----|-------------|------------|-------------|------------|-------------|------------|
| 0   | -0.01991035 | 0.05397288 | -0.36889552 | 0.71221342 | -0.12570808 | 0.08588737 |
| 1   | 0.00272057  | 0.07213233 | 0.03771632  | 0.96991462 | -0.13867333 | 0.14411447 |
| 2   | 0.22544998  | 0.03126138 | 7.21177413  | 0.00000000 | 0.16417138  | 0.28672857 |
| 3   | -0.01093015 | 0.04018747 | -0.27197896 | 0.78564382 | -0.08970569 | 0.06784539 |


``` python
import statsmodels.api as sm
import statsmodels.formula.api as smf

fit = smf.ols(formula='y2 ~ t * x', data=pcdf).fit()
ret = fit.get_robustcov_results(cov_type='cluster', groups=pcdf['user_id']).summary().tables[1]
pd.DataFrame(ret)
```

|     | 0         | 1       | 2       | 3      | 4        | 5       | 6       |
|-----|-----------|---------|---------|--------|----------|---------|---------|
| 0   |           | coef    | std err | t      | P\>\|t\| | \[0.025 | 0.975\] |
| 1   | Intercept | -0.0199 | 0.054   | -0.369 | 0.712    | -0.126  | 0.086   |
| 2   | t         | 0.0027  | 0.072   | 0.038  | 0.970    | -0.139  | 0.144   |
| 3   | x         | 0.2254  | 0.031   | 7.212  | 0.000    | 0.164   | 0.287   |
| 4   | t:x       | -0.0109 | 0.040   | -0.272 | 0.786    | -0.090  | 0.068   |



# all functions

- prop_test(y, t, confLevel = 0.95, alternative = ‘two-sided’, varEqual
  = false, removeNaN = true, correct = true)

- ttest(y, t, confLevel = 0.95, alternative = ‘two-sided’, varEqual =
  false, removeNaN = true)

- cuped(y, t, x, confLevel = 0.95, alternative = ‘two-sided’, varEqual =
  false, removeNaN = true)

- ols(y, X, confLevel = 0.95, stdErrorType = ‘const’, removeNaN = true)

- anova(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
  true)

- ancova1(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
  true)

- ancova2(y, t, confLevel = 0.95, stdErrorType = ‘const’, removeNaN =
  true)

- cluster_ols(y, X, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
  removeNaN = true)

- cluster_anova(y, t, x, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
  removeNaN = true)

- cluster_ancova1(y, t, x, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
  removeNaN = true)

- cluster_ancova2(y, t, x, cid, confLevel = 0.95, stdErrorType = ‘HC1’,
  removeNaN = true)

- avg_rank(): like rank(), but return average rank rather than min rank
  when ties

except avg_rank(window rank), all functions are aggregate functions.

## input

- y: metrics variable, double type, prop_test also accept boolean
  metrics col
- t: treat variable
    - two arms (A/B test): numeric type of 0/1, or boolean type
    - multi arm(A/B/C… test):
        - wrapped by `factor(t, array('t0', 't1', 't2'))`, the array is
          different values of t
        - the first is base group
        - ttest does’t accept multi treatment
- x: control variable, double type
- confLevel: confidence level of the interval, default value is 0.95
- alternative: a character string specifying the alternative hypothesis,
  must be one of “two.sided” (default), “greater” or “less”.
  You can specify just the initial letter.
- varEqual: is used to estimate the variance otherwise the Welch (or
  Satterthwaite) approximation to the degrees of freedom is used
- removeNaN: if true, any of y/t/x contains nan, the corresponding row will be
  removed, like na.action = ‘omit’
- stdErrorType: a character string specifying the estimation type, for
  ols/anova/ancova1/ancova2, default is const, for
  cluster_ols/anova/ancova1/ancova2, default is HC1

## output

- two arms (A/B test): return a struct with some fields:
    - delta: the estimated difference in means of y
    - stderr: the standard error of the mean (difference), used as
      denominator in the t-statistic formula
    - tvalue: the value of the t-statistic, equal to delta / tvalue
    - pvalue: the p-value for the test
    - lower: lower confidence limits for delta
    - upper upper confidence limits for delta
    - y0: mean of y in the treat group (t = 0)
    - y1: mean of y in the control group (t = 1)
    - x0: mean of x in the treat groups (t = 0)
    - x1: mean of x in the treat group (t = 0)
    - theta: for cuped only
    - my: mean of y in the corresponding treat group
    - mx: mean of x in the corresponding treat group
- multi arm (A/B/C… test): return an array of struct for each treat



