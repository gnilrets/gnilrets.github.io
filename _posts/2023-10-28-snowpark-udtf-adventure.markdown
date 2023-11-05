---
layout: post
title:  "Snowpark UDTF Adventure"
date:   2023-10-28
categories: python, snowpark, udtf
---

## Introduction

Last year, I got really excited when Snowflake [announced Snowpark for Python](https://www.snowflake.com/blog/snowpark-python-innovation-available-all-snowflake-customers/).  I imagined being able to run arbitrary
python code on a compute cluster with the low-maintenance ease and top-notch speed I've come to expect from Snowflake SQL.
Since then, I've been on the lookout for projects that would allow me to utilize what sounds like such a tremendously
useful resource.  I recently found a relatively simple but useful project that I felt would be a good use case and dove in.
While I got it to work in the end, the number of hoops I had to jump through, unexpected gotchas, and somewhat dissappointing
performance left me feeling a little underwhelmed with the tool.  I'm writing up this summary of my experience so that others
may learn from it and to suggest ways that the experience might be improved.

**Note**: All of the code examples here were developed and tested in
[Hex](hex.tech).  You may see some jinjafied SQL below when we mix
Python and SQL code.

**Disclaimer**: Obviously, this was my first dive into Snowpark and it
is very probably I did something wrong or missed some key component
that would have made all of this simpler.  If so, please let me know!


## Project goals
The goal of my project was pretty simple: *create a Snowpark function that can calculate a rolling median over a large dataset*.
This seemed like a good use case for Snowpark because:
* Snowflake SQL does not have a rolling median function ([rolling mean](https://docs.snowflake.com/en/sql-reference/functions/avg): yes; [window median](https://docs.snowflake.com/en/sql-reference/functions/median): yes, but no median that supports a window frame).
* The overall computation is not that difficult, [Pandas can do it with one line](https://pandas.pydata.org/docs/reference/api/pandas.core.window.rolling.Rolling.median.html):

```python
df['x'].rolling(3).median()
```

## Seems easy... wait, hold on

### Roadblock: choosing the right kind of function
The first thing I had to figure out when approaching this problem was how I would use Snowpark to calculate a rolling median.
I briefly explored [Snowpark dataframes](https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes), thinking
they'd provide a similar level of functionality and extensibility as Pandas dataframes.  However, it turns out that Snowpark dataframes
are just a wrapper around SQL, so there's really nothing you can do with Snowpark dataframes that you can't just do with SQL.  Next, I
looked at [User Defined Functions](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-introduction) (UDFs) and [User Defined Table Functions](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions) (UDTFs).  Both of these function types allow a user to write Python, register the Python with Snowpark, and execute it in a SQL query.  The main difference are:

* UDFs are scalar functions.  They take one input record (which can include multiple parameters) and return a single value.
* UDTFs can operate on multiple records at once and can return multiple records.

While a rolling median function does not require us to change the number of rows that are sent to the function, it does require processing multiple
rows at once.  So a UDTF was the only choice here.  Furthermore, I wanted to make sure that my function could process an arbitrarily large number of records.  This required that the function be able to fit into the memory of a compute node.  To be able to fit into memory, the data would have to be partitioned, even if I just wanted to compute a rolling median over the last 5 records of a billion record dataset.  Snowpark UDTFs only support this kind of partitioning through a [vectorized UDTF](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-vectorized).

Choice: Vectorized UDTF.

### Roadblock: UDTFs don't return passthrough columns
Great, so now I know a vectorized UDTF is the way to go.  In order to understand how vectorized UDTFs work, I wanted to write a really simple UDTF
that did nothing more than add `1` to the input column.  Here's the function I wrote

```python
# For brevity, I'm not going to show all of the imports required in subsequent code samples.
import snowflake
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import (
    PandasDataFrame,
    StructType,
    StructField,
    IntegerType,
    FloatType,
)

@udtf(
    packages=['pandas'],
    input_types=[
        IntegerType(),
    ],
    output_schema = StructType([
        StructField('plus_one', IntegerType())
    ]),
)
class MyUdtf:
    def end_partition(self, df: PandasDataFrame[int]) -> PandasDataFrame[int]:
        df['plus_one'] = df['ARG1'] + 1
        return df[['plus_one']]
```


Using this UDTF in SQL
```sql
with mydata as (
    select 1 as idx, 10 as x
    union all
    select 2 as idx, 20 as x
    union all
    select 3 as idx, 30 as x
)
select *
from
    mydata,
    table({{ MyUdtf.name|sqlsafe }}(x) over (partition by true))
```

We get data like

|   IDX |   X |   PLUS_ONE |
|------|----|-----------|
|   nan | nan |         11 |
|   nan | nan |         21 |
|   nan | nan |         31 |

So both the index column and the input data goes away!  To some degree, I understand why this must be true, since the vectorized UDTF
works on a partition level/not a row-level and can delete records or create them out of nothing, it wouldn't really know how to associate
the input records with the outputs.  However, in this case I definitely want to keep my input records around!

One solution might be to change my UDTF so that it accepts the `idx` column as an input, and then returns `idx`, `x`, and `plus_one`.  There are at least two issues with this, as we'll explore in the next two roadblocks.

### Roadblock: Lack of named parameters
One issue, while minor, is very annoying: there is no way to define named parameters in a UDTF.  Not that big of a deal with just two inputs, but obviously as the number of inputs grows, this becomes more of a problem in that the SQL developer has to remember the precise order of the arguments.  Here's what I wish we could do:
```sql
-- Does not work, but I wish it did
from
    mydata,
    table({{ MyUdtf.name|sqlsafe }}(
        record_index => idx,
        val => x
    ) over (partition by true))
```

### Roadblock: Fixed input and output columns
One of the bigger issues I found was the fact that the **input and output schemas of a UDTF are FIXED!**  So I couldn't just define a set of passthrough fields, nor could I dynamically generate columns based on the data I'm processing.  For the rolling mean UDTF, if I've got a large table and I just want to compute the rolling mean on a few columns, it's by no means straightforward to do so.  My first thought in working around this was to create a separate CTE for each field I wanted to compute a rolling mean on that contained a unique key for each record, run the rolling mean computation, and then re-join all of the individual CTEs back together.  What a pain!  I ended up going with a solution using variants as described below, but as I write this I'm not really certain that ended up as any simpler of a solution.

### Workaround: Variants all the way down and then back up again
I ended up working around some of the issues above by defining my inputs and outputs as a single variant columns.  This allows me to fake named parameters and pass through an arbitrary number of other columns.  The downside is that I have to flatten the data after it comes into the UDTF and then unflatten it before returning the results.

In our simple "plus one" example (don't worry, I'll get to the full rolling median soon) we end up with a UDTF like

```python
import pandas as pd

@udtf(
    packages=['pandas'],
    input_types=[
        VariantType(),
    ],
    output_schema = StructType([
        StructField('results', VariantType())
    ]),
)
class MyUdtf:
    def end_partition(self, df: PandasDataFrame[dict]) -> PandasDataFrame[dict]:
        flattened_df = pd.DataFrame.from_records(df['ARG1'])
        flattened_df['plus_one'] = flattened_df['x'] + 1

        result_df = pd.DataFrame()
        result_df['result'] = flattened_df[['idx', 'x', 'plus_one']].to_dict(orient='records')

        return result_df[['result']]

```

which can be used in SQL like

```sql
with mydata as (
    select 1 as idx, 10 as x
    union all
    select 2 as idx, 20 as x
    union all
    select 3 as idx, 30 as x
)
select
    f.results:idx::int as idx,
    f.results:x::int as x,
    f.results:plus_one::int as plus_one
from
    mydata,
    table({{ MyUdtf.name|sqlsafe }}(
        {
            'idx': idx,
            'x': x
        }::variant
    ) over (partition by true)) as f
```

which gives

|   IDX |   X |   PLUS_ONE |
|------:|----:|-----------:|
|     1 |  10 |         11 |
|     2 |  20 |         21 |
|     3 |  30 |         31 |


### Roadblock: Lack of non-column parameters
One final roadblock/annoyance before we get to the rolling median.  When a SQL call passes input to the `end_partition` function of the vectorized UDTF handler class, it can *only* be associated with a column in the pandas dataframe.  There is no way to define a scalar constant.  For example, with our "plus one" UDTF, we might want to change it to a "plus N" UDTF and supply a single constant `N` to be added to every record.  With the rolling median, we'd like to pass the window size and maybe other configuration arguments.  The simple workaround to this is to just add a constant to the input variant and then grab the scalars from the first record of the `end_partition` input dataframe.  Works, but feels wrong and wasteful.

### Solution: A functional rolling median solution
Ok, let's put together everything that we've learned from working with the toy example above:

* Use pandas to actually run the calculation: `df['x'].rolling(3).median()`
* Use it in a vectorized UDTF.
* Use a variant to define all of the input data, separating them into "passthrough" and "data" fields.
* Flatten the input data, run the calculation, then unflatten before passing back to SQL.

Additionally, I wanted to split out all of the flattening/calculation/unflatten bit from the UDTF handler class so that I could write unit tests on the core python functionality without having to run it all through Snowflake.  To do so, I wrapped all of this logic into a "Processor" class and then used that in the UDTF handler.

Here's what I finally came up with

```python
class RollingMedianProcessor:
    """Used by a UDTF to calculate a rolling median."""

    def __init__(self, source_df: pd.DataFrame):
        self.source_df: pd.DataFrame = source_df
        self.window_size: Optional[int] = None
        self.data_fields: list = []
        self.result_data_fields: list = []
        self.result_df: pd.DataFrame

    def flatten(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converts variant data input into distinct pandas columns."""

        self.window_size = df["window_size"].iloc[0]
        vardata_df = pd.DataFrame.from_records(df["vardata"])

        data_df = pd.DataFrame.from_records(vardata_df["data"])
        flattened = pd.concat(
            [
                vardata_df["attributes"],
                data_df,
            ],
            axis=1,
        )
        flattened.replace("NaN", np.nan, inplace=True)
        flattened.replace("NaT", np.nan, inplace=True)

        self.data_fields = list(data_df.columns)
        self.result_data_fields = list(data_df.columns)

        return flattened

    def rolling_median_field(
        self, df: pd.DataFrame, field: str, window_size: int = None
    ) -> pd.DataFrame:
        """Computes a rolling median for a specific field."""

        window_size = window_size or self.window_size
        median_field_name = f"median__{field}"
        self.result_data_fields.append(median_field_name)
        df[median_field_name] = df[field].rolling(window_size, min_periods=1).median()
        return df

    def rolling_median(self, df: pd.DataFrame) -> pd.DataFrame:
        """Computes rolling median for all data field."""

        for field in self.data_fields:
            self.rolling_median_field(df, field=field)
        return df

    def unflatten(self, df: pd.DataFrame) -> pd.DataFrame:
        """Repackages data into a variant to be passed back to Snowflake."""

        df["data"] = df[self.result_data_fields].to_dict(orient="records")
        return df

    def materialize(self):
        """Runs the rolling median process."""

        work_df = self.source_df.pipe(self.flatten).pipe(self.rolling_median).pipe(self.unflatten)
        self.result_df = work_df[["attributes", "data"]]
        return work_df

@udtf(
    packages=["pandas"],
    input_types=[
        VariantType(),
        IntegerType(),
    ],
    output_schema=StructType(
        [
            StructField("attributes", VariantType()),
            StructField("data", VariantType()),
        ]
    ),
)
class RollingMedianUdtf:
    def __init__(self):
        self.result_df = pd.DataFrame()

    def end_partition(self, input_df: PandasDataFrame[dict, int]) -> PandasDataFrame[dict, dict]:
        input_df.rename(
            columns={
                "ARG1": "vardata",
                "ARG2": "window_size",
            },
            inplace=True,
        )
        processor = RollingMedianProcessor(input_df)
        processor.materialize()
        return processor.result_df

```

Now let's demonstrate how this can be used.  Suppose we have some time series data and two variables for which we want to compute a rolling median.  Let's name the table `sample_data`.  So we don't have to deal with datetime complexity, let's just fake it for now.

**`sample_data`:**

|   HOUR_AT |   MINUTE_AT |   X |   Y |
|----------:|------------:|----:|----:|
|         1 |           1 |   1 |  10 |
|         1 |           2 |   2 |  20 |
|         1 |           3 |   3 |  30 |
|         1 |           4 |   4 |  40 |
|         2 |           1 |   5 |  50 |
|         2 |           2 |   6 |  60 |
|         2 |           3 |   7 |  70 |
|         2 |           4 |   8 |  80 |
|         3 |           1 |   9 |  90 |
|         3 |           2 |  10 | 100 |
|         3 |           3 |  11 | 110 |
|         3 |           4 |  12 | 120 |

To use our UDTF, we construct a SQL statement like this

```sql
select
    func.attributes:hour_at::int as hour_at,
    func.attributes:minute_at::int as minute_at,
    func.data:x::float as x,
    func.data:median__x::float as median__x,
    func.data:y::float as y,
    func.data:median__y::float as median__y

from
    sample_data,
    table({{ RollingMedianUdtf.name|sqlsafe }}(
        {
            'attributes': {
                'hour_at': hour_at,
                'minute_at': minute_at
            },
            'data': {
                'x': x,
                'y': y
            }
        }::variant,
        3
    ) over (partition by true)) as func
```

Which gives our expected result

|   HOUR_AT |   MINUTE_AT |   X |   MEDIAN__X |   Y |   MEDIAN__Y |
|----------:|------------:|----:|------------:|----:|------------:|
|         1 |           1 |   1 |         1   |  10 |          10 |
|         1 |           2 |   2 |         1.5 |  20 |          15 |
|         1 |           3 |   3 |         2   |  30 |          20 |
|         1 |           4 |   4 |         3   |  40 |          30 |
|         2 |           1 |   5 |         4   |  50 |          40 |
|         2 |           2 |   6 |         5   |  60 |          50 |
|         2 |           3 |   7 |         6   |  70 |          60 |
|         2 |           4 |   8 |         7   |  80 |          70 |
|         3 |           1 |   9 |         8   |  90 |          80 |
|         3 |           2 |  10 |         9   | 100 |          90 |
|         3 |           3 |  11 |        10   | 110 |         100 |
|         3 |           4 |  12 |        11   | 120 |         110 |


Fabulous, we finally got there!  But wait, there's a problem....

### Roadblock: Large partitions don't **just work**

The astute reader will notice that in the example above, we're passing all data to the vectorized UDTF in one big partition, `over (partion by true)`.  This works fine for small datasets.  But unfortunately I need this to run on time series containing millions of records, and I don't want to break my budget by using an [X-Large snowpark optimized warehouse](https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized).  Perhaps we could partition our data into years, months, or days in order to pass data small enough to fit into the memory of our default warehouse.  The problem of course is that calculations at the partition boundaries will not be accurate.  Maybe there are cases where that doesn't matter, but maybe not.

Ideally, Snowflake would provide some UDTF abstraction that would make dealing with easy, but no such thing exists right now so we've got to deal with it ourselves....

### Solution: Manually manage partition boundaries in calling SQL

We can use our existing UDTF with arbitrarily large datasets as long as we can partition our data by some reasonable unit of time and then knit together the boundaries.  To do this, we take the records we need from the previous partition and duplicate (union) those in the partition being processed.  If we want to calculate a rolling median with a window size of 3 records and we partition our data into hour bins, then we need to grab the last 3 records from the prior partition and replicate them in the partition being processed.  The SQL to do this is shown below

```sql
with w_frame_edges as (
    -- Take the last 3 records of an hourly partition and shift them to the next partition
    -- mark them as not being in the result frame so we can filter them out later
    select False as __is_result_frame, hour_at + 1 as __partition_frame, * from sample_data
        qualify row_number() over (partition by hour_at order by hour_at desc, minute_at desc) <= 3
    union all
    select True as __is_result_frame, hour_at as __partition_frame, * from sample_data
),
final as (
    select
        func.attributes:__is_result_frame::boolean as __is_result_frame,
        func.attributes:__partition_frame::int as __partition_frame,
        func.attributes:hour_at::int as hour_at,
        func.attributes:minute_at::int as minute_at,
        func.data:x::float as x,
        func.data:median__x::float as median__x,
        func.data:y::float as y,
        func.data:median__y::float as median__y

    from
        w_frame_edges,
        table({{ RollingMedianUdtf.name|sqlsafe }}(
            {
                'attributes': {
                    'hour_at': hour_at,
                    'minute_at': minute_at,
                    '__is_result_frame': __is_result_frame,
                    '__partition_frame': __partition_frame
                },
                'data': {
                    'x': x,
                    'y': y
                }
            }::variant,
            3
        ) over (partition by __partition_frame order by hour_at, minute_at)) as func
)
select *
from
    final
where
    __is_result_frame
order by
    __partition_frame, hour_at, minute_at
```

The SQL above includes a filter on `__is_result_frame == True`.  If we disable that filter we can see how the data is knit together (and also validate that when we do include the filter, we get the same result as we did before we partitioned the data)

| __IS_RESULT_FRAME   |   __PARTITION_FRAME |   HOUR_AT |   MINUTE_AT |   X |   MEDIAN__X |   Y |   MEDIAN__Y |
|:--------------------|--------------------:|----------:|------------:|----:|------------:|----:|------------:|
| True                |                   1 |         1 |           1 |   1 |         1   |  10 |          10 |
| True                |                   1 |         1 |           2 |   2 |         1.5 |  20 |          15 |
| True                |                   1 |         1 |           3 |   3 |         2   |  30 |          20 |
| True                |                   1 |         1 |           4 |   4 |         3   |  40 |          30 |
| False               |                   2 |         1 |           2 |   2 |         2   |  20 |          20 |
| False               |                   2 |         1 |           3 |   3 |         2.5 |  30 |          25 |
| False               |                   2 |         1 |           4 |   4 |         3   |  40 |          30 |
| True                |                   2 |         2 |           1 |   5 |         4   |  50 |          40 |
| True                |                   2 |         2 |           2 |   6 |         5   |  60 |          50 |
| True                |                   2 |         2 |           3 |   7 |         6   |  70 |          60 |
| True                |                   2 |         2 |           4 |   8 |         7   |  80 |          70 |
| False               |                   3 |         2 |           2 |   6 |         6   |  60 |          60 |
| False               |                   3 |         2 |           3 |   7 |         6.5 |  70 |          65 |
| False               |                   3 |         2 |           4 |   8 |         7   |  80 |          70 |
| True                |                   3 |         3 |           1 |   9 |         8   |  90 |          80 |
| True                |                   3 |         3 |           2 |  10 |         9   | 100 |          90 |
| True                |                   3 |         3 |           3 |  11 |        10   | 110 |         100 |
| True                |                   3 |         3 |           4 |  12 |        11   | 120 |         110 |
| False               |                   4 |         3 |           2 |  10 |        10   | 100 |         100 |
| False               |                   4 |         3 |           3 |  11 |        10.5 | 110 |         105 |
| False               |                   4 |         3 |           4 |  12 |        11   | 120 |         110 |

Success!  Kind of...

So all of this works, but it requires a lot of effort and thought on the part of both the developer and the user of the UDTF, and is likely quite error prone.

### How could Snowflake help make snowpark better?

I think there are several improvements that Snowflake could implement to make this whole process better for both Snowpark developers and our users.  Some are likely easier than other.

#### Support `*args` and `**kwargs` style input and output argument schemas
I spent a lot of mental effort, code real-estate, and compute resources on managing the fact that input and output schemas are fixed.  We shoud be able to create UDTFs with arbitrary (named) input columns and dynamic outputs.  Additionally, we should be able to pass scalar arguments to these UDTFs without needing to fake them as columnar data.

#### Give us an abstraction for managing partition boundaries so we don't have to work so hard on the SQL side
I want the users of my UDTF to be able to just use it without having to think so hard or make dramatic modifications to their SQL code just to add a simple calculation like a rolling median.  Perhaps Snowlake should provide some mechanism in the UDTF for combining data across partitions.

Somehow Snowflake is able to create window functions like a rolling mean that can operate reliably on arbitrarily large data without the user having to think about how it's partitioned to fit into the memory of an XSMALL warehouse.  Snowflake needs to figure out how to expose that functionality to developers to we can build more user-friendly UDTFs.

### Conclusion: Provide a UDWF?
Perhaps many of the above issues could be resolved by supporting a User Defined *Window* Function (UDWF)?  UDFs can only operate on a single row at a time and UDTFs operate on a whole partition and can generate or delete data in the partition.  I think we need something in between these two types of functions. One that returns the same number of rows from the input but can operate (efficiently) on a collection of records at a time.
