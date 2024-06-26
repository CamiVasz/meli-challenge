# Camila's meli challenge
This repository contains a python data pipeline. There are 3 inputs with data from different sources and the outcome is a dataset ready to be used by the model.
## Data Sources
All of these are in the data folder
- Prints (prints.json) a month historical data of displayed value props to the users.
- Taps (taps.jason) a month historical data with the clicked value props by the users.
- Payments (pays.csv) a month historical data with payments made by users
### Data reading
Giving the use case, the election of PySpark results in the implementation being more scalable than using other libraries such as pandas.
## Data processing
For the queries required to process the data and deliver the final results, the native functions from pySpark are used. In a larger scale, this allows the pipeline to take advantage of the optimizations the library provides.

Each one of the resulting columns is processed independently and joined at the end to generate the end result. This was done in development to identify possible failures along the way.
## Results
A parquet dataframe containing:
- Prints from the last week
- For each print:
    - *was_tapped*: Indicator (1,0). Marks if the value props were clicked or not
    - *total_views_last_3_weeks*: Number of times the user viewd the value pro in the last 3 weeks prior to the print.
    - *total_taps_last_3_weeks*: Number of times a user clicked on each of the value props in the last 3 weeks.
    - *number_payments_l3w*: Number of payments made by the user for each value props in the last 3 weeks.
    - *total_payments_last_3_weks*: Accumulated payments made by the user for each value props in the last 3 weeks.
### Output format
Parquet was selected over other formats since the data will be incorporated in the development of a machine learning model. 

It has a lot of advantages, it can be efficiently compressed and it is supported by big data processing frameworks and ML libraries, among others.
## Improvements
Here are some things that may be done to improve this implementation:
- Feed the pipeline with data coming from a cloud location that gets updated either on batch or streaming
- Implement tests to ensure the results are as intended, foreseeing changes in the data structure.
- Process data in a cleaner way that allows multiplpe outputs to be produced in the same query.
- Finish the pipeline by feeding the results to a cloud location/stream.
