# BetterUp Sovren Output Processor

This is a utility that processes output from the [Sovren Resume parsing API](https://www.sovren.com/products/parser/).

The utility reads in JSON output from the Sovren API and transforms it into a readable CSV file. It also joins the resume output with csv extracts from Mode, so that we can look for trends between resume attributes and coach hiring / coach quality. The utility is capable of reading in output from multiple runs of the Sovren parser at once. The output of running the utility is a timestamped CSV file.

Run the utility with
```
python main.py <Root Directory> <Output Directory>
```
`<Root Directory>` should contain two directories:
- `sovren output` : contains 1..n sub-directories containing output from the [Sovren batch utility](https://github.com/sovren/batch-utility/releases)
  - Each 1..n directory should have sub-directories `json`, `logs`, and `scrubbed`
- `Mode extracts` : contains two files
  - `coach leaderboard extract.csv` : combined output from [Coach leaderboard](https://modeanalytics.com/betterup/reports/c48360f535ec)
  - `dei coaches extract.csv` : output from `SELECT coach_id, first_name, last_name FROM analytics.dei_coaches`

`<Output Directory>` should specify the folder where you will store CSV output from running the utility
