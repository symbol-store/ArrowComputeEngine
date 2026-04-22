
# Building
```
git clone git@github.com:symbol-store/ArrowComputeEngine.git
cmake -BBuild/Debug -DCMAKE_BUILD_TYPE=Debug -DBOSS_SOURCE_BRANCH=nested-pattern-matching ArrowComputeEngine
cmake --build Build/Debug

```

# Trying it out

```{bash}
curl -sL 'https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv?raw=true' | cut -d, -f1,4,12 > owid-covid-data.csv
./Build/Debug/deps/bin/chibi-scheme  -mBOSS -p'(boss-eval (SetDefaultEnginePipeline "Build/Debug/libArrowComputeEngine.so"))' -p'(boss-eval (ToStatus (Name (Materialize (OrderBy (Project (Load "owid-covid-data.csv") iso_code date (int date) new_cases_per_million) (keys iso_code |int(date)|))) sorted)))' -p'(boss-eval (Join (GroupBy (Name (Pairwise (Cumulate (ByName sorted) (sum new_cases_per_million)) smoothed_new_cases_per_million sum_new_cases_per_million 7) smoothed) (max smoothed_new_cases_per_million) date) (keys date max_smoothed_new_cases_per_million) (ByName smoothed) (keys date smoothed_new_cases_per_million)))'
```
