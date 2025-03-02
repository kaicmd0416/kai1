# Data Update Analysis

## 1. Overall Structure and Purpose:

The `Data_update` folder contains scripts and modules responsible for updating various types of data, including market data, factor data, scores, and L4 data. The update processes are likely scheduled and automated using batch files and task scheduler configurations (XML files).

## 2. Key Components and Files:

*   **`update_main.py`:** This is a central script that orchestrates the data update process. It defines functions for different update schedules (7:05, 15:15, 18:00) and a main function (`daily_update_auto`) that determines which update to run based on the current time. It also includes a `daily_update_l4` function for L4 data updates.
*   **`history_main.py`:** This script seems to handle historical data updates. It calls functions from various submodules to update historical market data, factor data, scores, time series data, and L4 data.
*   **Batch files (`.bat`):** Several batch files are present, such as `daily_update_l4_test&product_tracking_main.bat`, `data_update_test.bat`, etc. These files are used to execute the Python scripts, likely through Anaconda. They set up the environment (e.g., `PYTHONPATH`, `ANACONDAPATH`) and then run the appropriate Python function.
*   **XML files (`.xml`):** Files like `daily_update_l4_test&product_tracking_main.xml` and `data_update_test.xml` are task scheduler configuration files. They define the schedule and settings for automated execution of the corresponding batch files.
*   **Subdirectories:** The folder contains several subdirectories, each responsible for updating a specific type of data:
    *   `FactorData_update`: Updates factor data.
    *   `MktData_update`: Updates market data.
    *   `Score_update`: Updates scores.
    *   `File_moving`: Moves files.
    *   `L4Data_update`: Updates L4 data.
    *   `TimeSeries_update`: Updates time series data.
    *   `Time_tools`: Provides time-related utility functions.
    *   `Data_checking`: Checks data quality.
    *   `global_tools_func`: Contains global utility functions.
    *   `config_project`: Contains configuration files.
    *   `config_path`: Contains path configuration files.
    *   `auto`: Contains `main.py` which is likely related to automated tasks.

## 3. Detailed Function Chart:

```
Data_update
├── update_main.py
│   ├── daily_update_705()
│   │   ├── time_tools.target_date_decision_705()
│   │   │   ├── time_tools_config.xlsx (read)
│   │   │   ├── global_tools.is_workday2()
│   │   │   ├── global_tools.next_workday_calculate()
│   │   │   └── global_tools.strdate_transfer()
│   │   ├── global_tools.strdate_transfer()
│   │   ├── score_update_main()
│   │   │   ├── rrScore_update.rr_update_main()
│   │   │   ├── scorePortfolio_update.scorePortfolio_update_main()
│   │   │   └── combineScore_update.score_combination_main()
│   │   └── checking.checking_main()
│   ├── daily_update_1515()
│   │   ├── time_tools.target_date_decision_1515()
│   │   │   ├── time_tools_config.xlsx (read)
│   │   │   ├── global_tools.is_workday2()
│   │   │   ├── global_tools.last_workday_calculate()
│   │   ├── global_tools.strdate_transfer()
│   │   ├── MktData_update_main()
│   │   │   ├── indexReturn_update.index_return_update_main()
│   │   │   ├── indexComponent_update.index_component_update_main()
│   │   │   └── stockData_update.stock_data_update_main()
│   │   ├── checking.checking_main()
│   │   ├── timeSeries_update_main_part1()
│   │   │   ├── index_return_update()
│   │   │   │   ├── output_indexreturn (read)
│   │   │   │   └── output_timeseries/index_return.csv (write)
│   │   │   └── stock_data_update()
│   │   │       ├── data_other/StockUniverse_new.csv (read)
│   │   │       ├── output_stockreturn (read)
│   │   │       ├── output_stockclose (read)
│   │   │       └── output_timeseries/stock_return.csv (write)
│   │   │       └── output_timeseries/stock_close.csv (write)
│   │   └── File_moving.file_moving_update_main()
│   │       ├── data_realtime_moving()
│   │       │   ├── input_realtime (read)
│   │       │   └── output_realtime (write)
│   │       └── data_cbond_moving()
│   │           ├── input_cbond (read)
│   │           └── output_cbond (write)
│   ├── daily_update_1800()
│   │   ├── time_tools.target_date_decision_1800()
│   │   │   ├── time_tools_config.xlsx (read)
│   │   │   ├── global_tools.is_workday2()
│   │   │   ├── global_tools.last_workday_calculate()
│   │   ├── global_tools.strdate_transfer()
│   │   ├── FactorData_update.FactorData_update_main()
│   │   │   ├── factor_update_main()
│   │   │   │   ├── data_source_priority.xlsx (read)
│   │   │   │   ├── FactorData_prepare.jy_factor_exposure_update()
│   │   │   │   ├── FactorData_prepare.wind_factor_exposure_update()
│   │   │   │   ├── output_factor_exposure (write)
│   │   │   │   ├── output_factor_return (write)
│   │   │   │   ├── output_factor_stockpool (write)
│   │   │   │   ├── output_factor_cov (write)
│   │   │   │   └── output_factor_specific_risk (write)
│   │   │   └── index_factor_update_main()
│   │   │       ├── data_source_priority.xlsx (read)
│   │   │       ├── FactorData_prepare.jy_factor_index_exposure_update()
│   │   │       ├── FactorData_prepare.wind_factor_index_exposure_update()
│   │   │       └── output_indexexposure (write)
│   │   ├── checking.checking_main()
│   │   ├── timeSeries_update_main_part2()
│   │   │   ├── factor_return_update()
│   │   │   │   ├── output_factor_return (read)
│   │   │   │   └── output_timeseries/lnmodel.csv (write)
│   │   │   ├── factor_stockpool_update()
│   │   │   │   ├── output_factor_stockpool (read)
│   │   │   │   └── output_timeseries/Stock_pool.csv (write)
│   │   │   └── factor_indexexposure_update()
│   │   │       ├── output_indexexposure (read)
│   │   │       └── output_timeseries/*因子风险暴露.csv (write)
│   │   └── File_moving.file_moving_update_main()
│   │       ├── data_product_moving()
│   │       │   ├── input_prod (read)
│   │       │   └── output_prod (write)
│   ├── daily_update_auto()
│   │   ├── time_tools.time_zoom_decision()
│   │   │   └── time_tools_config.xlsx (read)
│   │   ├── daily_update_705()  (conditional)
│   │   ├── daily_update_1515() (conditional)
│   │   └── daily_update_1800() (conditional)
│   └── daily_update_l4()
│       └── L4_update_main()
│           ├── valid_productCode_withdraw()
│           │   └── L4_config.xlsx (read)
│           ├── target_date_decision_L4()
│           ├── L4_running_main()
│           │   ├── L4Data_preparing.raw_L4_withdraw()
│   │       ├── L4Holding_update.L4Holding_processing()
│   │       ├── L4Info_update.L4Info_processing()
│   │       └── L4Prod_update.holding_diff()
└── history_main.py
    ├── Data_history()
    │   ├── File_moving.file_moving_history_main()
    │   │   ├── data_other_moving()
    │   │   │   ├── input_destination (read)
    │   │   │   └── output_destination (write)
    │   │   ├── data_realtime_moving()
    │   │   │   ├── input_realtime (read)
    │   │   │   └── output_realtime (write)
    │   │   ├── data_cbond_moving()
    │   │   │   ├── input_cbond (read)
    │   │   │   └── output_cbond (write)
    │   │   └── data_product_moving()
    │   │       ├── input_prod (read)
    │   │       └── output_prod (write)
    │   ├── MktData_update_main()
    │   │   ├── indexReturn_update.index_return_update_main()
    │   │   ├── indexComponent_update.index_component_update_main()
    │   │   └── stockData_update.stock_data_update_main()
    │   ├── FactorData_history_main()
    │   │   ├── factor_update_main()
    │   │   │   ├── data_source_priority.xlsx (read)
    │   │   │   ├── FactorData_prepare.jy_factor_exposure_update()
    │   │   │   ├── FactorData_prepare.wind_factor_exposure_update()
    │   │   │   ├── output_factor_exposure (write)
    │   │   │   ├── output_factor_return (write)
    │   │   │   ├── output_factor_stockpool (write)
    │   │   │   ├── output_factor_cov (write)
    │   │   │   └── output_factor_specific_risk (write)
    │   │   └── index_factor_update_main()
    │   │       ├── data_source_priority.xlsx (read)
    │   │       ├── FactorData_prepare.jy_factor_index_exposure_update()
    │   │       ├── FactorData_prepare.wind_factor_index_exposure_update()
    │   │       └── output_indexexposure (write)
    │   ├── score_update_main()
    │   │   ├── rrScore_update.rr_update_main()
    │   │   ├── scorePortfolio_update.scorePortfolio_update_main()
    │   │   └── combineScore_update.score_combination_main()
    │   ├── timeSeries_update_main_part1()
    │   │   ├── index_return_update()
    │   │   │   ├── output_indexreturn (read)
    │   │   │   └── output_timeseries/index_return.csv (write)
    │   │   └── stock_data_update()
    │   │       ├── data_other/StockUniverse_new.csv (read)
    │   │       ├── output_stockreturn (read)
    │   │       ├── output_stockclose (read)
    │   │       └── output_timeseries/stock_return.csv (write)
    │   │       └── output_timeseries/stock_close.csv (write)
    │   ├── timeSeries_update_main_part2()
    │   │   ├── factor_return_update()
    │   │   │   ├── output_factor_return (read)
    │   │   │   └── output_timeseries/lnmodel.csv (write)
    │   │   ├── factor_stockpool_update()
    │   │   │   ├── output_factor_stockpool (read)
    │   │   │   └── output_timeseries/Stock_pool.csv (write)
    │   │   └── factor_indexexposure_update()
    │   │       ├── output_indexexposure (read)
    │   │       └── output_timeseries/*因子风险暴露.csv (write)
    │   └── L4_history_main()
    │       ├── valid_productName_withdraw()
    │       │   └── L4_config.xlsx (read)
    │       ├── L4_running_main()
    │       │   ├── L4Data_preparing.raw_L4_withdraw()
    │   │   ├── L4Holding_update.L4Holding_processing()
    │   │   ├── L4Info_update.L4Info_processing()
    │   │   └── L4Prod_update.holding_diff()
```

## 4. Detailed Description:

The `Data_update` folder contains a system for updating various types of financial data, including market data, factor data, scores, and L4 data. The system is designed to be automated and scheduled, with different update routines running at different times of the day.

*   **`update_main.py`:** This script is the main entry point for the daily update process. It defines several functions that update different data components:
    *   **`daily_update_705()`:** This function updates scores. It determines the target date, updates the scores using `score_update_main()`, and performs data checking using `checking.checking_main()`.
    *   **`daily_update_1515()`:** This function updates market data, time series data and moves files. It determines the target date, updates the market data using `MktData_update_main()`, updates time series data using `timeSeries_update_main_part1()`, performs data checking using `checking.checking_main()`, and moves files using `File_moving.file_moving_update_main()`.
    *   **`daily_update_1800()`:** This function updates factor data, time series data and moves files. It determines the target date, updates the factor data using `FactorData_update.FactorData_update_main()`, updates time series data using `timeSeries_update_main_part2()`, performs data checking using `checking.checking_main()`, and moves files using `File_moving.file_moving_update_main()`.
    *   **`daily_update_auto()`:** This function acts as a dispatcher, determining which of the above update functions to call based on the current time. It uses the `time_tools.time_zoom_decision()` function to determine the appropriate time zoom and then calls the corresponding update function.
    *   **`daily_update_l4()`:** This function updates L4 data by calling `L4_update_main()`.
*   **`history_main.py`:** This script is used to update historical data. It defines a `Data_history()` function that calls functions to update historical market data, factor data, scores, time series data, and L4 data.
*   **`Time_tools/time_tools.py`:** This module provides time-related utility functions, including functions to determine the target date for each update routine and to determine the current time zoom.
*   **`FactorData_update/factor_update.py`:** This module is responsible for updating factor data. It defines functions to retrieve factor data from different sources (e.g., 'jy', 'wind') and to save the data to CSV files.
*   **`MktData_update/MktData_update_main.py`:** This module is responsible for updating market data. It calls functions to update index return, index component, and stock data.
*   **`Score_update/score_update_main.py`:** This module is responsible for updating scores. It calls functions to update different score components.
*   **`File_moving/File_moving.py`:** This module is responsible for moving files between different directories.
*   **`L4Data_update/L4_running_main.py`:** This module is responsible for updating L4 data. It defines functions to retrieve L4 data, process it, and save it to the database.
*   **`TimeSeries_update/time_series_data_update.py`:** This module is responsible for updating time series data, including index return, stock data, factor return, factor stock pool, and factor index exposure.

## 5. Data Flow:

The data flow in the `Data_update` system can be summarized as follows:

1.  The `daily_update_auto()` function in `update_main.py` is executed periodically, likely triggered by a task scheduler.
2.  `daily_update_auto()` determines the appropriate update routine to run based on the current time.
3.  The selected update routine (e.g., `daily_update_1515()`) is executed.
4.  The update routine calls functions from various submodules to retrieve and process the data.
5.  The data is retrieved from external sources (e.g., JY, Wind) or from local files.
6.  The data is processed and transformed.
7.  The updated data is saved to CSV files or other data stores.
8.  File moving operations are performed to move data files between directories.

## 6. Configuration:

The `Data_update` system relies on several configuration files to define settings such as data source priorities, file paths, and time zones. These configuration files are typically stored in Excel format and are read by the Python scripts using the `pandas` library. The paths to these configuration files are stored in the `global_setting.global_dic` module.

## 7. Automation and Scheduling:

The `Data_update` system is designed to be automated and scheduled. The batch files in the `Data_update` folder are used to execute the Python scripts, and the XML files are used to configure the Windows Task Scheduler to run the batch files at specific times or intervals.