# Data Update Code Analysis

## Overview

The `code/Data_update/update_main.py` file is the main entry point for the data update process. It contains functions for updating score data, market data, factor data, time series data, and L4 data. The updates are performed at specific times of the day (7:05, 15:15, and 18:00), and there is also a function for performing L4 data updates.

## Functions

The code is organized into several functions:

*   **`daily_update_705`:** Updates score data.
*   **`daily_update_1515`:** Updates market data and time series data.
*   **`daily_update_1800`:** Updates factor data and time series data.
*   **`daily_update_auto`:** Orchestrates the daily updates based on the current time.
*   **`daily_update_l4`:** Updates L4 data.

The `daily_update_auto` function determines the current time zoom and calls the corresponding update function. The other update functions call functions in other modules to perform the actual updates and data checks.

## Function Chart

The following function chart illustrates the relationships between the main functions in the `code/Data_update/update_main.py` file:

```mermaid
graph LR
    A[update_main.py] --> B[daily_update_705]
    A --> C[daily_update_1515]
    A --> D[daily_update_1800]
    A --> E[daily_update_auto]
    A --> F[daily_update_l4]

    E --> B
    E --> C
    E --> D

    B --> G[score_update_main]
    B --> H[time_tools]
    B --> I[checking]

    C --> J[MktData_update_main]
    C --> K[timeSeries_update_main_part1]
    C --> L[File_moving]
    C --> M[time_tools]
    C --> N[checking]

    D --> O[FactorData_update]
    D --> P[timeSeries_update_main_part2]
    D --> Q[File_moving]
    D --> R[time_tools]
    D --> S[checking]

    F --> T[L4_update_main]