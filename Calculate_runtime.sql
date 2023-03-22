-- First create a table with delta of original timestamps & lagged timestamps
WITH
    cte1_tbl_with_lagged_ts AS(
        SELECT
           id,
           t_stamp AS current_ts,
           RUN_STS,
           STOP_STS,
           BREAKDOWN_STS,
           -- Now get a time stamp difference between every consecutive entry
           DATEDIFF(second,LAG(t_stamp) OVER(ORDER BY tbl_ffs_en7913_loggr_ndx),t_stamp) AS difference
       
        FROM tbl_machine123_loggr
        WHERE
            t_stamp > :start_ts AND t_stamp < :end_ts
        ),

    cte2_tbl_runtime_calcs AS(
    -- Only select following columns in which RUN_STS = 1
        SELECT
            id,
            RUN_STS,
            difference AS runtime    
        FROM cte1_tbl_with_lagged_ts
        WHERE RUN_STS=1
        ),
    
    cte3_tbl_downtime_calcs AS(
    -- Only select following columns in which STOP_STS = 1
        SELECT
            id,
            STOP_STS,
            difference AS stoptime    
        FROM cte1_tbl_with_lagged_ts
        WHERE STOP_STS=1
        ),
        
    cte4_tbl_breakdowntime_calcs AS(
    -- Only select following columns in which BREAKDOWN_STS = 1
        SELECT
            id,
            BREAKDOWN_STS,
            difference AS breakdowntime    
        FROM cte1_tbl_with_lagged_ts
        WHERE BREAKDOWN_STS=1
        ),
        
    cte5_tbl_poweredofftime_calcs AS(
    -- Only select following columns where no STS signal is present (i.e. all of them are zero)
        SELECT
            id,
            current_ts,
            RUN_STS,
            STOP_STS,
            BREAKDOWN_STS,
            difference AS poweredofftime    
        FROM cte1_tbl_with_lagged_ts
        WHERE RUN_STS=0 AND BREAKDOWN_STS=0 AND STOP_STS = 0
        )

-- Take sum of all timestamp differences
SELECT
    SUM(runtime)/60.0 AS total_runtime
    FROM cte2_tbl_runtime_calcs;
    
/*  SUM(downtime)/60.0 AS total_runtime
    FROM cte3_tbl_downtime_calcs;
*/
/*  SUM(breakdowntime)/60.0 AS total_runtime
    FROM cte4_tbl_breakdowntime_calcs;
*/
