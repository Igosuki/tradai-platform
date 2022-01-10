set terminal wxt size 2800,1200 font ',10'
set multiplot layout 8,2 columnsfirst title "Multiplot layout 3, 2" font ",14"
set key left Left reverse
set key autotitle columnhead # use the first line as title
set tics out nomirror

server_ema_file = 'trader/target/test_results/strategies_mean_reverting_tests_scenario_'.order_mode.'/'.pair.'_ema_values.csv'
server_trades_file = 'trader/target/test_results/strategies_mean_reverting_tests_scenario_'.order_mode.'/trade_events.csv'
server_threshold_file = 'trader/target/test_results/strategies_mean_reverting_tests_scenario_'.order_mode.'/'.pair.'_thresholds.csv'

set datafile separator ','

# Set x time range from the timestamp format
stats server_ema_file u (strptime("%Y%m%d %H:%M:%S", strcol(1))):1 nooutput

set xdata time
set format x "%m/%d %H:%M"
set timefmt "%Y%m%d %H:%M"
set xrange [STATS_min_x:STATS_max_x]
#set format x "%m/%d"
#set timefmt "%m/%d/%Y %H:%M"

set xlabel 'Time'

plot server_ema_file using 1:2 with lines title "server_short_ema" noenhanced, \
    '' using 1:3 with lines title "server_long_ema" noenhanced

plot server_ema_file using 1:4 with lines title "server_apo" noenhanced

plot server_ema_file using 1:5 with lines title "server_value_strat" noenhanced

plot server_trades_file using 1:5 with lines title "server_price" noenhanced
 
plot server_trades_file using 1:6 with lines title "server_qty" noenhanced

plot server_trades_file using 1:7 with lines title "server_trades_value_strat" noenhanced

plot server_threshold_file using 1:2 with lines title "server_threshold_short" noenhanced

plot server_threshold_file using 1:3 with lines title "server_threshold_long" noenhanced

plot server_threshold_file using 1:3 with lines title "server_threshold_long" noenhanced, \
    server_threshold_file using 1:2 with lines title "server_threshold_short" noenhanced, \
    server_ema_file using 1:4 with lines title "server_apo" noenhanced, \
    server_trades_file using 1:(strcol(4) eq 'buy'?0:1/0) with points title "buy" linecolor rgb "red", \
    server_trades_file using 1:(strcol(4) eq 'sell'?0:1/0) with points title "sell" linecolor rgb "blue"

plot server_trades_file using 1:8 with lines title "server_borrowed" noenhanced
plot server_trades_file using 1:9 with lines title "server_interest" noenhanced

unset multiplot