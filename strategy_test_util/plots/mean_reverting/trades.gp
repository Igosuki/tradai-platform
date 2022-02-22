#!gnuplot --persist

set terminal wxt size 2800,1200 font ',10'
set multiplot layout 2,1 columnsfirst title "Multiplot layout 3, 2" font ",14"
set key left Left reverse
set key autotitle columnhead # use the first line as title
set tics out nomirror

server_ema_file = 'trader/strategies/test_results/strategies_mean_reverting_tests_scenario/'.pair.'_ema_values.csv'
r_ema_file = 'strategies/logs/mean_reverting_strategy/'.pair.'_ema_values.csv'
server_trades_file = 'trader/strategies/test_results/strategies_mean_reverting_tests_scenario/'.pair.'_trade_events.csv'
r_trades_file = 'strategies/logs/mean_reverting_strategy/'.pair.'_trade_events.csv'
server_threshold_file = 'trader/strategies/test_results/strategies_mean_reverting_tests_scenario/'.pair.'_thresholds.csv'
r_threshold_file = 'strategies/logs/mean_reverting_strategy/'.pair.'_thresholds.csv'

set datafile separator ','

# Set x time range from the timestamp format
stats server_ema_file u (strptime("%Y%m%d %H:%M:%S", strcol(1))):1 nooutput

set xdata time
set format x "%m/%d %H:%M"
set timefmt "%Y%m%d %H:%M"
#set xrange [STATS_min_x:"20200325 10:18"]
set xrange [STATS_min_x:STATS_max_x]
#set format x "%m/%d"
#set timefmt "%m/%d/%Y %H:%M"

set xlabel 'Time'

plot server_threshold_file using 1:3 with lines title "server_threshold_long" noenhanced, \
    server_threshold_file using 1:2 with lines title "server_threshold_short" noenhanced, \
    server_ema_file using 1:4 with lines title "server_ppo" noenhanced, \
    server_trades_file using 1:($2==0?0:1/0) with points title "buy" linecolor rgb "red", \
    server_trades_file using 1:($2==1?0:1/0) with points title "sell" linecolor rgb "blue"

unset multiplot
