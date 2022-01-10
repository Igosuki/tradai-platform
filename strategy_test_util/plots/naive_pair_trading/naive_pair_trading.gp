set terminal wxt size 2800,1200 font ',10'
set multiplot layout 7,2 columnsfirst title "Multiplot layout 3, 2" font ",14"
set key left Left reverse
set key autotitle columnhead # use the first line as title
set tics out nomirror

server_model_file = 'trader/target/test_results/strategies_naive_pair_trading_tests_backtests_spot/model_values.csv'
r_model_file = 'strategies/logs/naive_pair_trading/model_values.csv'
server_trades_file = 'trader/target/test_results/strategies_naive_pair_trading_tests_backtests_spot/trade_events.csv'
r_trades_file = 'strategies/logs/naive_pair_trading/trade_events.csv'

set datafile separator ','

# Set x time range from the timestamp format
stats server_model_file u (strptime("%Y%m%d %H:%M:%S", strcol(1))):1 nooutput

set xdata time
set format x "%m/%d %H:%M"
set timefmt "%Y%m%d %H:%M"
set xrange [STATS_min_x:STATS_max_x]
#set format x "%m/%d"
#set timefmt "%m/%d/%Y %H:%M"

set xlabel 'Time'

plot server_model_file using 1:4 with lines title "server_beta" noenhanced, \
    r_model_file using 1:2 with lines

plot server_model_file using 1:3 with lines title "server_alpha" noenhanced, \
    r_model_file using 1:3 with lines

plot server_model_file using 1:4 with lines title "server_predicted_right" noenhanced, \
    r_model_file using 1:4 with lines

plot server_model_file using 1:5 with lines title "server_res" noenhanced, \
    r_model_file using 1:5 with lines 

plot server_model_file using 1:2 with lines title "server_value_strat" noenhanced, \
    r_model_file using 1:6 with lines

plot server_trades_file using 1:6 with lines title "server_price" noenhanced, \
    r_trades_file using 1:5 with lines title "price"
 
plot server_trades_file using 1:7 with lines title "server_qty" noenhanced, \
    r_trades_file using 1:6 with lines title "qty"

plot server_trades_file using 1:8 with lines title "server_trades_value_strat" noenhanced, \
    r_trades_file using 1:7 with lines title "trades_value_strat" noenhanced

plot -0.03 with lines title "server_threshold_long" noenhanced, \
    0.03 with lines title "server_threshold_short" noenhanced, \
    server_model_file using 1:5 with lines title "server_res" noenhanced, \
    server_trades_file using 1:(strcol(5) eq 'buy'?0:1/0) with points title "buy" linecolor rgb "red", \
    server_trades_file using 1:(strcol(5) eq 'sell'?0:1/0) with points title "sell" linecolor rgb "blue"

plot -0.03 with lines title "r_threshold_long" noenhanced, \
    0.03 with lines title "r_threshold_short" noenhanced, \
    r_model_file using 1:5 with lines title "r_res" noenhanced, \
    r_trades_file using 1:(strcol(4) eq 'buy'?0:1/0) with points title "buy" linecolor rgb "red", \
    r_trades_file using 1:(strcol(4) eq 'sell'?0:1/0) with points title "sell" linecolor rgb "blue"


unset multiplot