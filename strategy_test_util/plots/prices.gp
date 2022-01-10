#!gnuplot --persist

# Prepare data firs with 
# cat data/Binance/order_books/pr=$pair/*.csv > $pair.csv


set terminal wxt size 2800,1200 font ',10'
set key left Left reverse
set key autotitle columnhead # use the first line as title
set tics out nomirror

order_books_by_1mn_file = pair.'.csv'

set datafile separator ','

# Set x time range from the timestamp format
stats order_books_by_1mn_file u (strptime("%Y-%m-%d %H:%M:%S", strcol(1))):1 nooutput

set xdata time
set format x "%m/%d %H:%M"
set timefmt "%Y-%m-%d %H:%M"
set xrange ["2020-03-25 00:00":"2020-09-01 00:00"]
#set xrange [STATS_min_x:STATS_max_x]
#set format x "%m/%d"
#set timefmt "%m/%d/%Y %H:%M"

set xlabel 'Time'

plot order_books_by_1mn_file using 1:2 with lines title "top_ask" noenhanced, \
    '' using 1:12 with lines title "top_bid" noenhanced
