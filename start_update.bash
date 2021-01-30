nohup julia update_continuously.jl > continuous_update.log 2>&1 &
echo $! > continuous_update_pid.txt

nohup julia get_gamefeed.jl > get_gamefeed.log 2>&1 &
echo $! > get_gamefeed_pid.txt

nohup julia site.jl -t auto > site.log 2>&1 &
echo $! > site_pid.txt