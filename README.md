alibaba rocketmq protocol wireshark lua plugin

usage:
1. find the plugin dir,  On my Mac, the path is "/Applications/WireShark.app/Contents/Resources/share/wireshark"

2. mkdir rocketmq-plugin

3. copy json.lua and rocketmq.lua to plugin dir "rocketmq-plugin".

4. modify last line of  init.lua
dofile("rocketmq-plugin/rocketmq.lua")

5. restart wireshark to make it done

ps, you may also need to modify the port your rocketmq is using, open rocketmq.lua, find the following line. Add your ports here:

```
local PORTS = { 9876, 10911, 20111 }
```