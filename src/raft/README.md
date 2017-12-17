# 2A测试
# follower -> candidate
如果在选举超时的时间内没有leader联系自己，那么follower成为candidate

# candidate -> leader
* 返回的请求中，大多数节点同意了候选者的投票请求
* 要满足在当前term中一个raft节点只投票一次，需要为该raft节点设置voteFor标记，代表赞成的候选者id；如果这个标记被占用，那么拒绝其他投票请求。该标记的后续的更新，是由选举出的leader完成；标记的清除是在leader心跳超时的时间内没有leader联系自己，那么raft节点自动清除。

# leader -> follower
* 发现更高的term，退回follower。这个发现更高term的时机可以是，leader发起心跳时检查返回值时；leader收到其它leader的心跳时；leader接收到候选者的选举请求时。
* 发起心跳请求时发现每个请求都超时，那么可能是网络原因，那么当前leader退回follower，这么做是为了完成leader的快速失败，因为如果是网络原因失联，那么在当前leader失败之后重连回集群后，很可能日志已经不是最新了，并且已经有了新的leader那么就能快速以follower的身份工作，减少集群中同时存在两个leader时间。

# candidate -> follower
* 发现更高的term时退回follower，这个时机可以是在发起投票的过程中；接收到别人的投票请求时。
* 接收到leader的心跳。
