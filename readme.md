Storm Performance Test

This test consist of two part: data-generator and storm.

data-generator generate data and send to storm. In this test, data-generator use Apache avro for sending data to storm.
storm receive data and process very simple topology that just reverse strings.

data-generator is using in this test, also you can use another transpering protocol frameworks, like thrift, netty and so on.


first, run storm on local or cluster like below.

storm jar storm-perf-test-jar-with-dependencies.jar com.tigerby.storm.perftest.PerfTopology -p 10002 --messageSize 500 --pollFreq 20



and, run data generator like below.

java -jar data-generator-jar-with-dependencies.jar -h daisy08 -p 10002 -r 2000 --threadNum 10

In cluster mode, notice that you should check where Storm's spout is running in the cluster and parameter it when you running data-generator.

I tested in single machine that is composed by 4 vCore, 8G RAM, CentOS 5.4 (64-bit).
In my case, it processed 5 MB/sec averagely.
My result in detail is like that:

data-generator:

[5] Sent 6008 in 5002(ms) avg ns/msg 1201120(ns) avg 1201(msg/s) sleep 0(ms)
[7] Sent 5988 in 5001(ms) avg ns/msg 1197361(ns) avg 1197(msg/s) sleep 0(ms)
[1] Sent 5932 in 5001(ms) avg ns/msg 1186163(ns) avg 1186(msg/s) sleep 0(ms)
[2] Sent 5990 in 5001(ms) avg ns/msg 1197761(ns) avg 1198(msg/s) sleep 0(ms)
[8] Sent 6541 in 5001(ms) avg ns/msg 1307939(ns) avg 1308(msg/s) sleep 0(ms)
[4] Sent 6112 in 5004(ms) avg ns/msg 1221423(ns) avg 1222(msg/s) sleep 0(ms)
[6] Sent 6309 in 5002(ms) avg ns/msg 1261296(ns) avg 1261(msg/s) sleep 0(ms)
[9] Sent 6163 in 5001(ms) avg ns/msg 1232354(ns) avg 1232(msg/s) sleep 0(ms)
[3] Sent 6129 in 5001(ms) avg ns/msg 1225555(ns) avg 1225(msg/s) sleep 0(ms)
[10] Sent 6216 in 5001(ms) avg ns/msg 1242951(ns) avg 1243(msg/s) sleep 0(ms)



Storm:

status	topologies	totalSlots	slotsUsed	totalExecutors	executorsWithMetrics	time	time-diff(ms)	transferred	throughput(MB/s)	reachToEnd	reachThroughput(MB/s)	reachThroughput(msg/s)	reachLatency(ns/msg)
WAITING	1	4	0	5	0	1384148343947	0	0	0.0	0	0.0	0	0
WAITING	1	4	2	5	5	1384148363951	20004	283320	6.753524478209826	141660	3.376762239104913	7083	141211
WAITING	1	4	2	5	5	1384148383952	20001	420520	10.025476814538179	210720	5.023705113572446	10536	94917
RUNNING	1	4	2	5	5	1384148403947	19995	443800	10.58366245614138	224680	5.35812816729573	11825	88993
RUNNING	1	4	2	5	5	1384148423948	20001	355840	8.483462545622718	177800	4.2388703929061355	8890	112491
RUNNING	1	4	2	5	5	1384148443953	20005	275460	6.565836720751453	149620	3.566327198717899	7481	133705
RUNNING	1	4	2	5	5	1384148463948	19995	394260	9.402241460023209	183120	4.367012773701237	9637	109190
RUNNING	1	4	2	5	5	1384148483953	20005	387860	9.24499176109293	194080	4.626071265386779	9704	103076
RUNNING	1	4	2	5	5	1384148503954	20001	544980	12.992686089572473	273780	6.527097503767389	13689	73055
RUNNING	1	4	2	5	5	1384148523948	19994	398740	9.509555289682607	200460	4.780773068590499	10550	99740
RUNNING	1	4	2	5	5	1384148543950	20002	515180	12.281620196134684	257100	6.129128755825589	12855	77798
RUNNING	1	4	2	5	5	1384148563951	20001	374860	8.936912010600642	188480	4.4934887044710266	9424	106117
RUNNING	1	4	2	5	5	1384148583949	19998	422080	10.064177804499199	209860	5.00395269629502	11045	95292
RUNNING	1	4	2	5	5	1384148603950	20001	271040	6.461774079264786	135540	3.2313638529499302	6777	147565
RUNNING	1	4	2	5	5	1384148623952	20002	457040	10.89559317993982	228160	5.439214379343316	11408	87666
RUNNING	1	4	2	5	5	1384148643946	19994	248700	5.9312494370869855	143560	3.4237642508572885	7555	139272
RUNNING	1	4	2	5	5	1384148663951	20005	416100	9.918117547029258	188840	4.501171154965165	9442	105936
RUNNING	1	4	2	5	5	1384148683948	19997	352500	8.405515740691182	175820	4.192504333413684	9253	113735

References